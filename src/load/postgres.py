import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg

from src.common.errors import PipelineError
from src.config import get_db_settings

STAGE = 'load'
TABLE = 'curated.sales_order_lines'
KEY_COLUMN = 'order_id'
logger = logging.getLogger(__name__)


def connect(stage=STAGE):
    try:
        settings = get_db_settings()
    except RuntimeError as error:
        raise PipelineError(stage, str(error)) from error
    target = f"{settings['host']}:{settings['port']}/{settings['dbname']} as {settings['user']}"
    try:
        return psycopg.connect(
            host=settings['host'],
            port=settings['port'],
            dbname=settings['dbname'],
            user=settings['user'],
            password=settings['password'],
            connect_timeout=10,
        )
    except psycopg.OperationalError as error:
        raise PipelineError(stage, f'cannot connect to {target}: {error}') from error


def to_python(value):
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    return value


def upsert_curated(connection, df):
    if not df[KEY_COLUMN].is_unique:
        raise PipelineError(STAGE, f'the incoming data has duplicate {KEY_COLUMN} values; refusing to load it')
    columns = list(df.columns)
    column_list = ', '.join(columns)
    update_list = ', '.join(f'{column} = EXCLUDED.{column}' for column in columns if column != KEY_COLUMN)
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE TEMP TABLE incoming (LIKE {TABLE} INCLUDING DEFAULTS) ON COMMIT DROP')
        with cursor.copy(f'COPY incoming ({column_list}) FROM STDIN') as copy:
            for row in df.itertuples(index=False, name=None):
                copy.write_row([to_python(value) for value in row])
        cursor.execute(
            f'INSERT INTO {TABLE} ({column_list}) SELECT {column_list} FROM incoming '
            f'ON CONFLICT ({KEY_COLUMN}) DO UPDATE SET {update_list} '
            f'WHERE {TABLE}.record_hash IS DISTINCT FROM EXCLUDED.record_hash '
            'RETURNING (xmax = 0) AS inserted'
        )
        outcomes = [row[0] for row in cursor.fetchall()]
    inserted = sum(outcomes)
    return {
        'rows_in': len(df),
        'inserted': inserted,
        'updated': len(outcomes) - inserted,
        'unchanged': len(df) - len(outcomes),
    }


def record_run(connection, run_id, started_at, completed_at, status, counts, message):
    with connection.cursor() as cursor:
        cursor.execute(
            'INSERT INTO audit.pipeline_runs '
            '(pipeline_run_id, started_at_utc, completed_at_utc, status, rows_staging, rows_curated, rows_quarantined, message) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
            'ON CONFLICT (pipeline_run_id) DO UPDATE SET '
            'completed_at_utc = EXCLUDED.completed_at_utc, status = EXCLUDED.status, '
            'rows_staging = EXCLUDED.rows_staging, rows_curated = EXCLUDED.rows_curated, '
            'rows_quarantined = EXCLUDED.rows_quarantined, message = EXCLUDED.message',
            (
                run_id,
                started_at,
                completed_at,
                status,
                counts['rows_staging'],
                counts['rows_curated'],
                counts['rows_quarantined'],
                message,
            ),
        )


def record_event(connection, event_type, run_id, status, outcome=None, partition_key=None, message=None):
    outcome = outcome or {}
    with connection.cursor() as cursor:
        cursor.execute(
            'INSERT INTO audit.pipeline_run_events '
            '(recorded_at_utc, event_type, pipeline_run_id, partition_key, status, rows_in, rows_inserted, rows_updated, rows_unchanged, message) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            (
                datetime.now(timezone.utc),
                event_type,
                run_id,
                partition_key,
                status,
                outcome.get('rows_in'),
                outcome.get('inserted'),
                outcome.get('updated'),
                outcome.get('unchanged'),
                message,
            ),
        )


def load_partition(connection, df, year, month, run_id):
    outcome = upsert_curated(connection, df)
    partition_key = f'{year:04d}-{month:02d}'
    with connection.cursor() as cursor:
        cursor.execute(
            'INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id) '
            'VALUES (%s, %s, %s, %s) '
            'ON CONFLICT (partition_key) DO UPDATE SET '
            'loaded_at_utc = EXCLUDED.loaded_at_utc, row_count = EXCLUDED.row_count, pipeline_run_id = EXCLUDED.pipeline_run_id',
            (partition_key, datetime.now(timezone.utc), len(df), run_id),
        )
    summary = f"inserted={outcome['inserted']} updated={outcome['updated']} unchanged={outcome['unchanged']}"
    record_event(connection, 'partition_load', run_id, 'loaded', outcome=outcome, partition_key=partition_key, message=summary)
    return outcome
