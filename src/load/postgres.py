import logging

import numpy as np
import pandas as pd
import psycopg

from src.common.errors import PipelineError
from src.config import get_db_settings

STAGE = 'load'
TABLE = 'curated.sales_order_lines'
KEY_COLUMN = 'order_id'
logger = logging.getLogger(__name__)


def connect():
    try:
        settings = get_db_settings()
    except RuntimeError as error:
        raise PipelineError(STAGE, str(error)) from error
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
        raise PipelineError(STAGE, f'cannot connect to {target}: {error}') from error


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


def load_partition(df, year: int, month: int, run_id: str) -> int:
    raise NotImplementedError('Implement Goal 3 selected-partition load')
