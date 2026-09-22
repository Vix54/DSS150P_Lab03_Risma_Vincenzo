from datetime import datetime, timezone

import pandas as pd

from src.load.postgres import TABLE, connect
from src.transform.curated import HASH_COLUMNS, row_hash

STAGE = 'validate'
MAX_EXAMPLES = 3
EVENTS_TABLE = 'audit.pipeline_run_events'


def examples(values):
    return ', '.join(str(value) for value in list(values)[:MAX_EXAMPLES])


def month_bounds(year, month):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
    return start, end


def month_mask(frame, year, month):
    stamps = pd.to_datetime(frame['order_timestamp'], utc=True)
    return (stamps.dt.year == year) & (stamps.dt.month == month)


def check_database(curated, run_id, partition=None):
    findings = []
    columns = ', '.join(HASH_COLUMNS + ['record_hash'])
    expected = curated
    row_query = f'SELECT {columns} FROM {TABLE}'
    row_parameters = None
    if partition:
        year, month = partition
        expected = curated[month_mask(curated, year, month)]
        start, end = month_bounds(year, month)
        row_query += ' WHERE order_timestamp >= %s AND order_timestamp < %s'
        row_parameters = (start, end)
    with connect(STAGE) as connection, connection.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*), COUNT(DISTINCT order_id) FROM {TABLE}')
        total, distinct = cursor.fetchone()
        cursor.execute(row_query, row_parameters)
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
        cursor.execute("SELECT to_regclass('audit.pipeline_run_events') IS NOT NULL")
        events_table_exists = cursor.fetchone()[0]
        audit = None
        events = 0
        if partition:
            partition_key = f'{year:04d}-{month:02d}'
            cursor.execute('SELECT row_count, pipeline_run_id FROM audit.partition_loads WHERE partition_key = %s', (partition_key,))
            audit = cursor.fetchone()
            if events_table_exists:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {EVENTS_TABLE} WHERE pipeline_run_id = %s AND event_type = 'partition_load' AND partition_key = %s AND status = 'loaded'",
                    (run_id, partition_key),
                )
                events = cursor.fetchone()[0]
        else:
            cursor.execute('SELECT status FROM audit.pipeline_runs WHERE pipeline_run_id = %s', (run_id,))
            audit = cursor.fetchone()
            if events_table_exists:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {EVENTS_TABLE} WHERE pipeline_run_id = %s AND event_type = 'load' AND status = 'loaded'",
                    (run_id,),
                )
                events = cursor.fetchone()[0]
    if total != distinct:
        findings.append(f'V-11 {TABLE} has {total} rows but {distinct} distinct order_id values')
    stored = {row['order_id']: row['record_hash'] for row in rows}
    missing = [order_id for order_id in expected['order_id'] if order_id not in stored]
    if missing:
        findings.append(f'{len(missing)} curated order(s) are not in {TABLE}, e.g. {examples(missing)}')
    different = [order_id for order_id, record_hash in zip(expected['order_id'], expected['record_hash']) if order_id in stored and stored[order_id] != record_hash]
    if different:
        findings.append(f'{len(different)} loaded order(s) have a different record_hash from the curated output, e.g. {examples(different)}')
    drifted = [row['order_id'] for row in rows if row_hash(row) != row['record_hash']]
    if drifted:
        findings.append(f'{len(drifted)} stored row(s) no longer match their own record_hash, e.g. {examples(drifted)}')
    if partition:
        if audit is None:
            findings.append(f'audit.partition_loads has no row for {partition_key}')
        elif audit[0] != len(expected) or audit[1] != run_id:
            findings.append(f'audit.partition_loads shows {audit[0]} rows from run {audit[1]} for {partition_key}, expected {len(expected)} rows from run {run_id}')
    elif audit is None:
        findings.append(f'audit.pipeline_runs has no row for {run_id}')
    elif audit[0] != 'loaded':
        findings.append(f'audit.pipeline_runs shows status {audit[0]} for {run_id} instead of loaded')
    if not events_table_exists:
        findings.append(f'{EVENTS_TABLE} does not exist; apply sql/init/03_audit_events.sql')
    elif events == 0:
        findings.append(f'{EVENTS_TABLE} has no successful load event for {run_id}')
    return findings
