from src.load.postgres import TABLE, connect
from src.transform.curated import HASH_COLUMNS, row_hash

STAGE = 'validate'
MAX_EXAMPLES = 3


def examples(values):
    return ', '.join(str(value) for value in list(values)[:MAX_EXAMPLES])


def check_database(curated, run_id):
    findings = []
    columns = ', '.join(HASH_COLUMNS + ['record_hash'])
    with connect(STAGE) as connection, connection.cursor() as cursor:
        cursor.execute(f'SELECT COUNT(*), COUNT(DISTINCT order_id) FROM {TABLE}')
        total, distinct = cursor.fetchone()
        cursor.execute(f'SELECT {columns} FROM {TABLE}')
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, values)) for values in cursor.fetchall()]
        cursor.execute('SELECT status FROM audit.pipeline_runs WHERE pipeline_run_id = %s', (run_id,))
        audit = cursor.fetchone()
    if total != distinct:
        findings.append(f'V-11 {TABLE} has {total} rows but {distinct} distinct order_id values')
    stored = {row['order_id']: row['record_hash'] for row in rows}
    missing = [order_id for order_id in curated['order_id'] if order_id not in stored]
    if missing:
        findings.append(f'{len(missing)} curated order(s) are not in {TABLE}, e.g. {examples(missing)}')
    different = [order_id for order_id, record_hash in zip(curated['order_id'], curated['record_hash']) if order_id in stored and stored[order_id] != record_hash]
    if different:
        findings.append(f'{len(different)} loaded order(s) have a different record_hash from the curated output, e.g. {examples(different)}')
    drifted = [row['order_id'] for row in rows if row_hash(row) != row['record_hash']]
    if drifted:
        findings.append(f'{len(drifted)} stored row(s) no longer match their own record_hash, e.g. {examples(drifted)}')
    if audit is None:
        findings.append(f'audit.pipeline_runs has no row for {run_id}')
    elif audit[0] != 'loaded':
        findings.append(f'audit.pipeline_runs shows status {audit[0]} for {run_id} instead of loaded')
    return findings
