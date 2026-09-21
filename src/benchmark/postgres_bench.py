import pandas as pd
from psycopg import sql

from src.benchmark.timing import measure, summarize
from src.load.postgres import connect, to_python

BENCH_TABLE = 'benchmark.sales_order_lines_bench'
INDEX_NAME = 'idx_bench_status'


def prepare_table(connection):
    with connection.cursor() as cursor:
        cursor.execute('CREATE SCHEMA IF NOT EXISTS benchmark')
        cursor.execute(f'DROP TABLE IF EXISTS {BENCH_TABLE}')
        cursor.execute(f'CREATE TABLE {BENCH_TABLE} (LIKE curated.sales_order_lines INCLUDING ALL)')
    connection.commit()


def truncate(connection):
    with connection.cursor() as cursor:
        cursor.execute(f'TRUNCATE {BENCH_TABLE}')
    connection.commit()


def copy_rows(connection, df):
    column_list = ', '.join(df.columns)
    with connection.cursor() as cursor:
        with cursor.copy(f'COPY {BENCH_TABLE} ({column_list}) FROM STDIN') as copy:
            for row in df.itertuples(index=False, name=None):
                copy.write_row([to_python(value) for value in row])
    connection.commit()


def fetch_frame(connection, query, parameters=None):
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        names = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=names)


def vacuum_analyze(connection):
    connection.commit()
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(f'VACUUM (ANALYZE) {BENCH_TABLE}')
    connection.autocommit = False


def relation_sizes(connection):
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT pg_total_relation_size(%s::regclass), pg_relation_size(%s::regclass), pg_indexes_size(%s::regclass)',
            (BENCH_TABLE, BENCH_TABLE, BENCH_TABLE),
        )
        total, table, indexes = cursor.fetchone()
    return {'total_bytes': total, 'table_bytes': table, 'index_bytes': indexes}


def explain_filtered(connection, status):
    statement = sql.SQL('EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM {} WHERE status = {}').format(
        sql.SQL(BENCH_TABLE), sql.Literal(status)
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)
        plan = cursor.fetchone()[0][0]
    return {
        'node_type': plan['Plan']['Node Type'],
        'execution_ms': plan['Execution Time'],
        'planning_ms': plan['Planning Time'],
    }


def drop_index(connection):
    with connection.cursor() as cursor:
        cursor.execute(f'DROP INDEX IF EXISTS benchmark.{INDEX_NAME}')
    connection.commit()


def create_index(connection):
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE INDEX {INDEX_NAME} ON {BENCH_TABLE} (status)')
    connection.commit()


def benchmark_postgres(df, repeats, status):
    full_query = f'SELECT * FROM {BENCH_TABLE}'
    filtered_query = f'SELECT * FROM {BENCH_TABLE} WHERE status = %s'
    with connect('benchmark') as connection:
        with connection.cursor() as cursor:
            cursor.execute('SHOW server_version')
            server_version = cursor.fetchone()[0]
        prepare_table(connection)
        write_times, _ = measure(lambda: copy_rows(connection, df), repeats, before=lambda: truncate(connection))
        vacuum_analyze(connection)
        plain = {'sizes': relation_sizes(connection)}
        full_times, full = measure(lambda: fetch_frame(connection, full_query), repeats)
        filtered_times, filtered = measure(lambda: fetch_frame(connection, filtered_query, (status,)), repeats)
        plain.update({
            'write': summarize(write_times),
            'full_read': summarize(full_times),
            'filtered_read': summarize(filtered_times),
            'row_count': len(full),
            'filtered_rows': len(filtered),
            'plan': explain_filtered(connection, status),
        })
        verification_frame = fetch_frame(connection, full_query)
        index_times, _ = measure(lambda: create_index(connection), repeats, before=lambda: drop_index(connection))
        vacuum_analyze(connection)
        indexed = {'sizes': relation_sizes(connection)}
        full_times, full = measure(lambda: fetch_frame(connection, full_query), repeats)
        filtered_times, filtered = measure(lambda: fetch_frame(connection, filtered_query, (status,)), repeats)
        indexed.update({
            'index_build': summarize(index_times),
            'full_read': summarize(full_times),
            'filtered_read': summarize(filtered_times),
            'row_count': len(full),
            'filtered_rows': len(filtered),
            'plan': explain_filtered(connection, status),
        })
    return {'server_version': server_version, 'plain': plain, 'indexed': indexed, 'verification_frame': verification_frame}
