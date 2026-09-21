import json
import logging
import platform
from datetime import datetime, timezone
from functools import partial
from importlib import metadata

import pandas as pd

from src.benchmark import formats
from src.benchmark.partitions import (
    belongs_to_month,
    describe_tree,
    parquet_files,
    read_all_partitions,
    read_partition,
    write_partitioned_parquet,
)
from src.benchmark.postgres_bench import benchmark_postgres
from src.benchmark.timing import measure, summarize
from src.common.errors import PipelineError
from src.config import SETTINGS, path_for

STAGE = 'benchmark'
MIN_REPEATS = 5
RESULT_COLUMNS = ['storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds', 'filtered_read_seconds', 'row_count', 'notes']
FILE_FORMATS = [
    {
        'name': 'csv',
        'file': 'sales_order_lines.csv',
        'kind': 'csv',
        'write': formats.write_csv,
        'read': formats.read_csv_full,
        'filtered': formats.read_csv_filtered,
        'notes': 'read_csv with type inference; the filter runs after a full read',
    },
    {
        'name': 'jsonl',
        'file': 'sales_order_lines.jsonl',
        'kind': 'jsonl',
        'write': formats.write_jsonl,
        'read': formats.read_jsonl_full,
        'filtered': formats.read_jsonl_filtered,
        'notes': 'one JSON object per line with money as exact strings; the filter runs after a full read',
    },
    {
        'name': 'parquet_snappy',
        'file': 'sales_order_lines_snappy.parquet',
        'kind': 'parquet',
        'write': partial(formats.write_parquet, compression='snappy'),
        'read': formats.read_parquet_full,
        'filtered': formats.read_parquet_filtered,
        'notes': 'compression=snappy; typed columnar file; the filter is pushed down to the reader',
    },
    {
        'name': 'parquet_zstd',
        'file': 'sales_order_lines_zstd.parquet',
        'kind': 'parquet',
        'write': partial(formats.write_parquet, compression='zstd'),
        'read': formats.read_parquet_full,
        'filtered': formats.read_parquet_filtered,
        'notes': 'compression=zstd; typed columnar file; the filter is pushed down to the reader',
    },
]
logger = logging.getLogger(__name__)


def benchmark_one_file(spec, df, files_dir, repeats, status):
    path = files_dir / spec['file']
    write_times, _ = measure(lambda: spec['write'](df, path), repeats)
    full_times, full = measure(lambda: spec['read'](path), repeats)
    filtered_times, filtered = measure(lambda: spec['filtered'](path, 'status', status), repeats)
    verified = formats.read_for_verification(path, spec['kind'])
    mismatches = formats.count_hash_mismatches(verified)
    same_rows = formats.same_row_set(df, verified)
    write, full_read, filtered_read = summarize(write_times), summarize(full_times), summarize(filtered_times)
    row = {
        'storage_type': spec['name'],
        'file_size_bytes': path.stat().st_size,
        'write_seconds': round(write['median'], 6),
        'full_read_seconds': round(full_read['median'], 6),
        'filtered_read_seconds': round(filtered_read['median'], 6),
        'row_count': len(full),
        'notes': f"filtered_rows={len(filtered)}; hash_mismatches={mismatches}; same_row_set={same_rows}; {spec['notes']}",
    }
    detail = {
        'file': str(path.name),
        'file_size_bytes': row['file_size_bytes'],
        'write': write,
        'full_read': full_read,
        'filtered_read': filtered_read,
        'row_count': len(full),
        'filtered_rows': len(filtered),
        'hash_mismatches': mismatches,
        'same_row_set': same_rows,
    }
    logger.info('%s: size=%d write=%.4fs full_read=%.4fs filtered_read=%.4fs', spec['name'], row['file_size_bytes'], row['write_seconds'], row['full_read_seconds'], row['filtered_read_seconds'])
    return row, detail


def postgres_rows(result):
    plain, indexed = result['plain'], result['indexed']
    mismatches = formats.count_hash_mismatches(result['verification_frame'])
    plain_row = {
        'storage_type': 'postgresql',
        'file_size_bytes': plain['sizes']['total_bytes'],
        'write_seconds': round(plain['write']['median'], 6),
        'full_read_seconds': round(plain['full_read']['median'], 6),
        'filtered_read_seconds': round(plain['filtered_read']['median'], 6),
        'row_count': plain['row_count'],
        'notes': (
            f"size=pg_total_relation_size with primary key index (table_bytes={plain['sizes']['table_bytes']}, index_bytes={plain['sizes']['index_bytes']}); "
            f"write=COPY plus commit; filtered_rows={plain['filtered_rows']}; plan={plain['plan']['node_type']}; "
            f"hash_mismatches={mismatches}; server={result['server_version']}"
        ),
    }
    indexed_row = {
        'storage_type': 'postgresql_status_index',
        'file_size_bytes': indexed['sizes']['total_bytes'],
        'write_seconds': round(indexed['index_build']['median'], 6),
        'full_read_seconds': round(indexed['full_read']['median'], 6),
        'filtered_read_seconds': round(indexed['filtered_read']['median'], 6),
        'row_count': indexed['row_count'],
        'notes': (
            f"write_seconds is CREATE INDEX on status; size includes both indexes (index_bytes={indexed['sizes']['index_bytes']}); "
            f"filtered_rows={indexed['filtered_rows']}; plan={indexed['plan']['node_type']}"
        ),
    }
    detail = {
        'plain': {key: value for key, value in plain.items()},
        'indexed': {key: value for key, value in indexed.items()},
        'hash_mismatches': mismatches,
    }
    return [plain_row, indexed_row], detail


def benchmark_partitions(df, root, run_id, repeats, year, month):
    summary = write_partitioned_parquet(df, root, run_id)
    full_times, full = measure(lambda: read_all_partitions(root), repeats)
    part_times, part = measure(lambda: read_partition(root, year, month), repeats)
    stamps = pd.to_datetime(df['order_timestamp'], utc=True)
    expected = int(((stamps.dt.year == year) & (stamps.dt.month == month)).sum())
    all_files, selected_files = parquet_files(root), parquet_files(root, year, month)
    return {
        'partition_count': len(summary['partitions']),
        'rows_per_partition_min': min(item['rows'] for item in summary['partitions']),
        'rows_per_partition_max': max(item['rows'] for item in summary['partitions']),
        'selected': {'order_year': year, 'order_month': month},
        'selected_rows_expected': expected,
        'selected_rows_read': len(part),
        'selected_rows_all_in_month': belongs_to_month(part, year, month),
        'all_files': len(all_files),
        'all_bytes': sum(path.stat().st_size for path in all_files),
        'selected_files': len(selected_files),
        'selected_bytes': sum(path.stat().st_size for path in selected_files),
        'full_dataset_read': summarize(full_times),
        'selected_partition_read': summarize(part_times),
        'full_dataset_rows': len(full),
        'tree': describe_tree(root),
    }


def environment_info(server_version):
    return {
        'python': platform.python_version(),
        'platform': platform.platform(),
        'pandas': metadata.version('pandas'),
        'pyarrow': metadata.version('pyarrow'),
        'psycopg': metadata.version('psycopg'),
        'postgresql': server_version,
    }


def run_benchmark(curated_path, run_id, repeats):
    if repeats < MIN_REPEATS:
        raise PipelineError(STAGE, f'--repeats must be at least {MIN_REPEATS} so that medians are meaningful')
    settings = SETTINGS['storage_benchmark']
    status = settings['filter_status']
    selected = settings['selected_partition']
    df = pd.read_parquet(curated_path)
    benchmark_dir = path_for('benchmark_dir')
    files_dir = benchmark_dir / 'files'
    files_dir.mkdir(parents=True, exist_ok=True)
    rows, details = [], {}
    for spec in FILE_FORMATS:
        row, detail = benchmark_one_file(spec, df, files_dir, repeats, status)
        rows.append(row)
        details[spec['name']] = detail
    result = benchmark_postgres(df, repeats, status)
    database_rows, database_detail = postgres_rows(result)
    rows.extend(database_rows)
    details['postgresql'] = database_detail
    details['partitioning'] = benchmark_partitions(df, path_for('partition_dir'), run_id, repeats, selected['year'], selected['month'])
    details['run'] = {
        'curated_run_id': run_id,
        'generated_at_utc': datetime.now(timezone.utc).isoformat(),
        'repeats': repeats,
        'warm_up_runs_discarded_per_measurement': 1,
        'filter': f'status = {status}',
        'rows': len(df),
        'columns': len(df.columns),
        'environment': environment_info(result['server_version']),
    }
    pd.DataFrame(rows, columns=RESULT_COLUMNS).to_csv(benchmark_dir / 'benchmark_results.csv', index=False)
    (benchmark_dir / 'benchmark_details.json').write_text(json.dumps(details, indent=2, default=str), encoding='utf-8')
    return rows, details
