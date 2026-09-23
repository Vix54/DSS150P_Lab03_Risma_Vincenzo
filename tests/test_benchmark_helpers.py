from datetime import timedelta, timezone

import pandas as pd
import pytest

from src.benchmark import formats
from src.benchmark.partitions import (
    belongs_to_month,
    parquet_files,
    read_partition,
    write_partitioned_parquet,
)
from src.benchmark.timing import measure, summarize
from src.transform.curated import build_curated

PROCESSED_AT = pd.Timestamp('2026-02-01T00:00:00.123456Z')
UPDATED = pd.Timestamp('2025-03-02T10:00:00Z')


def sample_curated():
    customers = pd.DataFrame({'customer_id': ['C1'], 'city': ['Pasig'], 'customer_tier': ['Gold']})
    products = pd.DataFrame({'product_id': ['P1'], 'name': ['Widget'], 'brand': ['Nova'], 'category_name': ['Audio']})
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct', 'status', 'updated_at']
    rows = [
        ['O1', 'C1', 'P1', pd.Timestamp('2025-01-31T23:30:00Z'), 3, 100.5, 0.1, 'PAID', UPDATED],
        ['O2', 'C1', 'P1', pd.Timestamp('2025-02-01T00:10:00Z'), 1, 20.0, 0.0, 'DELIVERED', UPDATED],
        ['O3', 'C1', 'P1', pd.Timestamp('2025-02-15T12:00:00Z'), 2, 10.1, 0.05, 'DELIVERED', UPDATED],
    ]
    orders = pd.DataFrame(rows, columns=columns)
    curated, _, _ = build_curated({'orders': orders, 'customers': customers, 'products': products}, [], 'run_a', PROCESSED_AT)
    return curated


def test_measure_discards_the_warm_up_and_returns_one_timing_per_repeat():
    calls = []
    timings, result = measure(lambda: calls.append(1) or len(calls), repeats=5)
    assert len(timings) == 5
    assert len(calls) == 6
    assert result == 6


def test_measure_runs_the_before_hook_ahead_of_every_run():
    events = []
    measure(lambda: events.append('run'), repeats=2, before=lambda: events.append('before'))
    assert events == ['before', 'run'] * 3


def test_summarize_reports_the_median_and_the_extremes():
    summary = summarize([3.0, 1.0, 2.0, 10.0, 4.0])
    assert summary['median'] == 3.0
    assert summary['min'] == 1.0
    assert summary['max'] == 10.0


@pytest.mark.parametrize('kind', ['csv', 'jsonl', 'parquet'])
def test_every_format_round_trips_to_identical_record_hashes(tmp_path, kind):
    df = sample_curated()
    path = tmp_path / f'rows.{kind}'
    if kind == 'csv':
        formats.write_csv(df, path)
    elif kind == 'jsonl':
        formats.write_jsonl(df, path)
    else:
        formats.write_parquet(df, path, 'snappy')
    reread = formats.read_for_verification(path, kind)
    assert formats.same_row_set(df, reread)
    assert formats.count_hash_mismatches(reread) == 0


def test_a_changed_value_in_a_file_is_detected_by_the_hash_check(tmp_path):
    df = sample_curated()
    path = tmp_path / 'rows.csv'
    formats.write_csv(df, path)
    text = path.read_text(encoding='utf-8').replace('PAID', 'SHIPPED')
    path.write_text(text, encoding='utf-8')
    assert formats.count_hash_mismatches(formats.read_for_verification(path, 'csv')) == 1


def test_the_hash_is_the_same_for_timestamps_expressed_in_another_time_zone():
    df = sample_curated()
    shifted = df.copy()
    zone = timezone(timedelta(hours=8))
    shifted['order_timestamp'] = [value.astimezone(zone) for value in shifted['order_timestamp']]
    assert formats.count_hash_mismatches(shifted) == 0


def test_partitions_are_named_by_utc_year_and_month_and_rewriting_is_idempotent(tmp_path):
    df = sample_curated()
    root = tmp_path / 'partitioned'
    write_partitioned_parquet(df, root, 'run_a')
    first = sorted(path.relative_to(root).as_posix() for path in parquet_files(root))
    write_partitioned_parquet(df, root, 'run_a')
    second = sorted(path.relative_to(root).as_posix() for path in parquet_files(root))
    assert first == second == [
        'order_year=2025/order_month=1/part-0.parquet',
        'order_year=2025/order_month=2/part-0.parquet',
    ]


def test_reading_one_partition_returns_only_that_month(tmp_path):
    df = sample_curated()
    root = tmp_path / 'partitioned'
    write_partitioned_parquet(df, root, 'run_a')
    february = read_partition(root, 2025, 2)
    assert sorted(february['order_id']) == ['O2', 'O3']
    assert belongs_to_month(february, 2025, 2)
    assert not belongs_to_month(read_partition(root, 2025, 1), 2025, 2)
    assert list(february.columns) == list(df.columns)
