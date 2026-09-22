from datetime import datetime, timezone

import pandas as pd

from src.common.audit import record_hash
from src.transform.curated import HASH_COLUMNS, build_curated, canonical
from src.validate.database import month_bounds, month_mask
from src.validate.quality import validate_curated

PROCESSED_AT = pd.Timestamp('2026-02-01T00:00:00Z')
UPDATED = pd.Timestamp('2025-03-02T10:00:00Z')


def sample_curated():
    customers = pd.DataFrame({'customer_id': ['C1'], 'city': ['Pasig'], 'customer_tier': ['Gold']})
    products = pd.DataFrame({'product_id': ['P1'], 'name': ['Widget'], 'brand': ['Nova'], 'category_name': ['Audio']})
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct', 'status', 'updated_at']
    rows = [
        ['O1', 'C1', 'P1', pd.Timestamp('2025-12-31T23:59:59Z'), 3, 100.5, 0.1, 'PAID', UPDATED],
        ['O2', 'C1', 'P1', pd.Timestamp('2026-01-01T00:00:00Z'), 1, 20.0, 0.0, 'DELIVERED', UPDATED],
    ]
    curated, _, _ = build_curated({'orders': pd.DataFrame(rows, columns=columns), 'customers': customers, 'products': products}, [], 'run_a', PROCESSED_AT)
    return curated


def test_month_bounds_roll_over_from_december_to_january():
    start, end = month_bounds(2025, 12)
    assert start == datetime(2025, 12, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert month_bounds(2026, 1)[1] == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_month_mask_uses_utc_boundaries():
    df = sample_curated()
    assert list(df.loc[month_mask(df, 2026, 1), 'order_id']) == ['O2']
    assert list(df.loc[month_mask(df, 2025, 12), 'order_id']) == ['O1']


def test_a_hash_computed_before_source_updated_at_joined_the_hash_is_rejected():
    df = sample_curated()
    old_columns = [column for column in HASH_COLUMNS if column != 'source_updated_at']
    df['record_hash'] = [
        record_hash({key: canonical(row[key]) for key in old_columns}, old_columns)
        for row in df[HASH_COLUMNS].to_dict('records')
    ]
    assert any('recomputed' in finding for finding in validate_curated(df))
