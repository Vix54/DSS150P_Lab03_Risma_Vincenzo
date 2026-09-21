from decimal import Decimal

import pandas as pd

from src.transform.curated import build_curated
from src.validate.quality import validate_curated

PROCESSED_AT = pd.Timestamp('2026-02-01T00:00:00Z')
ORDER_TIME = pd.Timestamp('2025-03-01T10:00:00Z')
UPDATED = pd.Timestamp('2025-03-02T10:00:00Z')


def valid_curated():
    customers = pd.DataFrame({'customer_id': ['C1'], 'city': ['Pasig'], 'customer_tier': ['Gold']})
    products = pd.DataFrame({'product_id': ['P1'], 'name': ['Widget'], 'brand': ['Nova'], 'category_name': ['Audio']})
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct', 'status', 'updated_at']
    rows = [
        ['O1', 'C1', 'P1', ORDER_TIME, 3, 100.5, 0.1, 'PAID', UPDATED],
        ['O2', 'C1', 'P1', ORDER_TIME, 1, 20.0, 0.0, 'SHIPPED', UPDATED],
    ]
    orders = pd.DataFrame(rows, columns=columns)
    curated, _, _ = build_curated({'orders': orders, 'customers': customers, 'products': products}, [], 'run_a', PROCESSED_AT)
    return curated


def rules_reported(findings):
    return {finding.split(' ')[0] for finding in findings}


def test_valid_curated_output_has_no_findings():
    assert validate_curated(valid_curated()) == []


def test_quantity_outside_the_configured_range_is_reported():
    df = valid_curated()
    df.loc[0, 'quantity'] = 0
    assert 'V-03' in rules_reported(validate_curated(df))


def test_status_that_is_not_allowed_is_reported():
    df = valid_curated()
    df.loc[0, 'status'] = 'UNKNOWN'
    assert 'V-08' in rules_reported(validate_curated(df))


def test_net_amount_that_breaks_the_formula_is_reported():
    df = valid_curated()
    df.loc[0, 'net_amount'] = df.loc[0, 'net_amount'] + Decimal('1.00')
    assert 'V-07' in rules_reported(validate_curated(df))


def test_negative_unit_price_is_reported():
    df = valid_curated()
    df.loc[0, 'unit_price'] = Decimal('-1.00')
    assert 'V-04' in rules_reported(validate_curated(df))


def test_duplicate_order_id_is_reported():
    df = valid_curated()
    df.loc[1, 'order_id'] = df.loc[0, 'order_id']
    assert 'V-01' in rules_reported(validate_curated(df))


def test_a_hash_that_does_not_match_the_row_is_reported():
    df = valid_curated()
    df.loc[0, 'record_hash'] = '0' * 64
    findings = validate_curated(df)
    assert any('recomputed' in finding for finding in findings)


def test_a_timestamp_without_a_timezone_is_reported():
    df = valid_curated()
    df['source_updated_at'] = df['source_updated_at'].dt.tz_localize(None)
    assert any(finding.startswith('V-09 source_updated_at') for finding in validate_curated(df))
