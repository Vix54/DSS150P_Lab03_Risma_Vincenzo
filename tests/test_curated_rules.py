from decimal import Decimal

import pandas as pd
import pytest

from src.transform.curated import build_curated

PROCESSED_AT = pd.Timestamp('2026-02-01T00:00:00Z')
ORDER_TIME = pd.Timestamp('2025-03-01T10:00:00Z')
UPDATED = pd.Timestamp('2025-03-02T10:00:00Z')


def customers_frame(city='Pasig'):
    return pd.DataFrame({'customer_id': ['C1'], 'city': [city], 'customer_tier': ['Gold']})


def products_frame():
    return pd.DataFrame({'product_id': ['P1'], 'name': ['Widget'], 'brand': ['Nova'], 'category_name': ['Audio']})


def orders_frame(*rows):
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct', 'status', 'updated_at']
    return pd.DataFrame(list(rows), columns=columns)


def order_row(order_id='O1', customer_id='C1', product_id='P1', quantity=3, unit_price=100.5, discount_pct=0.1, updated_at=UPDATED):
    return [order_id, customer_id, product_id, ORDER_TIME, quantity, unit_price, discount_pct, 'PAID', updated_at]


def build(orders, customers=None, quarantined_products=(), run_id='run_a', processed_at=PROCESSED_AT):
    staging = {'orders': orders, 'customers': customers if customers is not None else customers_frame(), 'products': products_frame()}
    return build_curated(staging, quarantined_products, run_id, processed_at)


def test_amounts_use_exact_decimals_and_net_is_gross_minus_discount():
    curated, _, _ = build(orders_frame(order_row(quantity=3, unit_price=100.5, discount_pct=0.1)))
    row = curated.iloc[0]
    assert row['gross_amount'] == Decimal('301.50')
    assert row['discount_amount'] == Decimal('30.15')
    assert row['net_amount'] == Decimal('271.35')


def test_discount_rounds_half_up_not_to_even():
    curated, _, _ = build(orders_frame(order_row(quantity=1, unit_price=10.10, discount_pct=0.05)))
    assert curated.iloc[0]['discount_amount'] == Decimal('0.51')
    assert curated.iloc[0]['net_amount'] == Decimal('9.59')


def test_orders_with_unknown_customer_or_product_are_quarantined_with_distinct_reasons():
    orders = orders_frame(
        order_row('O1'),
        order_row('O2', customer_id='C99'),
        order_row('O3', product_id='P99'),
        order_row('O4', product_id='P0'),
    )
    curated, quarantine, stats = build(orders, quarantined_products=['P0'])
    assert list(curated['order_id']) == ['O1']
    reasons = dict(zip(quarantine['business_key'], quarantine['reasons']))
    assert reasons == {
        'O2': 'order_customer_not_found',
        'O3': 'order_product_not_found',
        'O4': 'order_product_quarantined',
    }
    assert set(quarantine['stage']) == {'curated'}
    assert stats['orders_staged'] == stats['curated_rows'] + stats['quarantined_rows']


def test_record_hash_ignores_run_id_processing_time_and_source_updated_at():
    first, _, _ = build(orders_frame(order_row()))
    second, _, _ = build(
        orders_frame(order_row(updated_at=pd.Timestamp('2026-01-01T00:00:00Z'))),
        run_id='run_b',
        processed_at=pd.Timestamp('2026-03-01T00:00:00Z'),
    )
    assert first.iloc[0]['record_hash'] == second.iloc[0]['record_hash']
    assert first.iloc[0]['pipeline_run_id'] != second.iloc[0]['pipeline_run_id']


def test_record_hash_changes_when_business_content_changes():
    first, _, _ = build(orders_frame(order_row()))
    moved, _, _ = build(orders_frame(order_row()), customers=customers_frame(city='Makati'))
    repriced, _, _ = build(orders_frame(order_row(unit_price=101.5)))
    assert first.iloc[0]['record_hash'] != moved.iloc[0]['record_hash']
    assert first.iloc[0]['record_hash'] != repriced.iloc[0]['record_hash']


def test_duplicate_customer_rows_are_rejected_instead_of_multiplying_orders():
    duplicated = pd.concat([customers_frame(), customers_frame(city='Makati')], ignore_index=True)
    with pytest.raises(pd.errors.MergeError):
        build(orders_frame(order_row()), customers=duplicated)
