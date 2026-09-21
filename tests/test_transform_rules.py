import pandas as pd
import pytest

from src.common.errors import PipelineError
from src.transform.staging import stage_customers, stage_orders, stage_products

RUN_ID = 'run_test'
STAGED_AT = pd.Timestamp('2026-01-01T00:00:00Z')
OLD = '2025-01-01T00:00:00+00:00'
NEW = '2025-01-03T00:00:00+00:00'


def customers_frame(*rows):
    columns = ['customer_id', 'first_name', 'last_name', 'email', 'city', 'customer_tier', 'created_at', 'updated_at']
    return pd.DataFrame(list(rows), columns=columns, dtype=str)


def customer_row(customer_id='C1', email='a@example.com', city='Pasig', updated_at=NEW):
    return [customer_id, 'Ana', 'Cruz', email, city, 'Gold', '2024-01-01T00:00:00+00:00', updated_at]


def orders_frame(*rows):
    columns = ['order_id', 'customer_id', 'product_id', 'order_timestamp', 'quantity', 'unit_price', 'discount_pct', 'status', 'updated_at']
    return pd.DataFrame(list(rows), columns=columns, dtype=str)


def order_row(order_id='O1', quantity='2', status='PAID', updated_at=NEW):
    return [order_id, 'C1', 'P1', '2025-01-01T00:00:00+00:00', quantity, '100.50', '0.1', status, updated_at]


def product_record(product_id='P1', name='Widget', unit_price=10.0, updated_at=NEW):
    return {
        'product_id': product_id,
        'name': name,
        'category': {'name': 'Audio', 'department': 'Devices'},
        'brand': 'Nova',
        'unit_price': unit_price,
        'active': True,
        'updated_at': updated_at,
    }


def test_customers_keep_the_latest_version_and_normalize_text():
    raw = customers_frame(
        customer_row(email='Old@Example.com', city='pasig', updated_at=OLD),
        customer_row(email='  New@Example.COM ', city=' quezon city ', updated_at=NEW),
    )
    staged, quarantine, stats = stage_customers(raw, RUN_ID, STAGED_AT)
    assert len(staged) == 1 and quarantine.empty
    assert staged.loc[0, 'email'] == 'new@example.com'
    assert staged.loc[0, 'city'] == 'Quezon City'
    assert stats == {'raw_rows': 2, 'superseded_rows': 1, 'quarantined_rows': 0, 'staged_rows': 1}


def test_missing_email_is_kept_and_flagged():
    staged, quarantine, _ = stage_customers(customers_frame(customer_row(email='')), RUN_ID, STAGED_AT)
    assert len(staged) == 1 and quarantine.empty
    assert pd.isna(staged.loc[0, 'email'])
    assert bool(staged.loc[0, 'email_missing']) is True


def test_staging_adds_utc_timestamps_and_audit_columns():
    staged, _, _ = stage_customers(customers_frame(customer_row()), RUN_ID, STAGED_AT)
    assert str(staged['updated_at'].dtype) == 'datetime64[ns, UTC]'
    assert staged.loc[0, 'pipeline_run_id'] == RUN_ID
    assert staged.loc[0, 'staged_at_utc'] == STAGED_AT


def test_unparseable_timestamp_is_quarantined_with_a_reason():
    raw = customers_frame(customer_row(customer_id='C1'), customer_row(customer_id='C2', updated_at='not a date'))
    staged, quarantine, _ = stage_customers(raw, RUN_ID, STAGED_AT)
    assert list(staged['customer_id']) == ['C1']
    assert list(quarantine['business_key']) == ['C2']
    assert list(quarantine['reasons']) == ['timestamp_invalid']


def test_product_with_negative_price_is_quarantined_and_category_is_flattened():
    records = [product_record('P1'), product_record('P2', unit_price=-5.0)]
    staged, quarantine, stats = stage_products(records, RUN_ID, STAGED_AT)
    assert list(staged['product_id']) == ['P1']
    assert staged.loc[0, 'category_name'] == 'Audio'
    assert staged.loc[0, 'category_department'] == 'Devices'
    assert list(quarantine['reasons']) == ['product_unit_price_invalid']
    assert stats == {'raw_rows': 2, 'superseded_rows': 0, 'quarantined_rows': 1, 'staged_rows': 1}


def test_product_latest_version_wins():
    records = [product_record(name='Old name', updated_at=OLD), product_record(name='New name', updated_at=NEW)]
    staged, _, _ = stage_products(records, RUN_ID, STAGED_AT)
    assert list(staged['name']) == ['New name']


def test_order_failing_two_rules_gets_both_reasons_in_one_quarantine_row():
    raw = orders_frame(
        order_row('O1'),
        order_row('O2', quantity='0'),
        order_row('O3', status='UNKNOWN'),
        order_row('O4', quantity='21', status='UNKNOWN'),
    )
    staged, quarantine, stats = stage_orders(raw, RUN_ID, STAGED_AT)
    assert list(staged['order_id']) == ['O1']
    reasons = dict(zip(quarantine['business_key'], quarantine['reasons']))
    assert reasons['O2'] == 'order_quantity_invalid'
    assert reasons['O3'] == 'order_status_not_allowed'
    assert reasons['O4'] == 'order_quantity_invalid;order_status_not_allowed'
    assert stats['raw_rows'] == stats['superseded_rows'] + stats['quarantined_rows'] + stats['staged_rows']


def test_an_invalid_older_version_is_superseded_not_quarantined():
    raw = orders_frame(order_row(quantity='0', updated_at=OLD), order_row(quantity='5', updated_at=NEW))
    staged, quarantine, stats = stage_orders(raw, RUN_ID, STAGED_AT)
    assert list(staged['quantity']) == [5]
    assert quarantine.empty
    assert stats['superseded_rows'] == 1


def test_a_tie_on_the_greatest_updated_at_raises_instead_of_guessing():
    raw = orders_frame(order_row(status='PAID', updated_at=NEW), order_row(status='SHIPPED', updated_at=NEW))
    with pytest.raises(PipelineError, match='share the greatest updated_at'):
        stage_orders(raw, RUN_ID, STAGED_AT)
