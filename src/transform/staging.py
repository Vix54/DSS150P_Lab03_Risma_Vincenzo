import json

import pandas as pd

from src.common.errors import PipelineError
from src.config import SETTINGS

STAGE = 'transform'
ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QUANTITY = SETTINGS['quality']['min_quantity']
MAX_QUANTITY = SETTINGS['quality']['max_quantity']
QUARANTINE_COLUMNS = [
    'source',
    'business_key',
    'reasons',
    'stage',
    'record_json',
    'pipeline_run_id',
    'quarantined_at_utc',
]


def parse_utc(series):
    return pd.to_datetime(series, errors='coerce', utc=True, format='ISO8601')


def is_blank(series):
    return series.isna() | (series.astype(str).str.strip() == '')


def join_reasons(index, rules):
    reasons = pd.Series('', index=index, dtype='object')
    for code, mask in rules:
        reasons = reasons.where(~mask, reasons + code + ';')
    return reasons.str.rstrip(';')


def quarantine_rows(source, raw_rows, key, reasons, run_id, stamped_at):
    flagged = reasons.index[reasons != '']
    subset = raw_rows.loc[flagged]
    return pd.DataFrame({
        'source': source,
        'business_key': subset[key].astype(str).values,
        'reasons': reasons.loc[flagged].values,
        'stage': 'staging',
        'record_json': [json.dumps(record, default=str, sort_keys=True) for record in subset.to_dict('records')],
        'pipeline_run_id': run_id,
        'quarantined_at_utc': stamped_at,
    }, columns=QUARANTINE_COLUMNS)


def keep_latest(df, key, updated_col, source):
    latest_stamp = df.groupby(key)[updated_col].transform('max')
    at_latest = df[df[updated_col] == latest_stamp]
    tied = at_latest.loc[at_latest.duplicated(key, keep=False), key]
    if not tied.empty:
        keys = ', '.join(tied.astype(str).unique()[:10])
        raise PipelineError(STAGE, f'{source}: several versions share the greatest updated_at for key(s) {keys}')
    return at_latest


def build_stats(source, raw_rows, superseded, quarantined, staged):
    if raw_rows != superseded + quarantined + staged:
        raise PipelineError(STAGE, f'{source}: row counts do not balance ({raw_rows} != {superseded} + {quarantined} + {staged})')
    return {'raw_rows': raw_rows, 'superseded_rows': superseded, 'quarantined_rows': quarantined, 'staged_rows': staged}


def stage_customers(df_raw, run_id, staged_at):
    df = df_raw.copy()
    df['created_at_parsed'] = parse_utc(df['created_at'])
    df['updated_at_parsed'] = parse_utc(df['updated_at'])
    technical = join_reasons(df.index, [
        ('business_key_missing', is_blank(df['customer_id'])),
        ('timestamp_invalid', df['created_at_parsed'].isna() | df['updated_at_parsed'].isna()),
    ])
    quarantine = quarantine_rows('customers', df_raw, 'customer_id', technical, run_id, staged_at)
    candidates = df[technical == '']
    latest = keep_latest(candidates, 'customer_id', 'updated_at_parsed', 'customers')
    email = latest['email'].str.strip().str.lower()
    staged = pd.DataFrame({
        'customer_id': latest['customer_id'],
        'first_name': latest['first_name'],
        'last_name': latest['last_name'],
        'email': email.where(email != '', None),
        'email_missing': email == '',
        'city': latest['city'].str.strip().str.title(),
        'customer_tier': latest['customer_tier'],
        'created_at': latest['created_at_parsed'],
        'updated_at': latest['updated_at_parsed'],
        'pipeline_run_id': run_id,
        'staged_at_utc': staged_at,
    }).reset_index(drop=True)
    stats = build_stats('customers', len(df_raw), len(candidates) - len(latest), len(quarantine), len(staged))
    return staged, quarantine, stats


def stage_products(records, run_id, staged_at):
    df_raw = pd.DataFrame(records)
    df = df_raw.copy()
    df['updated_at_parsed'] = parse_utc(df['updated_at'])
    technical = join_reasons(df.index, [
        ('business_key_missing', is_blank(df['product_id'])),
        ('timestamp_invalid', df['updated_at_parsed'].isna()),
    ])
    quarantine_technical = quarantine_rows('products', df_raw, 'product_id', technical, run_id, staged_at)
    candidates = df[technical == '']
    latest = keep_latest(candidates, 'product_id', 'updated_at_parsed', 'products')
    price = pd.to_numeric(latest['unit_price'], errors='coerce')
    value_reasons = join_reasons(latest.index, [('product_unit_price_invalid', price.isna() | (price < 0))])
    quarantine_values = quarantine_rows('products', df_raw, 'product_id', value_reasons, run_id, staged_at)
    valid = latest[value_reasons == '']
    category = valid['category']
    staged = pd.DataFrame({
        'product_id': valid['product_id'],
        'name': valid['name'],
        'brand': valid['brand'],
        'category_name': category.map(lambda item: item.get('name') if isinstance(item, dict) else None),
        'category_department': category.map(lambda item: item.get('department') if isinstance(item, dict) else None),
        'unit_price': price.loc[valid.index],
        'active': valid['active'],
        'updated_at': valid['updated_at_parsed'],
        'pipeline_run_id': run_id,
        'staged_at_utc': staged_at,
    }).reset_index(drop=True)
    quarantine = pd.concat([quarantine_technical, quarantine_values], ignore_index=True)
    stats = build_stats('products', len(df_raw), len(candidates) - len(latest), len(quarantine), len(staged))
    return staged, quarantine, stats


def stage_orders(df_raw, run_id, staged_at):
    df = df_raw.copy()
    df['order_timestamp_parsed'] = parse_utc(df['order_timestamp'])
    df['updated_at_parsed'] = parse_utc(df['updated_at'])
    technical = join_reasons(df.index, [
        ('business_key_missing', is_blank(df['order_id'])),
        ('timestamp_invalid', df['order_timestamp_parsed'].isna() | df['updated_at_parsed'].isna()),
    ])
    quarantine_technical = quarantine_rows('orders', df_raw, 'order_id', technical, run_id, staged_at)
    candidates = df[technical == '']
    latest = keep_latest(candidates, 'order_id', 'updated_at_parsed', 'orders')
    quantity = pd.to_numeric(latest['quantity'], errors='coerce')
    unit_price = pd.to_numeric(latest['unit_price'], errors='coerce')
    discount = pd.to_numeric(latest['discount_pct'], errors='coerce')
    quantity_ok = quantity.notna() & (quantity % 1 == 0) & quantity.between(MIN_QUANTITY, MAX_QUANTITY)
    value_reasons = join_reasons(latest.index, [
        ('order_quantity_invalid', ~quantity_ok),
        ('order_status_not_allowed', ~latest['status'].isin(ALLOWED_STATUSES)),
        ('order_amount_field_invalid', unit_price.isna() | discount.isna()),
    ])
    quarantine_values = quarantine_rows('orders', df_raw, 'order_id', value_reasons, run_id, staged_at)
    valid = latest[value_reasons == '']
    staged = pd.DataFrame({
        'order_id': valid['order_id'],
        'customer_id': valid['customer_id'],
        'product_id': valid['product_id'],
        'order_timestamp': valid['order_timestamp_parsed'],
        'quantity': quantity.loc[valid.index].astype('int64'),
        'unit_price': unit_price.loc[valid.index],
        'discount_pct': discount.loc[valid.index],
        'status': valid['status'],
        'updated_at': valid['updated_at_parsed'],
        'pipeline_run_id': run_id,
        'staged_at_utc': staged_at,
    }).reset_index(drop=True)
    quarantine = pd.concat([quarantine_technical, quarantine_values], ignore_index=True)
    stats = build_stats('orders', len(df_raw), len(candidates) - len(latest), len(quarantine), len(staged))
    return staged, quarantine, stats


def build_staging(raw_dir, run_id):
    staged_at = pd.Timestamp.now(tz='UTC')
    customers = pd.read_csv(raw_dir / 'customers.csv', dtype=str, keep_default_na=False)
    orders = pd.read_csv(raw_dir / 'orders.csv', dtype=str, keep_default_na=False)
    with (raw_dir / 'products.json').open(encoding='utf-8') as handle:
        products = json.load(handle)
    staged_customers, quarantine_customers, stats_customers = stage_customers(customers, run_id, staged_at)
    staged_products, quarantine_products, stats_products = stage_products(products, run_id, staged_at)
    staged_orders, quarantine_orders, stats_orders = stage_orders(orders, run_id, staged_at)
    staging = {'customers': staged_customers, 'products': staged_products, 'orders': staged_orders}
    quarantine = pd.concat([quarantine_customers, quarantine_products, quarantine_orders], ignore_index=True)
    stats = {'customers': stats_customers, 'products': stats_products, 'orders': stats_orders}
    return staging, quarantine, stats
