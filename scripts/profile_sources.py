import json
from collections import Counter
from datetime import datetime, timezone
from itertools import islice

import pandas as pd

from src.config import SETTINGS, path_for

SOURCE_DIR = path_for('source_dir')
ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QUANTITY = SETTINGS['quality']['min_quantity']
MAX_QUANTITY = SETTINGS['quality']['max_quantity']
SAMPLE_SIZE = 5
MAX_GROUPS_SHOWN = 15


def section(title):
    print()
    print('=' * 72)
    print(title)
    print('=' * 72)


def load_csv(file_name):
    return pd.read_csv(SOURCE_DIR / file_name, dtype=str, keep_default_na=False)


def parse_utc(series):
    return pd.to_datetime(series, errors='coerce', utc=True, format='ISO8601')


def profile_keys(df, key, updated_col, label):
    total_rows = len(df)
    distinct_keys = df[key].nunique()
    shares_key = df[key].duplicated(keep=False)
    identical_extra = int(df.duplicated(keep='first').sum())
    print(f'-- {label}: business key {key}')
    print(f'   physical_rows={total_rows}')
    print(f'   distinct_keys={distinct_keys}')
    print(f'   extra_rows_beyond_first_per_key={total_rows - distinct_keys}')
    print(f'   rows_sharing_a_key_with_another_row={int(shares_key.sum())}')
    print(f'   extra_rows_identical_to_an_earlier_row={identical_extra}')
    groups = df[shares_key].groupby(key, sort=True)
    for key_value, group in islice(groups, MAX_GROUPS_SHOWN):
        differing = [c for c in group.columns if group[c].nunique() > 1]
        parsed = parse_utc(group[updated_col])
        rows_at_latest = int((parsed == parsed.max()).sum()) if parsed.notna().any() else 0
        print(f'   {key_value}: rows={len(group)}, differing_columns={differing}, '
              f'rows_at_latest_updated_at={rows_at_latest}, updated_at_values={list(group[updated_col])}')


def profile_blanks(df, label):
    print(f'-- {label}: blank values and outer whitespace per column')
    for column in df.columns:
        text = df[column].astype(str)
        blank = int((text.str.strip() == '').sum())
        padded = int((text != text.str.strip()).sum())
        print(f'   {column}: blank={blank}, outer_whitespace={padded}')


def profile_timestamps(df, column, label):
    text = df[column].astype(str)
    present = text.str.strip() != ''
    iso = parse_utc(text)
    lenient = pd.to_datetime(text, errors='coerce', utc=True, format='mixed')
    not_iso = int((present & iso.isna()).sum())
    unparseable = int((present & lenient.isna()).sum())
    offsets = text.str.extract(r'(Z|[+-]\d{2}:?\d{2})$')[0].fillna('no_offset').value_counts().to_dict()
    print(f'-- {label}.{column}: present={int(present.sum())}, not_iso8601={not_iso}, unparseable={unparseable}')
    print(f'   offsets={offsets}')
    print(f'   min={lenient.min()}, max={lenient.max()}')


def profile_numeric(raw, label):
    numeric = pd.to_numeric(raw, errors='coerce')
    present = raw.notna() & (raw.astype(str).str.strip() != '')
    non_numeric = int((present & numeric.isna()).sum())
    print(f'-- {label}: non_numeric={non_numeric}, min={numeric.min()}, max={numeric.max()}')
    return numeric


def profile_customers():
    section('customers.csv')
    df_customers = load_csv('customers.csv')
    profile_keys(df_customers, 'customer_id', 'updated_at', 'customers')
    profile_blanks(df_customers, 'customers')
    profile_timestamps(df_customers, 'created_at', 'customers')
    profile_timestamps(df_customers, 'updated_at', 'customers')
    created = parse_utc(df_customers['created_at'])
    updated = parse_utc(df_customers['updated_at'])
    print(f'-- customers: updated_at_before_created_at={int((updated < created).sum())}')
    email = df_customers['email']
    present = email.str.strip() != ''
    no_at_sign = present & ~email.str.contains('@', regex=False)
    print(f'-- customers.email: no_at_sign={int(no_at_sign.sum())}, differs_from_lowercase={int((email != email.str.lower()).sum())}')
    city = df_customers['city']
    normalized_city = city.str.strip().str.title()
    print(f'-- customers.city: distinct_as_written={city.nunique()}, distinct_after_trim_title={normalized_city.nunique()}')
    print(f'   values_after_trim_title={normalized_city.value_counts().head(15).to_dict()}')
    print(f'-- customers.customer_tier counts: {df_customers["customer_tier"].value_counts().to_dict()}')
    return df_customers


def profile_products():
    section('products.json')
    with (SOURCE_DIR / 'products.json').open(encoding='utf-8') as f:
        records = json.load(f)
    print(f'-- top_level_type={type(records).__name__}, records={len(records)}')
    key_sets = Counter(tuple(sorted(record.keys())) for record in records)
    for key_set, count in key_sets.items():
        print(f'   key_set={list(key_set)}, records={count}')
    fields = sorted({field for record in records for field in record})
    for field in fields:
        types = Counter('missing' if field not in record else type(record[field]).__name__ for record in records)
        print(f'   python_type_of_{field}={dict(types)}')
    nested = Counter(tuple(sorted(record['category'].keys())) for record in records if isinstance(record.get('category'), dict))
    print(f'-- nested category key sets: {dict(nested)}')
    df_products = pd.json_normalize(records)
    df_text = df_products.astype(object).where(df_products.notna(), '').astype(str)
    profile_keys(df_text, 'product_id', 'updated_at', 'products')
    profile_blanks(df_text, 'products')
    profile_timestamps(df_text, 'updated_at', 'products')
    price = profile_numeric(df_products['unit_price'], 'products.unit_price')
    not_positive = price <= 0
    offenders = list(zip(df_products.loc[not_positive, 'product_id'].head(SAMPLE_SIZE), df_products.loc[not_positive, 'unit_price'].head(SAMPLE_SIZE)))
    print(f'   not_positive={int(not_positive.sum())}, sample={offenders}')
    print(f'-- products.category.name counts: {df_text["category.name"].value_counts().head(15).to_dict()}')
    print(f'-- products.category.department counts: {df_text["category.department"].value_counts().head(15).to_dict()}')
    return df_products


def profile_orders():
    section('orders.csv')
    df_orders = load_csv('orders.csv')
    profile_keys(df_orders, 'order_id', 'updated_at', 'orders')
    profile_blanks(df_orders, 'orders')
    profile_timestamps(df_orders, 'order_timestamp', 'orders')
    profile_timestamps(df_orders, 'updated_at', 'orders')
    quantity = profile_numeric(df_orders['quantity'], 'orders.quantity')
    invalid_quantity = quantity.isna() | (quantity % 1 != 0) | (quantity < MIN_QUANTITY) | (quantity > MAX_QUANTITY)
    offenders = list(zip(df_orders.loc[invalid_quantity, 'order_id'].head(SAMPLE_SIZE), df_orders.loc[invalid_quantity, 'quantity'].head(SAMPLE_SIZE)))
    print(f'   invalid_quantity_rows={int(invalid_quantity.sum())} (missing, fractional or outside {MIN_QUANTITY}..{MAX_QUANTITY}), sample={offenders}')
    unit_price = profile_numeric(df_orders['unit_price'], 'orders.unit_price')
    print(f'   not_positive={int((unit_price <= 0).sum())}')
    discount = profile_numeric(df_orders['discount_pct'], 'orders.discount_pct')
    print(f'   outside_0_to_1={int(((discount < 0) | (discount > 1)).sum())}, distinct_values={sorted(discount.dropna().unique().tolist())[:20]}')
    status = df_orders['status']
    print(f'-- orders.status counts: {status.value_counts().to_dict()}')
    outside_as_written = ~status.isin(ALLOWED_STATUSES)
    outside_after_trim_upper = ~status.str.strip().str.upper().isin(ALLOWED_STATUSES)
    offenders = list(zip(df_orders.loc[outside_after_trim_upper, 'order_id'].head(SAMPLE_SIZE), df_orders.loc[outside_after_trim_upper, 'status'].head(SAMPLE_SIZE)))
    print(f'   not_in_allowed_as_written={int(outside_as_written.sum())}, not_in_allowed_after_trim_upper={int(outside_after_trim_upper.sum())}, sample={offenders}')
    ordered = parse_utc(df_orders['order_timestamp'])
    updated = parse_utc(df_orders['updated_at'])
    print(f'-- orders: updated_at_before_order_timestamp={int((updated < ordered).sum())}')
    months = ordered.dt.strftime('%Y-%m').value_counts().sort_index()
    print(f'-- orders per year-month of order_timestamp (UTC): {months.to_dict()}')
    return df_orders


def profile_cross_source(df_customers, df_orders, df_products):
    section('cross-source checks')
    references = [
        ('customer_id', df_customers['customer_id'], 'customers'),
        ('product_id', df_products['product_id'], 'products'),
    ]
    for column, known, target in references:
        known_ids = set(known)
        normalized_known = {str(value).strip().upper() for value in known_ids}
        orphan = ~df_orders[column].isin(known_ids)
        recoverable = orphan & df_orders[column].str.strip().str.upper().isin(normalized_known)
        offenders = list(df_orders.loc[orphan, ['order_id', column]].head(SAMPLE_SIZE).itertuples(index=False, name=None))
        print(f'-- orders.{column} not found in {target}: rows={int(orphan.sum())}, distinct_ids={df_orders.loc[orphan, column].nunique()}, '
              f'match_after_trim_upper={int(recoverable.sum())}, sample={offenders}')
    latest_products = (
        df_products.assign(product_updated=parse_utc(df_products['updated_at']))
        .sort_values(['product_id', 'product_updated'])
        .drop_duplicates('product_id', keep='last')
        .rename(columns={'unit_price': 'product_unit_price'})
    )
    merged = df_orders.merge(latest_products[['product_id', 'product_unit_price', 'product_updated']], on='product_id', how='inner')
    order_price = pd.to_numeric(merged['unit_price'], errors='coerce')
    product_price = pd.to_numeric(merged['product_unit_price'], errors='coerce')
    comparable = order_price.notna() & product_price.notna()
    differs = comparable & ((order_price - product_price).abs() > 0.005)
    order_before_product_update = differs & (parse_utc(merged['order_timestamp']) < merged['product_updated'])
    difference_products = merged.loc[differs, ['product_id', 'product_unit_price']].drop_duplicates().head(SAMPLE_SIZE)
    print('-- orders.unit_price vs latest products.unit_price (matched on product_id)')
    print(f'   orders_with_a_matching_product={len(merged)}, comparable={int(comparable.sum())}')
    print(f'   same_price={int((comparable & ~differs).sum())}, different_price={int(differs.sum())}')
    print(f'   distinct_products_among_differences={merged.loc[differs, "product_id"].nunique()}')
    print(f'   differences_where_product_price_not_positive={int((differs & (product_price <= 0)).sum())}')
    print(f'   differences_where_order_precedes_product_update={int(order_before_product_update.sum())}')
    print(f'   products_among_differences (id, latest product price)={list(difference_products.itertuples(index=False, name=None))}')


def main():
    print(f'profiling_run_at_utc={datetime.now(timezone.utc).isoformat()}')
    print(f'source_dir={SOURCE_DIR}')
    df_customers = profile_customers()
    df_products = profile_products()
    df_orders = profile_orders()
    profile_cross_source(df_customers, df_orders, df_products)


if __name__ == '__main__':
    main()
