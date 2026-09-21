import re

import pandas as pd

from src.config import SETTINGS
from src.transform.curated import HASH_COLUMNS, row_hash

ALLOWED_STATUSES = SETTINGS['quality']['allowed_order_statuses']
MIN_QUANTITY = SETTINGS['quality']['min_quantity']
MAX_QUANTITY = SETTINGS['quality']['max_quantity']
HASH_PATTERN = re.compile(r'^[0-9a-f]{64}$')
UTC_COLUMNS = ['order_timestamp', 'source_updated_at', 'processed_at_utc']
MAX_EXAMPLES = 3


def is_missing(value):
    return value is None or pd.isna(value)


def is_negative(value):
    return is_missing(value) or value < 0


def validate_curated(df):
    findings = []

    def check(rule, mask):
        count = int(mask.sum())
        if count:
            examples = ', '.join(str(value) for value in df.loc[mask, 'order_id'].head(MAX_EXAMPLES))
            findings.append(f'{rule}: {count} row(s), e.g. {examples}')

    check('V-01 order_id is null', df['order_id'].isna())
    check('V-01 order_id is duplicated', df['order_id'].duplicated(keep=False))
    check('V-02 customer_id is null', df['customer_id'].isna())
    check('V-02 product_id is null', df['product_id'].isna())
    quantity = pd.to_numeric(df['quantity'], errors='coerce')
    check(
        f'V-03 quantity is not an integer from {MIN_QUANTITY} to {MAX_QUANTITY}',
        quantity.isna() | (quantity % 1 != 0) | (quantity < MIN_QUANTITY) | (quantity > MAX_QUANTITY),
    )
    check('V-04 unit_price is null or negative', df['unit_price'].map(is_negative))
    check('V-05 discount_pct is null or outside 0 to 1', df['discount_pct'].map(lambda value: is_missing(value) or value < 0 or value > 1))
    for column in ['gross_amount', 'discount_amount', 'net_amount']:
        check(f'V-06 {column} is null or negative', df[column].map(is_negative))
    formula_broken = pd.Series(
        [
            is_missing(net) or is_missing(gross) or is_missing(discount) or net != gross - discount
            for gross, discount, net in zip(df['gross_amount'], df['discount_amount'], df['net_amount'])
        ],
        index=df.index,
    )
    check('V-07 net_amount differs from gross_amount - discount_amount', formula_broken)
    check('V-08 status is not an allowed value', ~df['status'].isin(ALLOWED_STATUSES))
    for column in UTC_COLUMNS:
        series = df[column]
        zone = series.dt.tz if pd.api.types.is_datetime64_any_dtype(series) else None
        if zone is None or str(zone) != 'UTC':
            findings.append(f'V-09 {column} is not a timezone-aware UTC timestamp')
        elif series.isna().any():
            findings.append(f'V-09 {column} contains null timestamps')
    check('V-10 pipeline_run_id is null', df['pipeline_run_id'].isna())
    check('V-10 processed_at_utc is null', df['processed_at_utc'].isna())
    hashes = df['record_hash']
    check('V-10 record_hash is not 64 lowercase hexadecimal characters', hashes.isna() | ~hashes.fillna('').map(lambda value: bool(HASH_PATTERN.match(value))))
    recomputed = pd.Series([row_hash(row) for row in df[HASH_COLUMNS].to_dict('records')], index=df.index)
    check('V-10 record_hash differs from the hash recomputed from the row', recomputed != hashes)
    return findings
