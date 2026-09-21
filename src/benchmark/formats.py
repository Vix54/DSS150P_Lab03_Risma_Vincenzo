from decimal import Decimal

import pandas as pd

from src.transform.curated import HASH_COLUMNS, row_hash
from src.transform.stages import CURATED_SCHEMA

MONEY_COLUMNS = ['unit_price', 'discount_pct', 'gross_amount', 'discount_amount', 'net_amount']
TIMESTAMP_COLUMNS = ['order_timestamp', 'source_updated_at', 'processed_at_utc']


def write_csv(df, path):
    df.to_csv(path, index=False)


def write_jsonl(df, path):
    frame = df.copy()
    for column in MONEY_COLUMNS:
        frame[column] = frame[column].map(lambda value: format(value, 'f'))
    frame.to_json(path, orient='records', lines=True, date_format='iso', date_unit='us')


def write_parquet(df, path, compression):
    df.to_parquet(path, engine='pyarrow', index=False, compression=compression, schema=CURATED_SCHEMA)


def read_csv_full(path):
    return pd.read_csv(path)


def read_jsonl_full(path):
    return pd.read_json(path, lines=True, dtype=False, convert_dates=False)


def read_parquet_full(path):
    return pd.read_parquet(path)


def read_csv_filtered(path, column, value):
    frame = pd.read_csv(path)
    return frame[frame[column] == value]


def read_jsonl_filtered(path, column, value):
    frame = read_jsonl_full(path)
    return frame[frame[column] == value]


def read_parquet_filtered(path, column, value):
    return pd.read_parquet(path, filters=[(column, '==', value)])


def read_for_verification(path, kind):
    if kind == 'csv':
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if kind == 'jsonl':
        return read_jsonl_full(path)
    return pd.read_parquet(path)


def restore_types(frame):
    restored = frame.copy()
    for column in MONEY_COLUMNS:
        restored[column] = restored[column].map(lambda value: Decimal(str(value)))
    for column in TIMESTAMP_COLUMNS:
        restored[column] = pd.to_datetime(restored[column], utc=True, format='ISO8601')
    restored['quantity'] = pd.to_numeric(restored['quantity']).astype('int64')
    return restored


def count_hash_mismatches(frame):
    restored = restore_types(frame)
    recomputed = pd.Series([row_hash(row) for row in restored[HASH_COLUMNS].to_dict('records')], index=restored.index)
    return int((recomputed != restored['record_hash']).sum())


def same_row_set(reference, frame):
    return len(reference) == len(frame) and set(reference['order_id']) == set(frame['order_id'])
