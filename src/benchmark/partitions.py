import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.transform.curated import CURATED_COLUMNS
from src.transform.stages import CURATED_SCHEMA

MARKER_NAME = '_SUCCESS.json'
PARTITION_COLUMNS = ['order_year', 'order_month']
PARTITION_SCHEMA = CURATED_SCHEMA.append(pa.field('order_year', pa.int32())).append(pa.field('order_month', pa.int32()))


def add_partition_columns(df):
    stamps = pd.to_datetime(df['order_timestamp'], utc=True)
    partitioned = df.copy()
    partitioned['order_year'] = stamps.dt.year.astype('int32')
    partitioned['order_month'] = stamps.dt.month.astype('int32')
    return partitioned


def write_partitioned_parquet(df, output_dir, run_id):
    root = Path(output_dir)
    if root.exists():
        shutil.rmtree(root)
    partitioned = add_partition_columns(df)
    table = pa.Table.from_pandas(partitioned, schema=PARTITION_SCHEMA, preserve_index=False)
    pq.write_to_dataset(table, root_path=str(root), partition_cols=PARTITION_COLUMNS, basename_template='part-{i}.parquet')
    counts = partitioned.groupby(PARTITION_COLUMNS).size().reset_index(name='rows')
    summary = {
        'run_id': run_id,
        'written_at_utc': datetime.now(timezone.utc).isoformat(),
        'rows': int(len(df)),
        'partitions': [
            {'order_year': int(row.order_year), 'order_month': int(row.order_month), 'rows': int(row.rows)}
            for row in counts.itertuples(index=False)
        ],
    }
    (root / MARKER_NAME).write_text(json.dumps(summary, indent=2), encoding='utf-8')
    return summary


def read_partition(root, year, month):
    frame = pd.read_parquet(root, filters=[('order_year', '==', year), ('order_month', '==', month)])
    return frame[CURATED_COLUMNS].reset_index(drop=True)


def read_all_partitions(root):
    return pd.read_parquet(root)


def belongs_to_month(frame, year, month):
    stamps = pd.to_datetime(frame['order_timestamp'], utc=True)
    return bool(((stamps.dt.year == year) & (stamps.dt.month == month)).all())


def parquet_files(root, year=None, month=None):
    base = Path(root)
    if year is not None:
        base = base / f'order_year={year}' / f'order_month={month}'
    return sorted(base.rglob('*.parquet'))


def describe_tree(root):
    base = Path(root)
    return [str(path.relative_to(base)) for path in parquet_files(base)]
