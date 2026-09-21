import logging

import pandas as pd
import pyarrow as pa

from src.common.errors import PipelineError
from src.common.runs import discard_incomplete, is_complete, run_dir, write_success_marker
from src.transform.curated import build_curated
from src.transform.staging import build_staging

STAGE = 'transform'
CURATED_FILE = 'sales_order_lines.parquet'
CURATED_SCHEMA = pa.schema([
    ('order_id', pa.string()),
    ('customer_id', pa.string()),
    ('product_id', pa.string()),
    ('order_timestamp', pa.timestamp('us', tz='UTC')),
    ('customer_city', pa.string()),
    ('customer_tier', pa.string()),
    ('product_name', pa.string()),
    ('category', pa.string()),
    ('brand', pa.string()),
    ('quantity', pa.int32()),
    ('unit_price', pa.decimal128(14, 2)),
    ('discount_pct', pa.decimal128(6, 4)),
    ('gross_amount', pa.decimal128(16, 2)),
    ('discount_amount', pa.decimal128(16, 2)),
    ('net_amount', pa.decimal128(16, 2)),
    ('status', pa.string()),
    ('source_updated_at', pa.timestamp('us', tz='UTC')),
    ('pipeline_run_id', pa.string()),
    ('processed_at_utc', pa.timestamp('us', tz='UTC')),
    ('record_hash', pa.string()),
])
logger = logging.getLogger(__name__)


def write_parquet(frame, path, schema=None):
    frame.to_parquet(path, engine='pyarrow', index=False, schema=schema)


def run_staging(run_id):
    raw_dir = run_dir('raw_dir', run_id)
    if not is_complete(raw_dir):
        raise PipelineError(STAGE, f'raw snapshot {raw_dir.name} is missing or incomplete; run extract with the same run id first')
    staging_dir = run_dir('staging_dir', run_id)
    if is_complete(staging_dir):
        logger.info('staging %s is already complete; nothing to do', staging_dir.name)
        return staging_dir
    if discard_incomplete(staging_dir):
        logger.warning('removed incomplete staging output %s before rebuilding it', staging_dir.name)
    staging, quarantine, stats = build_staging(raw_dir, run_id)
    quarantine_dir = run_dir('quarantine_dir', run_id)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(quarantine, quarantine_dir / 'staging.parquet')
    staging_dir.mkdir(parents=True)
    for name, frame in staging.items():
        write_parquet(frame, staging_dir / f'{name}.parquet')
    write_success_marker(staging_dir, {'run_id': run_id, 'stage': 'staging', 'counts': stats})
    for source, counts in stats.items():
        logger.info(
            '%s: raw=%d superseded=%d quarantined=%d staged=%d',
            source, counts['raw_rows'], counts['superseded_rows'], counts['quarantined_rows'], counts['staged_rows'],
        )
    return staging_dir


def run_curated(run_id):
    staging_dir = run_dir('staging_dir', run_id)
    if not is_complete(staging_dir):
        raise PipelineError(STAGE, f'staging output {staging_dir.name} is missing or incomplete; run the staging step first')
    curated_dir = run_dir('curated_dir', run_id)
    if is_complete(curated_dir):
        logger.info('curated %s is already complete; nothing to do', curated_dir.name)
        return curated_dir
    if discard_incomplete(curated_dir):
        logger.warning('removed incomplete curated output %s before rebuilding it', curated_dir.name)
    quarantine_dir = run_dir('quarantine_dir', run_id)
    staging_quarantine = pd.read_parquet(quarantine_dir / 'staging.parquet')
    quarantined_products = staging_quarantine.loc[staging_quarantine['source'] == 'products', 'business_key']
    staging = {name: pd.read_parquet(staging_dir / f'{name}.parquet') for name in ['customers', 'products', 'orders']}
    processed_at = pd.Timestamp.now(tz='UTC')
    curated, quarantine, stats = build_curated(staging, quarantined_products, run_id, processed_at)
    write_parquet(quarantine, quarantine_dir / 'curated.parquet')
    curated_dir.mkdir(parents=True)
    write_parquet(curated, curated_dir / CURATED_FILE, schema=CURATED_SCHEMA)
    write_success_marker(curated_dir, {'run_id': run_id, 'stage': 'curated', 'counts': stats})
    logger.info(
        'curated: orders_staged=%d curated=%d quarantined=%d reasons=%s',
        stats['orders_staged'], stats['curated_rows'], stats['quarantined_rows'], stats['quarantined_by_reason'],
    )
    return curated_dir
