import json
import logging

import pandas as pd

from src.benchmark.partitions import MARKER_NAME, belongs_to_month, read_partition, write_partitioned_parquet
from src.common.errors import PipelineError
from src.common.runs import is_complete, run_dir
from src.config import path_for
from src.load.postgres import connect, load_partition
from src.transform.stages import CURATED_FILE
from src.validate.quality import validate_curated

STAGE = 'load-partition'
MAX_REPORTED = 5
logger = logging.getLogger(__name__)


def ensure_partitions(run_id):
    root = path_for('partition_dir')
    marker_path = root / MARKER_NAME
    if marker_path.is_file() and json.loads(marker_path.read_text(encoding='utf-8'))['run_id'] == run_id:
        return root
    curated_dir = run_dir('curated_dir', run_id)
    if not is_complete(curated_dir):
        raise PipelineError(STAGE, f'curated output {curated_dir.name} is missing or incomplete; run transform first')
    write_partitioned_parquet(pd.read_parquet(curated_dir / CURATED_FILE), root, run_id)
    logger.info('rebuilt the partitioned dataset from run %s', run_id)
    return root


def run_load_partition(year, month, run_id):
    root = ensure_partitions(run_id)
    frame = read_partition(root, year, month)
    if frame.empty:
        raise PipelineError(STAGE, f'partition {year:04d}-{month:02d} does not exist or has no rows')
    if not belongs_to_month(frame, year, month):
        raise PipelineError(STAGE, f'the rows read for {year:04d}-{month:02d} include orders from another month')
    findings = validate_curated(frame)
    if findings:
        raise PipelineError(
            STAGE,
            f'partition {year:04d}-{month:02d} fails the pre-load checks and was not loaded; remove data/partitioned and run again: '
            + ' | '.join(findings[:MAX_REPORTED]),
        )
    with connect(STAGE) as connection:
        outcome = load_partition(connection, frame, year, month, run_id)
    logger.info(
        'partition %04d-%02d loaded from run %s: rows_in=%d inserted=%d updated=%d unchanged=%d',
        year, month, run_id, outcome['rows_in'], outcome['inserted'], outcome['updated'], outcome['unchanged'],
    )
    return outcome
