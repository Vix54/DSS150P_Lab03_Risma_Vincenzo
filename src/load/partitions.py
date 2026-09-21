import json
import logging

from src.benchmark.partitions import MARKER_NAME, belongs_to_month, read_partition
from src.common.errors import PipelineError
from src.config import path_for
from src.load.postgres import connect, load_partition

STAGE = 'load-partition'
logger = logging.getLogger(__name__)


def run_load_partition(year, month):
    root = path_for('partition_dir')
    marker_path = root / MARKER_NAME
    if not marker_path.is_file():
        raise PipelineError(STAGE, f'no partitioned dataset found in {root.name}; run benchmark first')
    marker = json.loads(marker_path.read_text(encoding='utf-8'))
    frame = read_partition(root, year, month)
    if frame.empty:
        raise PipelineError(STAGE, f'partition {year:04d}-{month:02d} does not exist or has no rows')
    if not belongs_to_month(frame, year, month):
        raise PipelineError(STAGE, f'the rows read for {year:04d}-{month:02d} include orders from another month')
    with connect(STAGE) as connection:
        outcome = load_partition(connection, frame, year, month, marker['run_id'])
    logger.info(
        'partition %04d-%02d loaded from run %s: rows_in=%d inserted=%d updated=%d unchanged=%d',
        year, month, marker['run_id'], outcome['rows_in'], outcome['inserted'], outcome['updated'], outcome['unchanged'],
    )
    return marker['run_id'], outcome
