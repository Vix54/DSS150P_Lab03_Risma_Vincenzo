import logging

from src.common.errors import PipelineError
from src.common.runs import discard_incomplete, is_complete, run_dir, write_success_marker
from src.transform.staging import build_staging

STAGE = 'transform'
logger = logging.getLogger(__name__)


def write_parquet(frame, path):
    frame.to_parquet(path, engine='pyarrow', index=False)


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
