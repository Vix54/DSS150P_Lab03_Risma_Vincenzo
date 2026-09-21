import logging

import pandas as pd

from src.common.errors import PipelineError
from src.common.runs import run_dir
from src.transform.stages import CURATED_FILE
from src.validate.database import check_database
from src.validate.integrity import check_counts, check_hashes, check_layers
from src.validate.quality import validate_curated

STAGE = 'validate'
MAX_REPORTED = 10
logger = logging.getLogger(__name__)


def run_validate(run_id):
    layer_findings = check_layers(run_id)
    if layer_findings:
        raise PipelineError(STAGE, '; '.join(layer_findings))
    curated = pd.read_parquet(run_dir('curated_dir', run_id) / CURATED_FILE)
    checks = [
        ('source and raw file hashes', lambda: check_hashes(run_id)),
        ('row counts reconcile across layers', lambda: check_counts(run_id)),
        ('curated rules V-01 to V-10', lambda: validate_curated(curated)),
        ('PostgreSQL V-11, hash match and audit row', lambda: check_database(curated, run_id)),
    ]
    problems = []
    for name, check in checks:
        findings = check()
        if findings:
            logger.error('check failed: %s (%d finding(s))', name, len(findings))
            problems.extend(findings)
        else:
            logger.info('check passed: %s', name)
    if problems:
        raise PipelineError(STAGE, f'{len(problems)} validation finding(s): ' + ' | '.join(problems[:MAX_REPORTED]))
    logger.info('validation of %s passed: %d checks, %d curated rows', run_id, len(checks), len(curated))
    return {'checks': len(checks), 'rows_checked': len(curated)}
