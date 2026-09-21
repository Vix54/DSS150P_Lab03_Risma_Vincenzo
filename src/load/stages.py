import json
import logging
from datetime import datetime, timezone

import pandas as pd
import psycopg

from src.common.errors import PipelineError
from src.common.runs import MANIFEST_NAME, SUCCESS_MARKER, is_complete, run_dir
from src.load.postgres import connect, record_event, record_run, upsert_curated
from src.transform.stages import CURATED_FILE
from src.validate.quality import validate_curated

STAGE = 'load'
MAX_REPORTED = 5
logger = logging.getLogger(__name__)


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def collect_run_facts(run_id):
    curated_dir = run_dir('curated_dir', run_id)
    curated_marker = read_json(curated_dir / SUCCESS_MARKER)
    staging_marker = read_json(run_dir('staging_dir', run_id) / SUCCESS_MARKER)
    manifest_path = run_dir('raw_dir', run_id) / MANIFEST_NAME
    started = read_json(manifest_path)['_ingested_at_utc'] if manifest_path.is_file() else curated_marker['completed_at_utc']
    staging_quarantined = sum(counts['quarantined_rows'] for counts in staging_marker['counts'].values())
    counts = {
        'rows_staging': curated_marker['counts']['orders_staged'],
        'rows_curated': curated_marker['counts']['curated_rows'],
        'rows_quarantined': staging_quarantined + curated_marker['counts']['quarantined_rows'],
    }
    return datetime.fromisoformat(started), counts


def record_failure(run_id, started_at, counts, message):
    try:
        with connect() as connection:
            record_run(connection, run_id, started_at, datetime.now(timezone.utc), 'failed', counts, message)
            record_event(connection, 'load', run_id, 'failed', message=message)
    except (PipelineError, psycopg.Error) as audit_error:
        logger.warning('could not record the failure of %s in the audit tables: %s', run_id, audit_error)


def run_load(run_id):
    curated_dir = run_dir('curated_dir', run_id)
    if not is_complete(curated_dir):
        raise PipelineError(STAGE, f'curated output {curated_dir.name} is missing or incomplete; run transform first')
    curated = pd.read_parquet(curated_dir / CURATED_FILE)
    findings = validate_curated(curated)
    if findings:
        raise PipelineError(
            STAGE,
            f'curated output {curated_dir.name} fails the pre-load checks and was not loaded; rebuild it with a new run ID: '
            + ' | '.join(findings[:MAX_REPORTED]),
        )
    started_at, counts = collect_run_facts(run_id)
    try:
        with connect() as connection:
            outcome = upsert_curated(connection, curated)
            summary = f"inserted={outcome['inserted']} updated={outcome['updated']} unchanged={outcome['unchanged']}"
            record_run(connection, run_id, started_at, datetime.now(timezone.utc), 'loaded', counts, summary)
            record_event(connection, 'load', run_id, 'loaded', outcome=outcome, message=summary)
    except psycopg.Error as error:
        message = f'{type(error).__name__}: {error}'
        record_failure(run_id, started_at, counts, message)
        raise PipelineError(STAGE, f'database error while loading {run_id}: {message}') from error
    logger.info('load %s: rows_in=%d %s', run_id, outcome['rows_in'], summary)
    return outcome
