import json
import os
import re
import shutil

from src.common.audit import new_run_id, utc_now_iso
from src.common.errors import PipelineError
from src.config import path_for

UNSAFE_RUN_ID_CHARS = re.compile(r'[^A-Za-z0-9_.-]')
RUN_DIR_PREFIX = 'run_id='
MANIFEST_NAME = 'manifest.json'
SUCCESS_MARKER = '_SUCCESS.json'


def sanitize_run_id(run_id):
    return UNSAFE_RUN_ID_CHARS.sub('_', run_id)


def resolve_run_id(cli_value=None):
    return sanitize_run_id(cli_value or new_run_id())


def run_dir(layer_key, run_id):
    return path_for(layer_key) / f'{RUN_DIR_PREFIX}{run_id}'


def is_complete(directory):
    return (directory / SUCCESS_MARKER).is_file()


def write_success_marker(directory, payload):
    marker = {**payload, 'completed_at_utc': utc_now_iso()}
    temporary = directory / f'{SUCCESS_MARKER}.tmp'
    temporary.write_text(json.dumps(marker, indent=2, sort_keys=True), encoding='utf-8')
    temporary.replace(directory / SUCCESS_MARKER)


def discard_incomplete(directory):
    if directory.exists() and not is_complete(directory):
        shutil.rmtree(directory)
        return True
    return False


def latest_complete_run_id(layer_key):
    base = path_for(layer_key)
    if not base.is_dir():
        return None
    finished = []
    for directory in base.iterdir():
        if directory.name.startswith(RUN_DIR_PREFIX) and is_complete(directory):
            marker = json.loads((directory / SUCCESS_MARKER).read_text(encoding='utf-8'))
            finished.append((marker['completed_at_utc'], directory.name[len(RUN_DIR_PREFIX):]))
    return max(finished)[1] if finished else None


def resolve_run_id_for_stage(cli_value, upstream_layer_key):
    explicit = cli_value or os.getenv('PIPELINE_RUN_ID')
    if explicit:
        return sanitize_run_id(explicit)
    latest = latest_complete_run_id(upstream_layer_key)
    if latest is None:
        raise PipelineError('run-id', f'no complete run found in {upstream_layer_key}; run the previous stage first')
    return latest
