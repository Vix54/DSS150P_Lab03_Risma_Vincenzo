import json
import re

from src.common.audit import new_run_id, utc_now_iso
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
