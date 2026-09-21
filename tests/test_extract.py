import json

import pytest

from src import config
from src.common.errors import PipelineError
from src.common.hashing import sha256_of_file
from src.common.runs import MANIFEST_NAME, SUCCESS_MARKER, resolve_run_id, sanitize_run_id
from src.extract.files import extract_sources

SOURCE_BYTES = {
    'customers.csv': b'customer_id,email\r\nC1,a@example.com\r\n',
    'products.json': b'[{"product_id": "P1"}]\n',
    'orders.csv': b'order_id,customer_id\r\nO1,C1\r\n',
}


@pytest.fixture
def project(tmp_path, monkeypatch):
    source_dir = tmp_path / 'data' / 'source'
    source_dir.mkdir(parents=True)
    for name, content in SOURCE_BYTES.items():
        (source_dir / name).write_bytes(content)
    monkeypatch.setattr(config, 'PROJECT_ROOT', tmp_path)
    return tmp_path


def raw_dir(project, run_id):
    return project / 'data' / 'raw' / f'run_id={run_id}'


def test_sanitize_replaces_characters_that_are_unsafe_in_paths():
    assert sanitize_run_id('manual__2026-09-22T02:00:00+00:00') == 'manual__2026-09-22T02_00_00_00_00'


def test_run_id_prefers_the_argument_then_the_environment(monkeypatch):
    monkeypatch.setenv('PIPELINE_RUN_ID', 'from_env')
    assert resolve_run_id(None) == 'from_env'
    assert resolve_run_id('cli:value') == 'cli_value'


def test_extract_copies_source_bytes_and_records_hashes(project):
    target = extract_sources('run_test')
    assert target == raw_dir(project, 'run_test')
    manifest = json.loads((target / MANIFEST_NAME).read_text(encoding='utf-8'))
    assert manifest['_ingested_at_utc'].endswith('+00:00')
    assert [entry['name'] for entry in manifest['files']] == list(SOURCE_BYTES)
    for entry in manifest['files']:
        assert (target / entry['name']).read_bytes() == SOURCE_BYTES[entry['name']]
        assert entry['sha256'] == sha256_of_file(project / 'data' / 'source' / entry['name'])
    assert (target / SUCCESS_MARKER).is_file()


def test_rerun_with_the_same_run_id_leaves_the_snapshot_untouched(project):
    target = extract_sources('run_test')
    before = (target / MANIFEST_NAME).read_text(encoding='utf-8')
    extract_sources('run_test')
    assert (target / MANIFEST_NAME).read_text(encoding='utf-8') == before


def test_missing_source_fails_before_any_snapshot_is_created(project):
    (project / 'data' / 'source' / 'orders.csv').unlink()
    with pytest.raises(PipelineError, match='orders.csv'):
        extract_sources('run_test')
    assert not raw_dir(project, 'run_test').exists()


def test_incomplete_snapshot_is_rebuilt(project):
    partial = raw_dir(project, 'run_test')
    partial.mkdir(parents=True)
    (partial / 'leftover.tmp').write_text('half written')
    target = extract_sources('run_test')
    assert not (target / 'leftover.tmp').exists()
    assert (target / SUCCESS_MARKER).is_file()


def test_tampered_snapshot_is_detected_on_rerun(project):
    target = extract_sources('run_test')
    (target / 'customers.csv').write_bytes(b'tampered')
    with pytest.raises(PipelineError, match='integrity'):
        extract_sources('run_test')
