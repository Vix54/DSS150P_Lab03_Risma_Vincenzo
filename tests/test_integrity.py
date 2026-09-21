import pytest

from src import config
from src.extract.files import extract_sources
from src.validate.integrity import check_hashes, check_layers

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


def test_hashes_match_right_after_extraction(project):
    extract_sources('run_test')
    assert check_hashes('run_test') == []


def test_a_source_file_that_changed_after_extraction_is_reported(project):
    extract_sources('run_test')
    (project / 'data' / 'source' / 'orders.csv').write_bytes(b'order_id,customer_id\r\nO1,C2\r\n')
    findings = check_hashes('run_test')
    assert any('source file orders.csv' in finding for finding in findings)


def test_a_tampered_raw_copy_is_reported(project):
    target = extract_sources('run_test')
    (target / 'customers.csv').write_bytes(b'tampered')
    findings = check_hashes('run_test')
    assert any('raw copy of customers.csv' in finding for finding in findings)


def test_missing_layers_are_reported(project):
    extract_sources('run_test')
    findings = check_layers('run_test')
    assert len(findings) == 2
    assert all('staging' in finding or 'curated' in finding for finding in findings)
