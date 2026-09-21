import json

import pandas as pd
import pyarrow.parquet as pq

from src.common.hashing import sha256_of_file
from src.common.runs import MANIFEST_NAME, SUCCESS_MARKER, is_complete, run_dir
from src.config import SETTINGS, path_for
from src.transform.stages import CURATED_FILE

LAYERS = [('raw_dir', 'raw'), ('staging_dir', 'staging'), ('curated_dir', 'curated')]


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def parquet_rows(path):
    return pq.ParquetFile(path).metadata.num_rows


def check_layers(run_id):
    return [
        f'the {label} layer for {run_id} is missing or has no completion marker'
        for key, label in LAYERS
        if not is_complete(run_dir(key, run_id))
    ]


def check_hashes(run_id):
    findings = []
    raw_dir = run_dir('raw_dir', run_id)
    manifest = read_json(raw_dir / MANIFEST_NAME)
    listed = {entry['name'] for entry in manifest['files']}
    if listed != set(SETTINGS['pipeline']['source_files']):
        findings.append(f'the manifest lists {sorted(listed)} instead of the configured source files')
    for entry in manifest['files']:
        source = path_for('source_dir') / entry['name']
        copied = raw_dir / entry['name']
        if not source.is_file():
            findings.append(f"source file {entry['name']} is missing")
        elif sha256_of_file(source) != entry['sha256']:
            findings.append(f"source file {entry['name']} no longer matches the hash recorded at extraction")
        if not copied.is_file():
            findings.append(f"raw copy of {entry['name']} is missing")
        elif sha256_of_file(copied) != entry['sha256']:
            findings.append(f"raw copy of {entry['name']} does not match the manifest hash")
    return findings


def count_raw_rows(raw_dir):
    with (raw_dir / 'products.json').open(encoding='utf-8') as handle:
        products = len(json.load(handle))
    return {
        'customers': len(pd.read_csv(raw_dir / 'customers.csv', dtype=str, keep_default_na=False)),
        'products': products,
        'orders': len(pd.read_csv(raw_dir / 'orders.csv', dtype=str, keep_default_na=False)),
    }


def check_counts(run_id):
    findings = []
    raw_dir = run_dir('raw_dir', run_id)
    staging_dir = run_dir('staging_dir', run_id)
    curated_dir = run_dir('curated_dir', run_id)
    quarantine_dir = run_dir('quarantine_dir', run_id)
    staging = read_json(staging_dir / SUCCESS_MARKER)['counts']
    curated = read_json(curated_dir / SUCCESS_MARKER)['counts']
    raw_counts = count_raw_rows(raw_dir)
    for source, counts in staging.items():
        if counts['raw_rows'] != raw_counts[source]:
            findings.append(f"{source}: the staging marker says {counts['raw_rows']} raw rows but the raw file has {raw_counts[source]}")
        if counts['raw_rows'] != counts['superseded_rows'] + counts['quarantined_rows'] + counts['staged_rows']:
            findings.append(f'{source}: raw rows do not equal superseded plus quarantined plus staged rows')
        staged_rows = parquet_rows(staging_dir / f'{source}.parquet')
        if staged_rows != counts['staged_rows']:
            findings.append(f"{source}: the staging file has {staged_rows} rows but the marker says {counts['staged_rows']}")
    staging_quarantined = sum(counts['quarantined_rows'] for counts in staging.values())
    quarantine_rows = parquet_rows(quarantine_dir / 'staging.parquet')
    if quarantine_rows != staging_quarantined:
        findings.append(f'the staging quarantine file has {quarantine_rows} rows but the marker says {staging_quarantined}')
    if curated['orders_staged'] != staging['orders']['staged_rows']:
        findings.append('the curated stage started from a different number of orders than staging produced')
    if curated['orders_staged'] != curated['curated_rows'] + curated['quarantined_rows']:
        findings.append('staged orders do not equal curated plus quarantined orders')
    curated_rows = parquet_rows(curated_dir / CURATED_FILE)
    if curated_rows != curated['curated_rows']:
        findings.append(f"the curated file has {curated_rows} rows but the marker says {curated['curated_rows']}")
    curated_quarantine_rows = parquet_rows(quarantine_dir / 'curated.parquet')
    if curated_quarantine_rows != curated['quarantined_rows']:
        findings.append(f"the curated quarantine file has {curated_quarantine_rows} rows but the marker says {curated['quarantined_rows']}")
    return findings
