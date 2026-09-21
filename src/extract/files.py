import json
import logging
import shutil

from src.common.audit import utc_now_iso
from src.common.errors import PipelineError
from src.common.hashing import sha256_of_file
from src.common.runs import MANIFEST_NAME, is_complete, run_dir, write_success_marker
from src.config import SETTINGS, display_path, path_for

STAGE = 'extract'
logger = logging.getLogger(__name__)


def list_source_files():
    source_dir = path_for('source_dir')
    paths = [source_dir / name for name in SETTINGS['pipeline']['source_files']]
    missing = [display_path(path) for path in paths if not path.is_file()]
    if missing:
        raise PipelineError(STAGE, 'missing source files: ' + ', '.join(missing))
    return paths


def verify_snapshot(target):
    manifest = json.loads((target / MANIFEST_NAME).read_text(encoding='utf-8'))
    for entry in manifest['files']:
        copied = target / entry['name']
        if not copied.is_file() or sha256_of_file(copied) != entry['sha256']:
            raise PipelineError(STAGE, f'raw snapshot {target.name} fails the integrity check for {entry["name"]}')
    return manifest


def extract_sources(run_id):
    target = run_dir('raw_dir', run_id)
    if is_complete(target):
        verify_snapshot(target)
        logger.info('raw snapshot %s is already complete and verified; nothing to copy', target.name)
        return target
    sources = list_source_files()
    if target.exists():
        logger.warning('removing incomplete raw snapshot %s before rebuilding it', target.name)
        shutil.rmtree(target)
    target.mkdir(parents=True)
    files = []
    for source in sources:
        destination = target / source.name
        shutil.copyfile(source, destination)
        source_hash = sha256_of_file(source)
        if sha256_of_file(destination) != source_hash:
            raise PipelineError(STAGE, f'the raw copy of {source.name} differs from the source file')
        files.append({
            'name': source.name,
            'source_path': display_path(source),
            'size_bytes': destination.stat().st_size,
            'sha256': source_hash,
        })
        logger.info('copied %s (%d bytes)', source.name, destination.stat().st_size)
    manifest = {'run_id': run_id, 'stage': STAGE, '_ingested_at_utc': utc_now_iso(), 'files': files}
    (target / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    write_success_marker(target, {'run_id': run_id, 'stage': STAGE})
    logger.info('raw snapshot %s complete with %d files', target.name, len(files))
    return target
