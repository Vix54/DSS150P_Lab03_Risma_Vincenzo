import sys
from importlib import metadata

REQUIRED_PACKAGES = ['pandas', 'pyarrow', 'psycopg', 'python-dotenv', 'PyYAML']


def check_environment() -> list[str]:
    problems = []
    print(f'python_version={sys.version.split()[0]}')
    for name in REQUIRED_PACKAGES:
        try:
            print(f'{name}=={metadata.version(name)}')
        except metadata.PackageNotFoundError:
            problems.append(f'package not installed: {name}')
    if problems:
        return problems

    from src.config import PROJECT_ROOT, SETTINGS, path_for, get_db_settings

    print(f'project_root={PROJECT_ROOT}')
    source_dir = path_for('source_dir')
    for name in SETTINGS['pipeline']['source_files']:
        if not (source_dir / name).is_file():
            problems.append(f'missing source file: {source_dir / name}')
    try:
        db = get_db_settings()
    except RuntimeError as exc:
        problems.append(str(exc))
    else:
        print(f"db_target={db['host']}:{db['port']}/{db['dbname']} user={db['user']}")
    return problems
