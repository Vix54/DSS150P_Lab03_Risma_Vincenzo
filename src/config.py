from pathlib import Path
import os
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / '.env')

with (PROJECT_ROOT / 'config' / 'settings.yml').open(encoding='utf-8') as f:
    SETTINGS = yaml.safe_load(f)

DB_ENV_KEYS = {
    'host': 'POSTGRES_HOST',
    'port': 'POSTGRES_PORT',
    'dbname': 'POSTGRES_DB',
    'user': 'POSTGRES_USER',
    'password': 'POSTGRES_PASSWORD',
}


def get_db_settings() -> dict:
    missing = [env for env in DB_ENV_KEYS.values() if not os.getenv(env)]
    if missing:
        raise RuntimeError('Missing required environment variables: ' + ', '.join(missing))
    settings = {key: os.environ[env] for key, env in DB_ENV_KEYS.items()}
    settings['port'] = int(settings['port'])
    return settings


def path_for(key: str) -> Path:
    return PROJECT_ROOT / SETTINGS['pipeline'][key]
