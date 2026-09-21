import argparse
import logging

from src.common.environment import check_environment
from src.common.errors import PipelineError
from src.common.logging_setup import configure_logging

logger = logging.getLogger('cli')
STAGE_COMMANDS = ['extract', 'transform', 'load', 'validate', 'run-all']


def build_parser():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    for name in STAGE_COMMANDS:
        stage_parser = sub.add_parser(name)
        stage_parser.add_argument('--run-id')
    benchmark = sub.add_parser('benchmark')
    benchmark.add_argument('--repeats', type=int)
    benchmark.add_argument('--run-id')
    partition = sub.add_parser('load-partition')
    partition.add_argument('--year', type=int, required=True)
    partition.add_argument('--month', type=int, required=True)
    return parser


def run_extract(args):
    from src.common.runs import resolve_run_id
    from src.config import display_path
    from src.extract.files import extract_sources

    run_id = resolve_run_id(args.run_id)
    raw_dir = extract_sources(run_id)
    print(f'run_id={run_id}')
    print(f'raw_dir={display_path(raw_dir)}')


def run_transform(args):
    from src.common.runs import resolve_run_id_for_stage
    from src.config import display_path
    from src.transform.stages import run_curated, run_staging

    run_id = resolve_run_id_for_stage(args.run_id, 'raw_dir')
    staging_dir = run_staging(run_id)
    curated_dir = run_curated(run_id)
    print(f'run_id={run_id}')
    print(f'staging_dir={display_path(staging_dir)}')
    print(f'curated_dir={display_path(curated_dir)}')


def run_load_command(args):
    from src.common.runs import resolve_run_id_for_stage
    from src.load.stages import run_load

    run_id = resolve_run_id_for_stage(args.run_id, 'curated_dir')
    outcome = run_load(run_id)
    print(f'run_id={run_id}')
    print(f"rows_in={outcome['rows_in']} inserted={outcome['inserted']} updated={outcome['updated']} unchanged={outcome['unchanged']}")


def run_validate_command(args):
    from src.common.runs import resolve_run_id_for_stage
    from src.validate.stages import run_validate

    run_id = resolve_run_id_for_stage(args.run_id, 'curated_dir')
    summary = run_validate(run_id)
    print(f'run_id={run_id}')
    print(f"validation=passed checks={summary['checks']} rows_checked={summary['rows_checked']}")


def run_benchmark_command(args):
    from src.benchmark.storage import run_benchmark
    from src.common.runs import resolve_run_id_for_stage, run_dir
    from src.config import SETTINGS
    from src.transform.stages import CURATED_FILE

    run_id = resolve_run_id_for_stage(args.run_id, 'curated_dir')
    repeats = args.repeats or SETTINGS['storage_benchmark']['repeats']
    rows, _ = run_benchmark(run_dir('curated_dir', run_id) / CURATED_FILE, run_id, repeats)
    print(f'curated_run_id={run_id}')
    print('storage_type,file_size_bytes,write_seconds,full_read_seconds,filtered_read_seconds,row_count')
    for row in rows:
        print(','.join(str(row[column]) for column in ['storage_type', 'file_size_bytes', 'write_seconds', 'full_read_seconds', 'filtered_read_seconds', 'row_count']))


def run_load_partition_command(args):
    from src.load.partitions import run_load_partition

    run_id, outcome = run_load_partition(args.year, args.month)
    print(f'partition={args.year:04d}-{args.month:02d} source_run_id={run_id}')
    print(f"rows_in={outcome['rows_in']} inserted={outcome['inserted']} updated={outcome['updated']} unchanged={outcome['unchanged']}")


def run_all(args):
    from src.common.runs import resolve_run_id
    from src.extract.files import extract_sources
    from src.load.stages import run_load
    from src.transform.stages import run_curated, run_staging
    from src.validate.stages import run_validate

    run_id = resolve_run_id(args.run_id)
    run_stage('extract', lambda _: extract_sources(run_id), None)
    run_stage('transform', lambda _: (run_staging(run_id), run_curated(run_id)), None)
    outcome = run_stage('load', lambda _: run_load(run_id), None)
    summary = run_stage('validate', lambda _: run_validate(run_id), None)
    print(f'run_id={run_id}')
    print(f"load: inserted={outcome['inserted']} updated={outcome['updated']} unchanged={outcome['unchanged']}")
    print(f"validation=passed checks={summary['checks']} rows_checked={summary['rows_checked']}")


def run_stage(name, function, args):
    try:
        return function(args)
    except PipelineError:
        raise
    except Exception as error:
        raise PipelineError(name, f'unexpected {type(error).__name__}: {error}') from error


STAGE_HANDLERS = {
    'extract': run_extract,
    'transform': run_transform,
    'load': run_load_command,
    'validate': run_validate_command,
    'run-all': run_all,
    'benchmark': run_benchmark_command,
    'load-partition': run_load_partition_command,
}


def dispatch(args):
    if args.command == 'validate-env':
        problems = check_environment()
        if problems:
            for problem in problems:
                print('PROBLEM:', problem)
            raise SystemExit(1)
        print('environment_ok=True')
        return
    handler = STAGE_HANDLERS.get(args.command)
    if handler is None:
        raise NotImplementedError(f'Wire command: {args.command}')
    run_stage(args.command, handler, args)


def main():
    args = build_parser().parse_args()
    configure_logging()
    try:
        dispatch(args)
    except PipelineError as error:
        logger.error('%s', error, exc_info=error.__cause__)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
