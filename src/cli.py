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
    benchmark.add_argument('--repeats', type=int, default=5)
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


def dispatch(args):
    if args.command == 'validate-env':
        problems = check_environment()
        if problems:
            for problem in problems:
                print('PROBLEM:', problem)
            raise SystemExit(1)
        print('environment_ok=True')
        return
    if args.command == 'extract':
        run_extract(args)
        return
    if args.command == 'transform':
        run_transform(args)
        return
    raise NotImplementedError(f'Wire command: {args.command}')


def main():
    args = build_parser().parse_args()
    configure_logging()
    try:
        dispatch(args)
    except PipelineError as error:
        logger.error('%s', error)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
