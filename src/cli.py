import argparse
from src.common.audit import new_run_id
from src.common.environment import check_environment


def main():
    parser = argparse.ArgumentParser(description='DSS150P modular pipeline')
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('validate-env')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    sub.add_parser('validate')
    b = sub.add_parser('benchmark'); b.add_argument('--repeats', type=int, default=5)
    p = sub.add_parser('load-partition'); p.add_argument('--year', type=int, required=True); p.add_argument('--month', type=int, required=True)
    sub.add_parser('run-all')
    args = parser.parse_args()

    if args.command == 'validate-env':
        problems = check_environment()
        if problems:
            for problem in problems:
                print('PROBLEM:', problem)
            raise SystemExit(1)
        print('environment_ok=True')
        return

    raise NotImplementedError(f'Wire command: {args.command}')

if __name__ == '__main__':
    main()
