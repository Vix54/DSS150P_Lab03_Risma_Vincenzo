import json
import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get('DSS150P_PROJECT_DIR', '/opt/airflow/project')
FAILURE_LOG = os.environ.get('DSS150P_FAILURE_LOG', '/opt/airflow/logs/dss150p_failures.jsonl')
RUN_ENV = {'PIPELINE_RUN_ID': '{{ run_id }}'}


def record_task_event(event, context):
    task_instance = context['task_instance']
    exception = context.get('exception')
    record = {
        'event': event,
        'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
        'dag_id': task_instance.dag_id,
        'task_id': task_instance.task_id,
        'run_id': context['run_id'],
        'try_number': task_instance.try_number,
        'params': dict(context['params']),
        'error': str(exception) if exception else None,
    }
    print(f'TASK {event.upper()}:', json.dumps(record, default=str))
    try:
        with open(FAILURE_LOG, 'a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, default=str) + '\n')
    except OSError as error:
        print(f'could not write {FAILURE_LOG}: {error}')


def on_task_failure(context):
    record_task_event('failure', context)


def on_task_retry(context):
    record_task_event('retry', context)


DEFAULT_ARGS = {
    'owner': 'dss150p',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'on_failure_callback': on_task_failure,
    'on_retry_callback': on_task_retry,
}

LOAD_COMMAND = (
    'cd ' + PROJECT + ' && '
    'if [ "{{ params.run_mode }}" = "partition" ]; then '
    'python -m src.cli load-partition --year {{ params.year }} --month {{ params.month }}; '
    'else python -m src.cli load; fi'
)
VALIDATE_COMMAND = (
    'cd ' + PROJECT + ' && '
    'if [ "{{ params.run_mode }}" = "partition" ]; then '
    'python -m src.cli validate --year {{ params.year }} --month {{ params.month }}; '
    'else python -m src.cli validate; fi'
)

with DAG(
    dag_id='dss150p_sales_pipeline',
    description='Extract, transform, load and validate the sales order lines; all logic lives in src.cli',
    start_date=datetime(2026, 1, 1),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args=DEFAULT_ARGS,
    params={
        'run_mode': Param('full', enum=['full', 'partition'], description='full loads every curated row; partition loads one month'),
        'year': Param(2026, type='integer', minimum=2000, maximum=2100, description='partition mode only'),
        'month': Param(1, type='integer', minimum=1, maximum=12, description='partition mode only'),
    },
    tags=['DSS150P'],
) as dag:
    extract = BashOperator(
        task_id='extract',
        bash_command='cd ' + PROJECT + ' && python -m src.cli extract',
        env=RUN_ENV,
        append_env=True,
    )
    transform = BashOperator(
        task_id='transform',
        bash_command='cd ' + PROJECT + ' && python -m src.cli transform',
        env=RUN_ENV,
        append_env=True,
    )
    load = BashOperator(
        task_id='load',
        bash_command=LOAD_COMMAND,
        env=RUN_ENV,
        append_env=True,
    )
    validate = BashOperator(
        task_id='validate',
        bash_command=VALIDATE_COMMAND,
        env=RUN_ENV,
        append_env=True,
    )

    extract >> transform >> load >> validate
