
from datetime import timedelta, datetime
import airflow
import time, os, sys, json, ast
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv

load_dotenv()


email_list = ast.literal_eval(os.getenv('RECIVER_LIST'))
args = {
    'owner': 'ssh',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 13),
    'email': email_list,
    'email_on_failure': True,
    # 'email_on_retry': True,
    'email_on_success': True,
    'retries': False,
    # 'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    dag_id='python_airflow_test', 
    default_args=args,
    schedule_interval='*/2 * * * *',
    catchup=False)


def _check_date(execution_date, **context):
    min_date = datetime.datetime.now() - datetime.timedelta(weeks=1)
    if execution_date < min_date:
        raise AirflowSkipException(f"No data available on this execution_date ({execution_date}).")

check_date = PythonOperator(
    task_id="check_if_min_date",
    python_callable=_check_date,
    provide_context=True,
    dag=dag,
)

task1 = DummyOperator(task_id="task1", dag=dag)
task2 = DummyOperator(task_id="task2", dag=dag)

check_date >> task1 >> task2