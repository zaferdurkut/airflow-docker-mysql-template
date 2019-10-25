from datetime import timedelta, datetime
import airflow
import time, os, sys, json, ast
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook
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
    dag_id='ssh_airflow_test', 
    default_args=args,
    schedule_interval='*/1 * * * *',
    catchup=False)

bash_command= """python /Users/zaferdurkut/test/dizin1/ssh_test.py"""
ssh_hook = SSHHook(username=os.getenv('SSH_USER'),password=os.getenv('SSH_PASSWORD'),remote_host=os.getenv('SSH_HOST'))

ssh_task = SSHOperator(
            task_id='ssh_airflow_test_task',
            ssh_hook=ssh_hook,
            command=bash_command,
            dag=dag)

ssh_task
 
