from datetime import datetime, timedelta
from random import randint

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_that_executes_via_k8s_executor',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    max_active_runs=1,
    concurrency=10
)

# Generate 2 tasks
tasks = ["task{}".format(i) for i in range(1, 3)]
example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)

org_dags = []
for task in tasks:

    bash_command = 'echo HELLO'

    org_node = BashOperator(
        task_id="{}".format(task),
        bash_command=bash_command,
        wait_for_downstream=False,
        retries=5,
        dag=dag
    )
    org_node.set_downstream(example_dag_complete_node)
