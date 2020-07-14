from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
    'example_kubernetes_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),
    max_active_runs=1,
    concurrency=10
)

# Generate 5 tasks
tasks = ["task{}".format('One') for i in range(1, 2)]
example_dag_complete_node = DummyOperator(task_id="example_dag_complete", dag=dag)

org_dags = []
for task in tasks:

    bash_command = 'echo HELLO'

    org_node = KubernetesPodOperator(
        namespace='default',
        image="python",
        cmds=["python", "-c"],
        arguments=["print('HELLO')"],
        labels={"foo": "bar"},
        image_pull_policy="Always",
        name=task,
        task_id=task,
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )

    org_node.set_downstream(example_dag_complete_node)
