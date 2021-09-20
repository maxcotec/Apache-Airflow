import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.configuration import conf

from statsd import StatsClient


STATSD_HOST = conf.get("metrics", "statsd_host")
STATSD_PORT = conf.get("metrics", "statsd_port")
STATSD_PREFIX = conf.get("metrics", "statsd_prefix")


def task(run_time, **context):
    metric_name = f'dag.{context["dag"].dag_id}.{context["task"].task_id}.my_counter'
    client = StatsClient(host=STATSD_HOST, port=STATSD_PORT, prefix=STATSD_PREFIX)
    for i in range(0, run_time):
        time.sleep(1)
        client.incr(metric_name)
        print(i)


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='',
    schedule_interval="*/2 * * * *",
    catchup=False
)

my_task_1 = PythonOperator(
    task_id='my_task',
    python_callable=task,
    op_args=[5],
    dag=dag,
    provide_context=True
)

my_task_2 = PythonOperator(
    task_id='my_task_2',
    python_callable=task,
    op_args=[2],
    dag=dag,
    provide_context=True
)

my_task_1 >> my_task_2



