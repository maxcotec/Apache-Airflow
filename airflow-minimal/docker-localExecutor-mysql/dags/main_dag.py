# We'll start by importing the DAG object
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='',
    schedule_interval="*/5 * * * *",
    catchup=False
)

my_task = BashOperator(
    task_id='my_task',
    bash_command='for i in {1..5}; do echo $i; sleep 1; done',
    retries=0,
    dag=dag,
)
