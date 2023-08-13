import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.sensors.bash import BashSensor

from custom_sensors import MyCustomFileSensor


@task
def final_task():
    return json.dumps({'return': 'i am done'})


with DAG(
    dag_id="sensors_dag_demo",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    sleep_10sec_sensor = BashSensor(
        task_id='wait_for_10sec',
        poke_interval=2,
        timeout=30,
        soft_fail=False,
        retries=0,
        bash_command="sleep 10",
        dag=dag)

    wait_for_file = MyCustomFileSensor(
        task_id='check_file',
        filepath='/opt/airflow/abc.txt',
        poke_interval=10,
        timeout=60,
        dag=dag
    )

    end_task = final_task()

    chain(sleep_10sec_sensor,
          wait_for_file,
          end_task)