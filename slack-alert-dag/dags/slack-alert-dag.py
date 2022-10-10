import sys
import time

from datetime import datetime, timedelta
from typing import Optional

from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

SLACK_WEBHOOK_URL = "---PASTE-YOUR-WEBHOOK-HERE---"


def alert_slack_channel(context: dict):
    """ Alert to slack channel on failed dag

    :param context: airflow context object
    """
    if not SLACK_WEBHOOK_URL:
        # Do nothing if slack webhook not set up
        return

    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
                  for ti in task_instances
                  if ti.state == 'failed']
    title = f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()

    SlackWebhookHook(
        webhook_token=SLACK_WEBHOOK_URL,
        message=msg,
    ).execute()


default_args = {
    'owner': 'airflow',
}


@dag(
    description='Hello World DAG',
    default_args=default_args,
    schedule_interval='0 12 * * *',
    on_failure_callback=alert_slack_channel,
    start_date=datetime(2017, 3, 20), catchup=False
)
def SlackAlertTestDag():
    @task(on_failure_callback=alert_slack_channel)
    def task1():
        print('Hello world from first task. I am failing :(')
        sys.exit(1)

    @task
    def task2():
        print('Hello world from second task. I am failing :(')
        time.sleep(10)
        sys.exit(1)

    @task
    def task3():
        time.sleep(10)
        print('Hello world from third task')

    @task
    def task4():
        time.sleep(10)
        print('Hello world from forth task. I am failing :(')
        sys.exit(1)

    task1() >> task2()
    task3()
    task4()


dag = SlackAlertTestDag()
