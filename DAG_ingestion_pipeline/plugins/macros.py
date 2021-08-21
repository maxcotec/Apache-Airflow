# Defining the plugin class
from datetime import datetime

from airflow.plugins_manager import AirflowPlugin


def execution_date_to_millis(execution_date):
    """converts execution date (in DAG timezone) to epoch millis

    Args:
        date (execution date): %Y-%m-%d

    Returns:
        milliseconds
    """
    date = datetime.strptime(execution_date, "%Y-%m-%d")
    epoch = datetime.utcfromtimestamp(0)
    return (date - epoch).total_seconds() * 1000.0


class MyPlugins(AirflowPlugin):
    name = "custom_macros"
    macros = [execution_date_to_millis]
