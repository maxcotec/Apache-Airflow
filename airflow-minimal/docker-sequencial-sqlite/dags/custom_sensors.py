import os

from airflow.sensors.base import BaseSensorOperator


class MyCustomFileSensor(BaseSensorOperator):
    def __init__(self, filepath, *args, **kwargs):
        self.filepath = filepath
        super().__init__(*args, **kwargs)

    def poke(self, context):
        # Check if the file exists
        print(f'checking for file `{self.filepath}`...')
        return os.path.exists(self.filepath)
