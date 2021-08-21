## First Airflow DAG: Data ingestion pipeline

1. Prepare the database first `docker-compose up airflow-init`

This is going to created db/airflow.db sqlite database

2. Add raw data for current execution date and hour to be ingested

3. - Launch Airflow `docker-compose up`

Wait for scheduler and webserver to get healthy, then go to `localhost:8081` 

```python
username: admin
password: airflow
```

4. Enable the DAG and watch it ingest data.
