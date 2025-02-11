from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "mboya",
    "start_date": datetime(2025, 2, 10),
    "retries": 1,
}

with DAG(
    "test_postgres_connection",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:

    test_connection = PostgresOperator(
        task_id="test_postgres_query",
        postgres_conn_id="postgres_default",
        sql="SELECT 1;",
    )

    test_connection