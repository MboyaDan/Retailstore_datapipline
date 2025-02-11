from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

SPARK_SCRIPT = "/opt/airflow/scripts/transform_data.py"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
}

with DAG(
    dag_id="transform_data_spark",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    transform_task = BashOperator(
        task_id="run_spark_transformation",
        bash_command=f"spark-submit {SPARK_SCRIPT}",
    )

    transform_task
