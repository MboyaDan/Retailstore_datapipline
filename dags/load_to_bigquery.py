from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

BUCKET_NAME = "retail-store-bucket"
DATASET_NAME = "retail_store"
TABLE_NAME = "retail_sales"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
}

with DAG(
    dag_id="load_to_bigquery",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Upload transformed data to GCS
    upload_to_gcs = BashOperator(
        task_id="upload_parquet_to_gcs",
        bash_command=f"gsutil cp /opt/airflow/data/transformed/*.parquet gs://{BUCKET_NAME}/transformed/",
    )

    # Load data from GCS to BigQuery (Automatically creates table)
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["transformed/*.parquet"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        autodetect=True,  # Enables automatic schema detection
        write_disposition="WRITE_APPEND",  # Append data to existing table
    )

    upload_to_gcs >> load_to_bigquery
