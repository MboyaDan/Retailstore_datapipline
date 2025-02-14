from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Set default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,  # Retry 3 times before failing
    "retry_delay": timedelta(minutes=5),
}

# Load environment variables
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "retail-store-bucket")
DATASET_NAME = os.getenv("BQ_DATASET_NAME", "retail_store")
TABLE_NAME = os.getenv("BQ_TABLE_NAME", "retail_sales")

# DAG Definition
with DAG(
    dag_id="load_to_bigquery",
    default_args=default_args,
    description="Uploads transformed data to GCS & loads it into BigQuery",
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "bigquery"],
) as dag:

    # Upload Parquet files to Google Cloud Storage
    upload_to_gcs = BashOperator(
        task_id="upload_parquet_to_gcs",
        bash_command=f"gsutil -m cp /opt/airflow/data/transformed/*.parquet gs://{BUCKET_NAME}/transformed/",
    )

    # Load data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["transformed/*.parquet"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        dag=dag,  # Explicitly attach DAG
    )

    # Define task dependencies
    upload_to_gcs.set_downstream(load_to_bigquery)
