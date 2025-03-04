from datetime import timedelta
import json
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import os

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Load environment variables
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "retail-store-bucket")
DATASET_NAME = os.getenv("BQ_DATASET_NAME", "retail_store")
TABLE_NAME = os.getenv("BQ_TABLE_NAME", "retail_sales")

# Function to list partitioned Parquet files in GCS
def list_parquet_files(**kwargs):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="transformed/sales_customers/")

    parquet_files = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in blobs if blob.name.endswith(".parquet")
    ]

    print("🔍 Found Parquet Files:", parquet_files)
    kwargs['ti'].xcom_push(key="parquet_files", value=parquet_files)

# Function to fetch Parquet files from XCom **BEFORE** passing to GCSToBigQueryOperator
def get_parquet_files_from_xcom(**kwargs):
    ti = kwargs["ti"]
    parquet_files = ti.xcom_pull(task_ids="check_gcs_files", key="parquet_files")

    if not parquet_files:
        raise ValueError("❌ No Parquet files found in GCS.")

    print("📦 Retrieved Parquet files from XCom:", parquet_files)
    return parquet_files  # ✅ Ensuring it's a valid Python list

# Define DAG
with DAG(
    dag_id="load_to_bigquery",
    default_args=default_args,
    description="Loads Parquet files from GCS into BigQuery",
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "bigquery"],
) as dag:
    
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=DATASET_NAME,
        exists_ok=True,
    )

    check_gcs_task = PythonOperator(
        task_id="check_gcs_files",
        python_callable=list_parquet_files,
        provide_context=True,
    )

    get_parquet_files_task = PythonOperator(
        task_id="get_parquet_files",
        python_callable=get_parquet_files_from_xcom,
        provide_context=True,
    )

    # Fix: Pass the retrieved parquet files directly
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=BUCKET_NAME,
        source_objects="{{ ti.xcom_pull(task_ids='get_parquet_files') }}",  # Fix: Using correct task_id
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "sale_date"},
    )

    # DAG Task Dependencies
    create_bq_dataset >> check_gcs_task >> get_parquet_files_task >> load_to_bigquery
