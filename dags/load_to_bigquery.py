from datetime import timedelta
import json
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage, bigquery

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

# Function to get list of Parquet files in GCS
def list_parquet_files(**kwargs):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="transformed/sales_customers/")

    parquet_files = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in blobs if blob.name.endswith(".parquet")
    ]

    if not parquet_files:
        print("❌ No Parquet files found in GCS.")
        kwargs['ti'].xcom_push(key="parquet_files", value=[])
        return
    
    kwargs['ti'].xcom_push(key="parquet_files", value=parquet_files)
    print(f"✅ Found {len(parquet_files)} Parquet files.")

# Function to get existing files from BigQuery
def get_existing_files_in_bq(**kwargs):
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT _FILE_NAME FROM `{DATASET_NAME}.{TABLE_NAME}`
    """
    query_job = client.query(query)
    existing_files = [row["_FILE_NAME"] for row in query_job]

    kwargs['ti'].xcom_push(key="existing_files", value=existing_files)
    print(f"✅ Found {len(existing_files)} existing files in BigQuery.")

# Function to load only new files into BigQuery
def load_files_to_bq(**kwargs):
    ti = kwargs["ti"]
    all_parquet_files = ti.xcom_pull(task_ids="check_gcs_files", key="parquet_files")
    existing_files = ti.xcom_pull(task_ids="get_existing_files_in_bq", key="existing_files") or []

    if not all_parquet_files:
        print("❌ No new Parquet files found in GCS.")
        return

    # Filter out files that are already in BigQuery
    new_files = [file for file in all_parquet_files if os.path.basename(file) not in existing_files]

    if not new_files:
        print("✅ No new files to upload. Skipping...")
        return

    # Upload only new files
    for file in new_files:
        print(f"📤 Uploading new file: {file}")
        os.system(f"bq load --source_format=PARQUET --autodetect {DATASET_NAME}.{TABLE_NAME} {file}")

# Define DAG
with DAG(
    dag_id="load_to_bigquery",
    default_args=default_args,
    description="Load Parquet files from GCS into BigQuery while avoiding re-uploads",
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

    check_bq_task = PythonOperator(
        task_id="get_existing_files_in_bq",
        python_callable=get_existing_files_in_bq,
        provide_context=True,
    )

    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_files_to_bq,
        provide_context=True,
    )

    # DAG Task Dependencies
    create_bq_dataset >> check_gcs_task >> check_bq_task >> load_to_bigquery_task
