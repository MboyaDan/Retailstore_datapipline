from datetime import timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash import BashOperator
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

# Function to dynamically list partitioned Parquet files in GCS
def list_parquet_files(**kwargs):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # List all folders under transformed/sales_customers/
    blobs = bucket.list_blobs(prefix="transformed/sales_customers/")
    
    # Extract available sale_date partitions
    sale_dates = sorted(set(blob.name.split("/")[2] for blob in blobs if "sale_date=" in blob.name), reverse=True)

    if not sale_dates:
        raise ValueError("No sale_date partitions found in GCS.")

    latest_sale_date = sale_dates[0]  # Pick the most recent partition

    print(f"Using latest sale_date: {latest_sale_date}")

    prefix = f"transformed/sales_customers/{latest_sale_date}/"

    parquet_files = [
        f"gs://{BUCKET_NAME}/{blob.name}"
        for blob in bucket.list_blobs(prefix=prefix)
        if blob.name.endswith(".parquet") and "/store_id=" in blob.name
    ]

    print(f"Found Parquet files: {parquet_files}")

    if parquet_files:
        kwargs['ti'].xcom_push(key='parquet_files', value=parquet_files)
        return "load_data_to_bq"
    else:
        kwargs['ti'].xcom_push(key='parquet_files', value=[])
        return "upload_parquet_to_gcs"


# Function to pull file paths from XCom
def get_parquet_files_from_xcom(**kwargs):
    ti = kwargs['ti']
    parquet_files = ti.xcom_pull(task_ids='check_gcs_files', key='parquet_files')
    if not parquet_files:
        raise ValueError("No Parquet files found in GCS.")
    return parquet_files

# Define DAG
with DAG(
    dag_id="load_to_bigquery",
    default_args=default_args,
    description="Checks GCS, uploads transformed data if needed, and loads it into BigQuery",
    schedule_interval="@daily",
    catchup=False,
    tags=["retail", "bigquery"],
) as dag:

    # Ensure the dataset exists in BigQuery
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=DATASET_NAME,
        exists_ok=True,
    )

    # Check if transformed Parquet files exist in GCS
    check_gcs_task = PythonOperator(
        task_id="check_gcs_files",
        python_callable=list_parquet_files,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Upload partitioned Parquet files to Google Cloud Storage
    upload_to_gcs = BashOperator(
        task_id="upload_parquet_to_gcs",
        bash_command=f"""
            echo "Uploading partitioned Parquet files to GCS..."
            gsutil -m cp -r /opt/airflow/data/transformed/sales_customers.parquet/sale_date=* gs://{BUCKET_NAME}/transformed/sales_customers/
            echo "Upload complete."
        """,
    )

    # Pull file paths from XCom before passing to GCSToBigQueryOperator
    get_parquet_files_task = PythonOperator(
        task_id="get_parquet_files",
        python_callable=get_parquet_files_from_xcom,
    )

    # Load partitioned data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_data_to_bq",
        bucket=BUCKET_NAME,
        source_objects="{{ ti.xcom_pull(task_ids='get_parquet_files', key='return_value') }}",
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="PARQUET",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "sale_date"},
        )


    # DAG Task Dependencies
    create_bq_dataset >> check_gcs_task >> [upload_to_gcs, get_parquet_files_task]
    get_parquet_files_task >> load_to_bigquery
    upload_to_gcs >> load_to_bigquery
