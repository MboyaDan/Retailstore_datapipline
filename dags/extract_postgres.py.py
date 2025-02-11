from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Define output directory
OUTPUT_DIR = "/opt/airflow/data/extracted"

# Function to extract data from PostgreSQL
def extract_table(table_name):
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure directory exists

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()
    
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    
    file_path = f"{OUTPUT_DIR}/{table_name}.csv"
    df.to_csv(file_path, index=False)
    
    conn.close()
    print(f"Extracted {table_name} to {file_path}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 10),
    "retries": 1,
}

with DAG(
    dag_id="extract_postgres",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    tables = ["customers", "inventory", "products", "sales", "stores"]
    
    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table,
            op_args=[table],
        ) for table in tables
    ]

    extract_tasks
