from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Output directory
OUTPUT_DIR = "/opt/airflow/data/extracted"

# Function to extract data from PostgreSQL with batching
def extract_table(table_name, batch_size=10000):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg_hook.get_conn()

    # Extract in batches
    offset = 0
    all_data = []
    while True:
        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        df = pd.read_sql(query, conn)
        if df.empty:
            break
        all_data.append(df)
        offset += batch_size

    # Concatenate and save
    final_df = pd.concat(all_data)
    file_path = f"{OUTPUT_DIR}/{table_name}.csv.gz"
    final_df.to_csv(file_path, index=False, compression="gzip")

    conn.close()
    print(f"Extracted {table_name} to {file_path}")

# Define DAG
default_args = {"owner": "airflow", 
                "start_date": datetime(2025, 2, 11),
                  "retries": 1}

with DAG(dag_id="extract_postgres", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    tables = ["customers", "inventory", "products", "sales", "stores"]

    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_table,
            op_args=[table],
        ) for table in tables
    ]


    extract_tasks
