from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 12),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "transform_sales_customers",
    default_args=default_args,
    description="Transform sales and customer data using PySpark",
    schedule_interval="@daily",  # Runs daily
    catchup=False
)

# DAG Documentation
dag.doc_md = """
### DAG: Transform Sales & Customers Data
This DAG runs a **PySpark transformation** on sales and customer data.  
- Runs **daily** (`@daily`).  
- Uses `spark-submit` to execute the script located at `/opt/airflow/scripts/transform_data.py`.  
"""

# Task: Run PySpark transformation script
transform_task = BashOperator(
    task_id="run_pyspark_transform",
    bash_command="export GOOGLE_APPLICATION_CREDENTIALS='/opt/airflow/retailstorepipline-718307efa143.json' && spark-submit /opt/airflow/scripts/transform_data.py",
    dag=dag
)

# Set Task Execution Order
transform_task
