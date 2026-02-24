from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='my_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['production', 'etl'],
)

# Task 1: Download CSV files from S3 bucket
download_task = PythonOperator(
    task_id='download_csv_files',
    python_callable=lambda **context: download_csv_files(),
    dag=dag,
)

# Task 2: Clean the data (remove duplicates)
clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=lambda **context: clean_data(),
    dag=dag,
)

# Task 3: Load into PostgreSQL
load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=lambda **context: load_into_postgres(),
    dag=dag,
)

# Task 4: Send email notification
notify_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=lambda **context: send_email_notification(),
    dag=dag,
)

# Set dependencies between tasks
download_task >> clean_task >> load_task >> notify_task