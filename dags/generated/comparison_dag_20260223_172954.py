from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd

dag = DAG(
    dag_id='comparison_dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 21),
)

# Task 1: Download CSV files from S3 bucket
download_task = PythonOperator(
    task_id='download_csv',
    python_callable=lambda **context: download_csv(**context),
    dag=dag,
)

# Task 2: Clean the data (remove duplicates)
clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=lambda **context: clean_data(**context),
    dag=dag,
)

# Task 3: Load into PostgreSQL
load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=lambda **context: load_into_postgres(**context),
    dag=dag,
)

# Task 4: Send email notification
email_task = PythonOperator(
    task_id='send_email',
    python_callable=lambda **context: send_email(**context),
    dag=dag,
)

# Define dependencies between tasks
download_task >> clean_task >> load_task >> email_task