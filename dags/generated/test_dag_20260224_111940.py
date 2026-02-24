from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='test_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Task 1: Download CSV files from S3 bucket
download_task = PythonOperator(
    task_id='download_csv_files',
    python_callable=lambda **context: download_csv_files(),
    dag=dag,
)

# Task 2: Clean data (remove duplicates)
clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=lambda **context: clean_data(),
    dag=dag,
)

# Task 3: Load into PostgreSQL
load_into_postgres_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=lambda **context: load_into_postgres(),
    dag=dag,
)

# Task 4: Send email notification
send_email_notification_task = BashOperator(
    task_id='send_email_notification',
    bash_command=f'echo "Data loaded successfully" | mail -s "Data Loaded" {Variable.get("email_recipient")}',
    dag=dag,
)

# Set dependencies between tasks
download_task >> clean_data_task >> load_into_postgres_task >> send_email_notification_task