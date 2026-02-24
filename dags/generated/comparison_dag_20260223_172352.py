from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_postgres import S3ToPostgresOperator
from airflow.utils.email import send_email

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='comparison_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

download_csv_files = S3ToPostgresOperator(
    task_id='download_csv_files',
    bucket='my-s3-bucket',
    aws_conn_id='aws_default',
    postgres_conn_id='postgres_default',
    sql='SELECT * FROM my_table WHERE date = {{ ds }}',
    dag=dag,
)

clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=lambda: clean_data(),
    dag=dag,
)

load_into_postgres = S3ToPostgresOperator(
    task_id='load_into_postgres',
    bucket='my-s3-bucket',
    aws_conn_id='aws_default',
    postgres_conn_id='postgres_default',
    sql='INSERT INTO my_table (date, data) VALUES {{ ds }}, {{ data }}',
    dag=dag,
)

send_email = PythonOperator(
    task_id='send_email',
    python_callable=lambda: send_email(),
    dag=dag,
)

download_csv_files >> clean_data >> load_into_postgres >> send_email