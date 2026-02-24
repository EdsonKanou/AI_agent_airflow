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
    dag_id='test_dag',
    default_args=default_args,
    schedule_interval='@daily'
)

download_csv_files = S3ToPostgresOperator(
    task_id='download_csv_files',
    s3_bucket='my-s3-bucket',
    postgres_conn_id='my_postgres_conn',
    sql='SELECT * FROM my_table WHERE date = CURRENT_DATE',
    dag=dag
)

clean_data = PythonOperator(
    task_id='clean_data',
    python_callable=lambda: clean_csv_files(download_csv_files.output),
    dag=dag
)

load_into_postgres = S3ToPostgresOperator(
    task_id='load_into_postgres',
    s3_bucket='my-s3-bucket',
    postgres_conn_id='my_postgres_conn',
    sql='INSERT INTO my_table (date, data) VALUES (CURRENT_DATE, {{ dag.task_instance_key_str }})',
    dag=dag
)

send_email = PythonOperator(
    task_id='send_email',
    python_callable=lambda: send_email('my-email@example.com', 'DAG Run Successful'),
    dag=dag
)

download_csv_files >> clean_data >> load_into_postgres >> send_email