from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='test_etl_pipeline',
    default_args={'owner': 'data_team'},
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 21),
    tags=['etl', 'test'],
)

download_task = PythonOperator(
    task_id='download_csv_from_s3',
    python_callable=lambda: s3.download_file('my-bucket', 'raw/data.csv', '/tmp/data.csv'),
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=lambda: pd.read_csv('/tmp/data.csv').drop_duplicates(),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=lambda: postgres.insert_rows('my_table', clean_task.output),
    dag=dag,
)

download_task >> clean_task >> load_task