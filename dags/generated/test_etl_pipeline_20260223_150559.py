from datetime import timedelta
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
    dag_id='test_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    tags=['etl', 'test'],
)

s3_bucket = Variable.get("s3_bucket", default_var="my-data-bucket")
s3_key = Variable.get("s3_key", default_var="raw/sales_data.csv")
postgres_conn_id = "postgres_default"

download_task = PythonOperator(
    task_id='download_from_s3',
    python_callable=lambda **context: S3Hook().download_file(
        bucket_name=s3_bucket,
        key=s3_key,
        filename='/tmp/sales_data.csv'
    ),
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=lambda **context: pd.read_csv('/tmp/sales_data.csv')\
        .drop_duplicates()\
        .to_csv('/tmp/sales_data_cleaned.csv', index=False),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_into_postgres',
    python_callable=lambda **context: PostgresHook().run(
        sql="COPY sales FROM '/tmp/sales_data_cleaned.csv' DELIMITER ',' CSV HEADER;"
    ),
    dag=dag,
)

download_task >> clean_task >> load_task