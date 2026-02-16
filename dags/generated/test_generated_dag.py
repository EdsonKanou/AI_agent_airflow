```python
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    schedule_interval='@daily',
    tags=['test', 'hello'],
)

task1 = PythonOperator(
    task_id='print_hello',
    python_callable=lambda: print('Hello'),
    dag=dag,
)

task2 = BashOperator(
    task_id='print_world',
    bash_command='echo "World"',
    dag=dag,
)

task1 >> task2
```
This DAG has two tasks: `print_hello` and `print_world`. The `print_hello` task prints the string 'Hello' using a PythonOperator, while the `print_world` task runs a shell command to print the string 'World'. The `print_world` task is dependent on the successful completion of the `print_hello` task, and both tasks run daily at 9 AM.