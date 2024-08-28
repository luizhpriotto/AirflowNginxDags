from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function that will be called by the PythonOperator
def print_hello():
    print("Hello, World!")

# Define the DAG
dag = DAG(
    'hello_world',
    description='A simple hello world DAG',
    schedule_interval='0 12 * * *',  # Daily at noon
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Define the task using PythonOperator
hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag
)

# If you have more tasks, define them here and set dependencies
# For this simple example, we only have one task
