from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the task function
def say_hello():
    print("Hello, Airflow!")

# Define the DAG
with DAG(
    dag_id='example_hello_world',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Only run manually
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )

    hello_task



