from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
}

with DAG('example_dag', default_args=default_args, schedule_interval='@daily') as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    def print_hello():
        print("Hello, World!")

    hello_task = PythonOperator(task_id='hello_task', python_callable=print_hello)

    start >> hello_task >> end