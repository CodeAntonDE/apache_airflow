from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Определяем аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
}

# Создаем DAG
with DAG('bash_operator_example', default_args=default_args, schedule_interval='@daily') as dag:

    # Определяем задачу с использованием BashOperator
    task_bash = BashOperator(
        task_id='run_bash_command',
        bash_command='echo "Hello, World!" > /tmp/output.txt'
    )