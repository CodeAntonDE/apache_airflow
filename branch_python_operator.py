from datetime import time
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

# Определяем аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 1,
}

# Создаем DAG
dag = DAG(
    'time_based_branching',
    default_args=default_args,
    description='Пример ветвления в зависимости от времени суток',
    schedule_interval='0 */4 * * *',
)

# Определяем функцию для выбора задачи
def choose_task_by_time():
    now = datetime.now().time()
    if 9 <= now.hour < 18:
        return 'day_task'
    else:
        return 'night_task'

# Настройка BranchPythonOperator
branch_operator = BranchPythonOperator(
    task_id='branch_operator',
    python_callable=choose_task_by_time,
    provide_context=True,
    dag=dag,
)

# Определяем задачи
day_task = DummyOperator(task_id='day_task', dag=dag)
night_task = DummyOperator(task_id='night_task', dag=dag)

# Соединяем задачи
branch_operator >> [day_task, night_task]