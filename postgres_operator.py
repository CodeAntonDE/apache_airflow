from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

# Определяем аргументы по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Создаем DAG
dag = DAG(
    'postgres_operator_example_2',
    default_args=default_args,
    description='Пример использования PostgresOperator для выполнения SQL-запросов',
    schedule_interval='@daily',
)

# Определяем задачи
create_table_task = PostgresOperator(
    task_id='create_table',
    sql='''
        CREATE TABLE IF NOT EXISTS example_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)

insert_data_task = PostgresOperator(
    task_id='insert_data',
    sql='''
        INSERT INTO example_table (name, age) VALUES ('Alice', 25), ('Bob', 30);
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)

select_data_task = PostgresOperator(
    task_id='select_data',
    sql='''
        SELECT * FROM example_table;
    ''',
    postgres_conn_id='postgres_default',
    dag=dag,
)

# Соединяем задачи
create_table_task >> insert_data_task >> select_data_task

