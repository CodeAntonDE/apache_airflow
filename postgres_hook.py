from airflow.hooks.postgres_hook import PostgresHook

def execute_query(sql_query, parameters=None, conn_id='postgres_default'):
    """
    Функция для выполнения SQL-запроса с использованием хука PostgresHook.

    :param sql_query: SQL-запрос для выполнения.
    :param parameters: Параметры для SQL-запроса (опционально).
    :param conn_id: ID подключения к базе данных PostgreSQL.
    :return: Результат выполнения запроса.
    """
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    return pg_hook.execute(sql_query, parameters)

# Пример использования
execute_query("SELECT * FROM users WHERE id = %s", parameters=(1,), conn_id='postgres_default')