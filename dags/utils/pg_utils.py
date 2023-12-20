from airflow.providers.postgres.hooks.postgres import PostgresHook


def truncate_table(table_name: str, hook: PostgresHook, table_schema: str = 'public') -> None:
    query = f'TRUNCATE TABLE {table_schema}.{table_name} RESTART IDENTITY;'

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
