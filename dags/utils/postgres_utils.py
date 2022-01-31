from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_max_updated_at_value(schema, table_name, updated_at_field, postgres_conn_id="postgresql_conn"):
    query = f"SELECT MAX({updated_at_field}) FROM {schema}.{table_name};"
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(results)
    cursor.close()
    pg_connection.close()
    return results[0][0].strftime("%Y-%m-%d %H:%M:%S")
