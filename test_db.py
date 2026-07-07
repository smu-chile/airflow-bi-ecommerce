from airflow.providers.postgres.hooks.postgres import PostgresHook
pg = PostgresHook(postgres_conn_id="postgresql_conn")
conn = pg.get_conn()
cursor = conn.cursor()
cursor.execute("SELECT table_name, table_schema FROM information_schema.tables WHERE table_schema IN ('ecommdata', 'catalogo');")
for row in cursor.fetchall():
    print(row[1] + "." + row[0])
