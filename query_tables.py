from airflow.providers.postgres.hooks.postgres import PostgresHook
import sys

try:
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name ILIKE '%bundle%';
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"{r[0]}.{r[1]}")
    
    cursor.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_name ILIKE '%exc%';
    """)
    rows = cursor.fetchall()
    for r in rows:
        print(f"Exc: {r[0]}.{r[1]}")
except Exception as e:
    print(f"Error: {e}")
