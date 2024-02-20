from airflow import DAG
from airflow import macros
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

PARTITION_PERIODS = {
    "daily": "daily",
}

def _get_daily_partitioned_tables(ti, ds):
    
    exec_date = macros.ds_add(ds, 1)
    print(exec_date)

    query = f"""
        SELECT pp.schema_name
            , pp.table_name
        FROM maintenance.periodic_partition as pp
        WHERE pp.period = '{PARTITION_PERIODS['daily']}' 
        AND (pp.updated_at <> '{exec_date}'
            OR pp.updated_at IS NULL);
    """ 
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()

    print(results)
    ti.xcom_push(key="daily_partitioned_tables", value=results)

    return

def _create_new_daily_partitions(ti, ds):

    exec_date = macros.ds_add(ds, 1)
    print(exec_date)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()

    daily_partitioned_tables = ti.xcom_pull(key="daily_partitioned_tables", task_ids=["get_daily_partitioned_tables"])[0]
    for table_data in daily_partitioned_tables:
        schema_name = table_data[0]
        table_name = table_data[1]
        print(table_name)
        exec_date_split = exec_date.split("-")
        part_year = exec_date_split[0]
        part_month = exec_date_split[1]
        part_day = exec_date_split[2]
        partition_name = f"{schema_name}.{table_name}_y{part_year}m{part_month}d{part_day}"
        print(partition_name)

        create_partition_query = f"""
            BEGIN;

            CREATE TABLE {partition_name}
            PARTITION OF {schema_name}.{table_name}
            FOR VALUES FROM ('{exec_date}') TO ('{macros.ds_add(ds, 2)}');

            UPDATE maintenance.periodic_partition
            SET updated_at = '{exec_date}'
            WHERE schema_name = '{schema_name}'
            AND table_name = '{table_name}';
            
            COMMIT;
        """

        print(create_partition_query)
        cursor.execute(create_partition_query)

        pg_connection.commit()

        print(f"Partition created: {partition_name}")
    
    cursor.close()
    pg_connection.close()

    return

default_args = {
    "owner": "maintenance",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'maintenance_periodic_table_partitions',
    default_args=default_args,
    description="Creación de particiones periodicas.",
    schedule_interval="0 3 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["MAINTENANCE", "partitions", "DB", "PostgreSQL", "MATIAS"],
) as dag:

    dag.doc_md = """
    Creación de particiones periódicas.
    """ 
    
    t0 = PythonOperator(
        task_id = "get_daily_partitioned_tables",
        python_callable = _get_daily_partitioned_tables
    )

    t1 = PythonOperator(
        task_id = "create_new_daily_partitions",
        python_callable = _create_new_daily_partitions
    )

    t0 >> t1
