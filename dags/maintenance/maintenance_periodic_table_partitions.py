from airflow import DAG
from airflow import macros
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime

PARTITION_PERIODS = {
    "daily": "daily",
}

def _create_partition(table_name):

    return

def _get_daily_partitioned_tables(ti, ds):
    
    exec_date = macros.ds_add(ds, 1)
    print(exec_date)

    query = f"""
        SELECT *
        FROM maintenance.periodic_partition as pp
        WHERE pp.period = '{PARTITION_PERIODS['daily']}' 
        AND pp.updated_at <> '{exec_date}';
    """ 
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    print(results)
    ti.xcom_push(key="daily_partitioned_tables", value=results)

    return

def _create_new_daily_partitions(ti, ds):

    exec_date = macros.ds_add(ds, 1)
    print(exec_date)

    daily_partitioned_tables = ti.xcom_pull(key="daily_partitioned_tables", task_ids=["get_daily_partitioned_tables"])
    for table in daily_partitioned_tables:
        print(table)
        exec_date_split = exec_date.split("-")
        part_year = exec_date_split[0]
        part_month = exec_date_split[1]
        part_day = exec_date_split[2]
        partition_name = f"{table}_y{part_year}_m{part_month}_d{part_day}"
        print(partition_name)

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
    tags=["MAINTENANCE", "partitions", "DB", "PostgreSQL"],
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
