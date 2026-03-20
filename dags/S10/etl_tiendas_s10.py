from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.bigquery_utils import load_custom_bq_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np

    tiendas_s10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+tiendas_s10_file)
    if not s3_hook.check_for_key(tiendas_s10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % tiendas_s10_file)

    tiendas_s10_object = s3_hook.get_key(tiendas_s10_file, bucket_name=s3_bucket)

    column_types = {
        "ID_TIENDA": "str",
        "NOMBRE_TIENDA": "str"
    }

    df = pd.read_csv(tiendas_s10_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    # Normalizar nombres a minúsculas para inserción Postgres
    df.columns = map(str.lower, df.columns)
    df = df[["id_tienda", "nombre_tienda"]]

    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif pd.isna(value) or value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(str(value).strip())
        fixed_records.append(tuple(fixed_record))
        
    print(f"Number of records to load: {str(len(fixed_records))}")
    
    incremental_query = """
        INSERT INTO ecommdata_s10.tiendas (id_tienda, nombre_tienda) 
        VALUES (%s, %s)
        ON CONFLICT (id_tienda)
        DO UPDATE SET 
            nombre_tienda = EXCLUDED.nombre_tienda,
            fecha_actualizacion = CURRENT_TIMESTAMP;
    """
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres. ecommdata_s10.tiendas")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_tiendas_s10',
    default_args=default_args,
    description="Extracción de tiendas robusta para S10 desde BigQuery",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["S10", "BQ", "S3", "tiendas", "ecommerce"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción robusta e idempotente de tiendas para la integración **S10** desde BigQuery.
    
    - Almacena en `ecommdata_s10.tiendas`.
    - No sobrescribe la configuración manual del indicador `last_millers_rappi`.
    """ 
    
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT ID_TIENDA, NOMBRE_TIENDA 
                FROM `cl-cda-prod.DS_CDA_BI_SOURCES.VW_DIM_TIENDA`
                WHERE ID_FORMATO = '09'
            """,
            "query_name": "tiendas_s10",
            "aws_conn_id": "aws_s3_connection"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1 = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = _load_to_postgres
    )

    t0 >> t1
