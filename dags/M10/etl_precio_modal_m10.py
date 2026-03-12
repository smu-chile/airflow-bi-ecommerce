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

    precio_modal_M10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+precio_modal_M10_file)
    if not s3_hook.check_for_key(precio_modal_M10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % precio_modal_M10_file)

    precio_modal_M10_object = s3_hook.get_key(precio_modal_M10_file, bucket_name=s3_bucket)

    column_types = {
        "FORMATO_ID": "int",
        "CODIGO_MATERIAL": "int",
        "MATERIAL": "str",
        "UMV": "str",
        "ID_CATEGORIA": "int",
        "CATEGORIA": "str",
        "PRECIO_MODAL": "int",
        "ID_SEMANA": "int"
    }

    df = pd.read_csv(precio_modal_M10_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    df = df[["FORMATO_ID", "CODIGO_MATERIAL", "UMV", "ID_SEMANA", "MATERIAL", "ID_CATEGORIA", "CATEGORIA", "PRECIO_MODAL"]]

    columns = [
        "MATERIAL",
        "ID_CATEGORIA",
        "CATEGORIA",
        "PRECIO_MODAL"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s, %s, %s, %s, "+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    from psycopg2.extras import execute_values

    incremental_query = """
        INSERT INTO ecommdata_m10.precio_modal (FORMATO_ID, CODIGO_MATERIAL, UMV, ID_SEMANA, MATERIAL, ID_CATEGORIA, CATEGORIA, PRECIO_MODAL) 
        VALUES %s
        ON CONFLICT (FORMATO_ID, CODIGO_MATERIAL, UMV, ID_SEMANA)
        DO UPDATE SET (MATERIAL, ID_CATEGORIA, CATEGORIA, PRECIO_MODAL) = (EXCLUDED.MATERIAL, EXCLUDED.ID_CATEGORIA, EXCLUDED.CATEGORIA, EXCLUDED.PRECIO_MODAL) ;
    """
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    execute_values(cursor, incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres. ecommdata_m10.precio_modal")

    return

    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_precio_modal_M10',
    default_args=default_args,
    description="Extracción de precio modal de M10 desde dw",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["M10", "DW", "S3", "precio modal", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción de precio modal de M10 desde dw.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT *
                FROM cl-cda-prod.DS_CDA_BI_SOURCES.PRECIO_MODAL
                WHERE FORMATO_ID = '09'
            """,
            "query_name": "precio_modal_M10",
            "aws_conn_id": "aws_s3_connection"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1= PythonOperator(
        task_id = "load_to_postgres",
        python_callable = _load_to_postgres
    )

    

    t0 >> t1
    