from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np

    workflow_M10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+workflow_M10_file)
    if not s3_hook.check_for_key(workflow_M10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % workflow_M10_file)

    workflow_M10_object = s3_hook.get_key(workflow_M10_file, bucket_name=s3_bucket)

    df = pd.read_csv(workflow_M10_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    df = df[["N_PROMOCION",
        "MATERIAL",
        "NOMBRE_PROMOCION",
        "DESC_MATERIAL",
        "ID_EVENTO",
        "DESCRIPCION_EVENTO_PROMOCIONAL",
        "ID_MECANICA",
        "DESCRIPCION_MECANICA",
        "UN_MEDIDA_VENTA",
        "ORGANIZACION_VENTAS",
        "CANAL_DISTRIBUCION",
        "EAN",
        "LINEA",
        "DESCRIPCION_LINEA",
        "MARCA",
        "TIPO_PROMOCION",
        "DESC_PROMOCION",
        "PRECIO_MODAL",
        "PRECIO_MODAL_TOTAL",
        "PRECIO_PROMOCIONAL",
        "PRECIO_TOTAL_PROMOCIONAL",
        "AHORRO",
        "AHORRO_TOTAL",
        "CANTIDAD_N",
        "CANTIDAD_M",
        "FECHA_INICIO_DE_PROMOCION",
        "FECHA_FIN_DE_PROMOCION"]]
    
    columns = [
        "NOMBRE_PROMOCION",
        "ID_EVENTO",
        "DESCRIPCION_EVENTO_PROMOCIONAL",
        "ID_MECANICA",
        "DESCRIPCION_MECANICA",
        "DESC_MATERIAL",
        "UN_MEDIDA_VENTA",
        "ORGANIZACION_VENTAS",
        "CANAL_DISTRIBUCION",
        "EAN",
        "LINEA",
        "DESCRIPCION_LINEA",
        "MARCA",
        "TIPO_PROMOCION",
        "DESC_PROMOCION",
        "PRECIO_MODAL",
        "PRECIO_MODAL_TOTAL",
        "PRECIO_PROMOCIONAL",
        "PRECIO_TOTAL_PROMOCIONAL",
        "AHORRO",
        "AHORRO_TOTAL",
        "CANTIDAD_N",
        "CANTIDAD_M",
        "FECHA_INICIO_DE_PROMOCION",
        "FECHA_FIN_DE_PROMOCION"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s, %s,"+",".join(["%s" for column in columns])
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
    incremental_query = """
        INSERT INTO ecommdata_m10.workflow (N_PROMOCION, MATERIAL ,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (N_PROMOCION, MATERIAL)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres. ecommdata_m10.workflow")

    return

    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_workflow_M10',
    default_args=default_args,
    description="Extracción de workflow de M10 desde dw",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["M10", "DW", "S3", "workflow", "MATIAS", "TEST"],
) as dag:

    dag.doc_md = """
    Extracción de workflow de M10 desde dw.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT *
                FROM NZ_SMU_BI_DEV.BI.VW_FACT_WORKFLOW_M10
            """,
            "query_name": "workflow_M10"
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
    