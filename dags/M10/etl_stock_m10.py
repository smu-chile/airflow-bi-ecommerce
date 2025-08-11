from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    stock_M10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_M10_file)
    if not s3_hook.check_for_key(stock_M10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_M10_file)

    tiendas_M10_object = s3_hook.get_key(stock_M10_file, bucket_name=s3_bucket)

    column_types = {
        "ID_TIENDA": "str",
        "FECHA_MEDICION_INVENTARIO": "str", 
        "SKU": "str",
        "DESC_SKU": "str",
        "UMB": "str",
        "INSTOCK": "bool",
        "BLOQUEOS": "bool"
}

    df = pd.read_csv(tiendas_M10_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    columns = [
        "id_tienda",
        "fecha_carga",
        "material",
        "descripcion_producto",
        "umv",
        "stock",
        "in_stock",
        "bloqueos"
    ]

    df.columns = columns
    df["stock"] = pd.to_numeric(df["stock"], errors='coerce').fillna(0).astype(int)
    df["fecha_carga"] = pd.to_datetime(df["fecha_carga"])
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df["umv"] = df["umv"].str.replace('ST', 'UN')

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df.to_sql(name="stock",
                    con=conn,         
                    schema="ecommdata_m10",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_M10',
    default_args=default_args,
    description="Extracción de stock de M10 desde dw",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2024, 5, 28, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["M10", "DW", "S3", "stock", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Extracción de stock de M10 desde dw.
    """ 
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT
                DISTINCT ("VARCHAR"(O.OU_ID))::VARCHAR(4) AS ID_TIENDA,
                "TIMESTAMP"(L.DATE_VALUE)                 AS FECHA_MEDICION_INVENTARIO,
                ("VARCHAR"(H.SKU_PRODUCT))::VARCHAR(18)   AS SKU,
                H.SKU_NM                                  AS DESC_SKU,
                (H.UMB)::VARCHAR(4)                       AS UMB,
                INT8(L.STOCK_UMB_ST)                      AS STOCK_UMB,
                INT4(L.IN_STOCK_FOTO)                     AS INSTOCK,
                CASE WHEN (L.BLOQUEO_TIENDA ='' OR L.BLOQUEO_FORMATO ='') THEN FALSE ELSE TRUE END AS BLOQUEOS
                FROM DWC_SMU.SMU.VW_FACT_OU_LOGT_SMY L 
                JOIN DWC_SMU.SMU.VW_DIM_OU_HIERARCHY O 
                ON L.OU_KEY = O.OU_KEY AND  O.ORG_IP_ID in ('02','09')
                JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY H 
                ON L.SKU_KEY = H.SKU_KEY 
                WHERE
                    (((((((H.SKU_PRODUCT NOTNULL) AND
                    (O.OU_KEY NOTNULL)) AND
                    (L.CONSIG <> 'X'::"NVARCHAR")) AND
                    (L.GDS_PD_TP_ID <> 'VERP'::"NVARCHAR")) AND
                    ("NVARCHAR"(L.APLICA_STOCK) = 'S'::"NVARCHAR")) AND
                    ((L.DATE_VALUE) = '{{ds}}'::DATE-1)))
            """,
            "query_name": "stock_m10"
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