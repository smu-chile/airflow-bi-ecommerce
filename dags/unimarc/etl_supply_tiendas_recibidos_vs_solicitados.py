from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    supply_stock = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(supply_stock.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    df = df[['PLU_SAP60','TIENDA','DATE_VALUE','CANPEDUMB','CANRECUMB']]
    df.columns = ['material','id_tienda','ultimo_recibido','cant_pedida','cant_recibida']
    df = df.dropna(subset=['material'])
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.supply_stock_recibido")
        df.to_sql(name="supply_stock_recibido",
                    con=conn,         
                    schema="ecommdata",         
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
    'etl_supply_recibidos_vs_solicitados',
    default_args=default_args,
    description="Extracción de costos por sku de dw",
    schedule_interval="0 12 * * *",
    start_date=pendulum.datetime(2024, 11, 26, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "DW", "S3", "supply", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Extracción supply por sku de dw.
    """ 
#    t0 = PythonOperator(
#        task_id = "extract_data_from_dw",
#        python_callable = load_custom_query_to_s3,
#        op_kwargs = {
#            "query": """
#                SELECT
#                J.SPL_RQS_DOC                                           AS NroDocumento,
#                SAFE_CAST(E.SKU_PRODUCT AS INT64)                       AS PLU_SAP60,   -- numérico en BQ (sin (18,0))
#                J.fecha_pedido                                          AS FechaDocumento,
#                Z.DATE_VALUE                                            AS FechaEntrega,
#                SUBSTR(CAST(I.OU_ID AS STRING), 1, 4)                   AS CD,          -- VARCHAR -> STRING
#                SUBSTR(CAST(D.OU_ID AS STRING), 1, 4)                   AS Tienda,
#                J.POSICION                                              AS Posicion,
#                SUM(J.Pedido_umb)                                       AS CanpedUMB,
#                SUM(J.Pedido_ump)                                       AS Canped,
#                SUM(J.Recibido_umb)                                     AS CanrecUMB,
#                SUM(J.Recibido_ump)                                     AS Canrec,
#                SUM(J.RECIBIDO_A_TIEMPO_UMB)                            AS CanRecTiempoUMB,
#                SUM(J.RECIBIDO_A_TIEMPO_UMP)                            AS CanRecTiempo
#                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_COMPRAS` AS J
#                JOIN (
#                SELECT
#                    SPL_RQS_DOC,
#                    SKU_KEY,
#                    MAX(DATE_VALUE) AS DATE_VALUE
#                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_COMPRAS_ESPERADO`
#                WHERE DATE_VALUE BETWEEN DATE_SUB(DATE '{{ds}}', INTERVAL 6 DAY)
#                                    AND DATE_ADD (DATE '{{ds}}', INTERVAL 1 DAY)
#                    AND SKU_KEY != MD5('SKU^CL^SMC^000000000000900827')
#                GROUP BY SPL_RQS_DOC, SKU_KEY
#                ) AS Z
#                ON J.SPL_RQS_DOC = Z.SPL_RQS_DOC
#                AND J.SKU_KEY     = Z.SKU_KEY
#                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION_UNIT` AS D
#                ON J.OU_RECEP_KEY = D.OU_KEY
#                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY`     AS E
#                ON J.SKU_KEY      = E.SKU_KEY
#                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION_UNIT` AS I
#                ON J.OU_PROV_KEY  = I.OU_KEY
#                WHERE D.OU_ID = '0442'
#                GROUP BY
#                NroDocumento, PLU_SAP60, FechaDocumento, FechaEntrega, CD, Tienda, Posicion
#                HAVING SUM(J.Pedido_ump) > 0;
#
#            """,
#            "query_name": "supply"
#        },
#        retries = 2,
#        retry_delay = timedelta(minutes=1),
#        execution_timeout = timedelta(minutes=60),
#        pool = "backfill_pool"
#    )
#
#    t1= PythonOperator(
#        task_id = "load_to_postgres",
#        python_callable = _load_to_postgres
#    )

#t0 >> t1