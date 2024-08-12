from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy
    from sqlalchemy import text

    file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ file)
    if not s3_hook.check_for_key(file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file)

    costos_dw_object = s3_hook.get_key(file, bucket_name=s3_bucket)

    column_types = {
        "FECHA": "str",
        "CANAL_VENTA": "str",
        "SKU_KEY": "str",
        "NOMBRE_LOCAL": "str",
        "CENTRO_LOCAL": "str",
        "VENTA": "str",
        "VENTA_UMB": "str",
        "COSTO_NETO": "str",
        "Q_APROX_TRX": "str"
    }

    df = pd.read_csv(costos_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    column_names = {
        "FECHA": "fecha",
        "CANAL_VENTA": "canal_venta",
        "SKU_KEY": "sku_key",
        "NOMBRE_LOCAL": "nombre_local",
        "CENTRO_LOCAL": "id_tienda",
        "VENTA": "venta",
        "VENTA_UMB": "venta_umb",
        "COSTO_NETO": "costo_neto",
        "Q_APROX_TRX": "numero_aprox_trx"
    }

    df = df.rename(columns=column_names)

    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    df['venta'] = df['venta'].astype(int)
    df['numero_aprox_trx'] = df['numero_aprox_trx'].astype(int)

    df['venta_umb'] = df['venta_umb'].astype(float)
    df['costo_neto'] = df['costo_neto'].astype(float)

    df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True)

    df['id_tienda'] = df['id_tienda'].str.zfill(4)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE ecommdata.costos_ventas_sku_tienda;")
        df.to_sql(name="costos_ventas_sku_tienda",
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
    'etl_ventas_costos',
    default_args=default_args,
    description="Extracción de ventas y costos por sku, tienda y canal de ventas",
    schedule_interval="15 7 * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="America/Santiago"),
    catchup=False,#True,
    max_active_runs = 1,
    tags=["DW", "P&L", "unimarc", "PATRICIO"],
) as dag:
    
    dag.doc_md = """
    Descarga data de DW y construye tabla P&L.
    """ 

    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """SELECT
            FECHA as Fecha,
            CANAL_VENTA,
            DPH.SKU_KEY,
            b.STORE as Nombre_Local,
            b.STORE_ID as Centro_Local,
            sum(a.VENTA_NETA ) as Venta,
            sum(( DP.CONT_CONV_UMB / DP.DENOM_UMB::BIGINT ) * (VENTA_UMV)) as Venta_UMB,
            sum(COSTO_UNITARIO*( DP.CONT_CONV_UMB / DP.DENOM_UMB::BIGINT ) * (VENTA_UMV)) as Costo_Neto,
            COUNT(DISTINCT MARKET_BASKET_KEY) AS Q_APROX_TRX
            from DWC_SMU.SMU.VW_FACT_VENTA_E_COMMERCE as a
                join DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY as b USING (STORE_KEY)
                join DWC_SMU.SMU.VW_DIM_PRODUCT_HIERARCHY as DPH USING (PRODUCT_KEY)
                join DWC_SMU.SMU.VW_DIM_PRODUCT as DP USING (PRODUCT_KEY,SKU_KEY)
                --left join DWC_SMU.SMU.VW_FACT_PRESUPUESTO_VENTAS as c USING (STORE_KEY,DATE_KEY)
                left join (SELECT 
            DATE_KEY, STORE_KEY, SKU_KEY, 
            AVG(
            case when VENTA_UMB=0 THEN 0
            ELSE ROUND(COSTO_NETO/ VENTA_UMB,0) END) AS COSTO_UNITARIO
            FROM DWC_SMU.SMU.VW_FACT_REGISTRO_VENTA_CONTABLE
            WHERE DATE_VALUE  between '20240501' AND '{{ds}}'::DATE
            GROUP BY 1,2,3) as CST USING (DATE_KEY, STORE_KEY, SKU_KEY)
            WHERE
            a.FECHA between '20240501' AND '{{ds}}'::DATE
            AND CANAL_VENTA = 'E-COMMERCE'
            GROUP BY
            1,2,3,4,5
            """,
            "query_name": "costos_y_ventas_dw"
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

    t2 = PostgresOperator(
        task_id = "p_and_l",
        postgres_conn_id="postgresql_conn",
        sql="sql/p_and_l.sql",
    )

    t0 >> t1 >> t2