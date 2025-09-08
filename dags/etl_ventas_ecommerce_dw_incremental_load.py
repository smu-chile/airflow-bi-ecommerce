from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta

import pendulum

def _ventas_dw_incremental_load(ti):
    import pandas as pd
    import numpy as np

    ventas_dw_file = ti.xcom_pull(key="return_value", task_ids=["extract_last_7_days_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ventas_dw_file)
    if not s3_hook.check_for_key(ventas_dw_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % ventas_dw_file)

    ventas_dw_object = s3_hook.get_key(ventas_dw_file, bucket_name=s3_bucket)

    column_types = {
        "DATE_KEY": "str",
        "PRODUCT_KEY": "str",
        "CENTRO": "str",
        "FECHA": "str",
        "PTR_CODPROD": "str",
        "CANAL_VENTA": "str",
        "NUM_TRXN": "str",
        "POS": "str",
        "PEDIDO": "str",
        "VENTA_UMV": "str",
        "VENTA_BRUTA": "int",
        "VENTA_NETA": "int",
        "UNIDAD_DE_MEDIDA": "str", 
        "SKU_PRODUCT": "str", 
        "BRAND_DESC": "str", 
        "GRUPO_DSC": "str", 
        "CAT_DSC": "str", 
        "LIN_DESC": "str", 
        "SEC_DSC": "str", 
        "NEG_DSC": "str",
        "TIPO_DOC": "str",
    }

    df = pd.read_csv(ventas_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    df["UNIDAD_DE_MEDIDA"] = np.where(df["UNIDAD_DE_MEDIDA"] == "ST", "UN", df["UNIDAD_DE_MEDIDA"])
    df["id"] = df["DATE_KEY"] + df["CENTRO"] + df["NUM_TRXN"] + df["PRODUCT_KEY"]
    df["ref_id_sku"] = np.where((df["SKU_PRODUCT"].isnull()) | (df["UNIDAD_DE_MEDIDA"].isnull()), 
                                        "NULL", 
                                        df["SKU_PRODUCT"] + "-" + df["UNIDAD_DE_MEDIDA"])
    df["PEDIDO"] = np.where(df["PEDIDO"].isnull(), "NULL", df["PEDIDO"].str[1:])
    df["PEDIDO"] = df["PEDIDO"].astype("int", errors="ignore")
    df = df.drop(columns=["DATE_KEY", "PRODUCT_KEY", "SKU_PRODUCT", "UNIDAD_DE_MEDIDA"])

    column_names = {
        "CENTRO": "id_tienda",
        "FECHA": "fecha_facturacion",
        "PTR_CODPROD": "ean",
        "CANAL_VENTA": "canal_venta",
        "NUM_TRXN": "num_trxn",
        "POS": "pos",
        "PEDIDO": "id_orden",
        "VENTA_UMV": "venta_umv",
        "VENTA_BRUTA": "venta_bruta",
        "VENTA_NETA": "venta_neta", 
        "BRAND_DESC": "marca", 
        "GRUPO_DSC": "grupo", 
        "CAT_DSC": "categoria", 
        "LIN_DESC": "linea", 
        "SEC_DSC": "seccion", 
        "NEG_DSC": "negocio",
        "TIPO_DOC": "tipo_doc",
    }

    df = df.rename(columns=column_names)

    columns = [
        "ref_id_sku",
        "id_tienda",
        "fecha_facturacion",
        "ean",
        "canal_venta",
        "num_trxn",
        "pos",
        "id_orden",
        "venta_umv",
        "venta_bruta",
        "venta_neta", 
        "marca", 
        "grupo", 
        "categoria", 
        "linea", 
        "seccion", 
        "negocio",
        "tipo_doc"
    ]

    df = df[["id"]+columns]
    df = df.replace(r'^\s*$', np.nan, regex=True)

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
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
        INSERT INTO ecommdata.ventas_ecommerce_datawarehouse (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data saved to PostgreSQL. Table: ecommdata.ventas_ecommerce_datawarehouse")
    
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "etl_ventas_ecommerce_datawarehouse_incremental_load",
    default_args=default_args,
    description="Extracción diaria de ventas ecommerce de DataWarehouse.",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2020, 8, 1, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    concurrency=2,
    tags=["DATA", "DW", "S3", "workspace", "ventas_ecommerce_datawarehouse", "unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extract ecommerce's sales data from Datawarehouse with last millers sales.
    """ 
    t0 = PythonOperator(
        task_id = "extract_last_7_days_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT VE.MARKET_BASKET_KEY,
            VE.STORE_KEY,
            VE.DATE_KEY,
            VE.PRODUCT_KEY,
            VE.CENTRO,
            VE.TIPO_DOC,
            VE.FECHA,
            VE.PTR_CODPROD,
            VE.CANAL_VENTA,
            VE.NUM_TRXN,
            VE.POS,
            CASE 
                WHEN VE.TIPO_DOC = 'NE' THEN CAST(FMB.VTX_SEQUENCE AS VARCHAR(14))
                ELSE VE.PEDIDO
            END AS PEDIDO,
            VE.VENTA_UMV,
            VE.VENTA_BRUTA,
            VE.VENTA_NETA,
            VE.MARKET_BASKET_NK,
            DS_INSERTION,
            P.UNIDAD_DE_MEDIDA , S.SKU_PRODUCT, S.BRAND_DESC, SH.GRUPO_DSC, SH.CAT_DSC, SH.LIN_DESC, SH.SEC_DSC, SH.NEG_DSC 
                FROM DWC_SMU.SMU.VW_FACT_VENTA_E_COMMERCE VE
                LEFT JOIN DWC_SMU.SMU.VW_DIM_PRODUCT P ON VE.PRODUCT_KEY = P.PRODUCT_KEY
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR S ON P.SKU_KEY = S.SKU_KEY 
                LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY SH ON SH.SKU_KEY = S.SKU_KEY
                LEFT JOIN DWC_SMU.SMU.VW_FACT_MARKET_BASKET_E_COMMERCE FMB ON VE.MARKET_BASKET_KEY = FMB.MARKET_BASKET_KEY 
                WHERE FECHA BETWEEN TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') - INTERVAL '7 days'
                                    AND TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') 
            """,
            "query_name": "DWC_SMU.SMU.VW_FACT_VENTA_E_COMMERCE"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1 = PythonOperator(
        task_id = "ventas_dw_incremental_load",
        python_callable = _ventas_dw_incremental_load,
        pool = "backfill_pool"
    )

    t0 >> t1
