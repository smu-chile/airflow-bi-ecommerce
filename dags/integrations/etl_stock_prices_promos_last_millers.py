from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

import pendulum

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta
    
def _get_last_millers_stores():
    last_millers_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn_prod")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(last_millers_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def extract_stock_from_dw(ti,ds,ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    ids_tiendas = [id[0] for id in ids_tiendas]
    ids_tiendas_str = str(tuple(ids_tiendas))
    print(ids_tiendas_str)

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"ls_millers_patito/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    query = f"""SELECT S.NBR_ITM 
                , S.SKU_KEY
                , SA.SKU_PRODUCT 
                , OU.OU_ID 
            FROM DWC_SMU.SMU.VW_FACT_STOCK S
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
            WHERE OU.OU_ID in {ids_tiendas_str}
            AND S.DATE_VALUE = '{ds}'
            AND S.APLICA_STOCK = 'S'
            AND A.ALMACEN_COD = '0001'
            AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)
            AND PART.PARTICULARIDAD_COD = 'A'
            AND S.NBR_ITM > 0
            limit 10000
            ;"""
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"stock_sap_query")
        print("Searching file: "+filename)
        return "stock_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_stock"
    
def extract_product_from_dw(ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    query = f"""--SELECT P.SKU_KEY
                    , P.EAN 
                    , P.CONT_CONV_UMB
                    , P.NM
                    , P.BRAND_DESC
                    , P.UNIDAD_DE_MEDIDA
                FROM DWC_SMU.SMU.VW_DIM_PRODUCT P
                WHERE p.indic_ean_ppal = 'X';
            """
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"product_dw")
        print("Searching file: "+filename)
        return "product_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_producto"


def stock_to_postgresql(ti):
    print('\n carga de stock sap a postgresql')
    return

def product_to_postgresql(ti):
    print('\n carga de productos sap a postgresql')
    return




default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_prices_promos_last_millers',
    default_args=default_args,
    description="cargar stock,precios y promos a la tabla lss_millers_promos",
    schedule_interval="20 9,15 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "last_millers", "integraciones", "stock", "prices", "promos"],
) as dag:
    

    dag.doc_md = """
    cargar stock,precios y promos a la tabla lss_millers_promos\n
    guardar en S3 y postgresql.
    """ 

    t_dummy_p = DummyOperator(
            task_id='fallo_dw_producto',
        )
    
    t_dummy_s = DummyOperator(
            task_id='fallo_dw_stock',
        )
    
    t0  = PythonOperator(
        task_id = "get_last_millers_stores",
        python_callable = _get_last_millers_stores
    )

    t1 = BranchPythonOperator(
        task_id = "extract_stock_from_dw",
        python_callable = extract_stock_from_dw,
    )

    t2 = BranchPythonOperator(
        task_id = "extract_product_from_dw",
        python_callable = extract_product_from_dw,
    )

    t3 = PythonOperator(
        task_id = "stock_to_postgresql",
        python_callable = stock_to_postgresql,
    )
    t4 = PythonOperator(
        task_id = "product_to_postgresql",
        python_callable = product_to_postgresql,
    )

    t1 >>  t_dummy_s 
    t2 >>  t_dummy_p
    t0 >> [t1,t2]
    t1 >> t3
    t2 >> t4
