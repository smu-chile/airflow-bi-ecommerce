from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime

def _load_sales_analysis_to_workspace(ti):
    # Prefer local import at Task level for better DAG run time.
    import pandas as pd

    dw_file_name = ti.xcom_pull(key="return_value", task_ids=["load_sales_analysis_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(dw_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_file_name)
    
    dw_s3_object = s3_hook.get_key(dw_file_name, bucket_name=s3_bucket)
    df = pd.read_csv(dw_s3_object.get()["Body"])

    print("Sales DW:")
    print(len(df.index))

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_stock_seguridad',
    default_args=default_args,
    description="Calculo y carga de stock de seguridad en Janis.",
    schedule_interval="0 12 * * *",
    start_date=datetime(2022, 2, 1),
    catchup=False,
    tags=["OPS", "Janis", "stock_seguridad"],
) as dag:

    dag.doc_md = """
    Cálculo de stock de seguridad diario y carga en Janis. \n
    Descarga de análisis de venta en día-semana comparable de las últimas 4 semanas desde
    Data Warehouse. \n
    Cruce de análisis de venta con tabla <b>ecommdata.categoría</b> para obtener el mínimo
    stock de seguridad por material.
    """ 
    t0 = PythonOperator(
        task_id = "load_sales_analysis_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT STORE.STORE_ID
                    , STORE_H.ORG_IP
                    , SKUATTR.BRAND_DESC
                    , SKUATTR.NM
                    , SKUATTR.SKU_PRODUCT
                    , ROUND(SUM(VENTAC.VENTA_NETA)/4,0) AS VENTA_NETA_PROMEDIO
                    , ROUND(SUM(VENTAC.VENTA_UMV)/4,0) AS VENTA_UMV_PROMEDIO
                FROM DWC_SMU.SMU.VW_FACT_REGISTRO_VENTA_CONTABLE VENTAC
                JOIN DWC_SMU.SMU.VW_DIM_DATE FECHA ON FECHA.DATE_KEY = VENTAC.DATE_KEY
                JOIN DWC_SMU.SMU.VW_DIM_STORE STORE ON STORE.STORE_KEY = VENTAC.STORE_KEY
                JOIN DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY STORE_H ON STORE_H.STORE_KEY = VENTAC.STORE_KEY
                JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SKUATTR ON VENTAC.SKU_KEY = SKUATTR.SKU_KEY
                WHERE (FECHA.DATE_VALUE = TO_DATE('{{execution_date.strftime('%%Y-%%m-%%d')}}', 'YYYY-MM-DD') - '7 days'::"INTERVAL"
                    OR FECHA.DATE_VALUE = TO_DATE('{{execution_date.strftime('%%Y-%%m-%%d')}}', 'YYYY-MM-DD') - '14 days'::"INTERVAL"
                    OR FECHA.DATE_VALUE = TO_DATE('{{execution_date.strftime('%%Y-%%m-%%d')}}', 'YYYY-MM-DD') - '21 days'::"INTERVAL"
                    OR FECHA.DATE_VALUE = TO_DATE('{{execution_date.strftime('%%Y-%%m-%%d')}}', 'YYYY-MM-DD') - '28 days'::"INTERVAL")
                    AND STORE_H.ORG_IP_ID = '01'
                GROUP BY STORE.STORE_ID, STORE_H.ORG_IP, SKUATTR.BRAND_DESC, SKUATTR.NM, SKUATTR.SKU_PRODUCT
                ORDER BY SUM(VENTAC.VENTA_UMV)/4 DESC;
            """,
            "query_name": ""
        }
    )
