from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.bigquery_utils import load_custom_bq_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

import pendulum

def _sku_categorias_datawarehouse_unimarc_incremental_load(ti, ts):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    print("Execution datetime: " + ts)
    curr_datetime = ts[:10].replace("-", "/")
    dw_categories_file = ti.xcom_pull(key="return_value", task_ids=["extract_sku_hierarchy_table_from_dw_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+dw_categories_file)
    if not s3_hook.check_for_key(dw_categories_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_categories_file)

    dw_categories_object = s3_hook.get_key(dw_categories_file, bucket_name=s3_bucket)
    columns_types = {
        "SKU_PRODUCT": "string",
        "UMB": "string",
        "GRUPO_DSC": "string", 
        "CAT_DSC": "string", 
        "SEC_DSC": "string", 
        "NEG_DSC": "string", 
        "LIN_DESC": "string",
    }
    df = pd.read_csv(dw_categories_object.get()["Body"], dtype=columns_types)

    # Left pad material column:
    df["SKU_PRODUCT"] = df["SKU_PRODUCT"].astype("string").str.pad(18, side="left", fillchar="0")
    df["UMB"] = df["UMB"].str.replace("ST", "UN")

    columns_rename = {
        "SKU_PRODUCT": "material",
        "UMB": "umv",
        "GRUPO_DSC": "grupo", 
        "CAT_DSC": "categoria", 
        "SEC_DSC": "seccion", 
        "NEG_DSC": "negocio", 
        "LIN_DESC": "linea",
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="sku_categorias_datawarehouse_unimarc",
                con=engine,         
                schema="staging",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_sku_categorias_datawarehouse_incremental_load',
    default_args=default_args,
    description="Extraction and transformation of incremental sku_categories data from datawarehouse.",
    schedule_interval="45 7 * * *",
    start_date=pendulum.datetime(2022, 8, 2, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "DW", "S3", "ecommdata", "sku_categorias_datawarehouse", "Unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    El proceso inicia limpiando la tabla staging.sku_categorias_datawarehouse_unimarc. \n
    Extracción de vista cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY desde DataWarehouse, filtrando aquellos registros con material (SKU_PRODUCT) nulo. \n
    Se filtra también un SKU_PRODUCT duplicado: REF_S210. \n
    Solo se extraen columnas referentes al material, umv, negocio, sección, linea, categoría y grupo. \n
    La extracción de esta vista es almacenada en una tabla temporal en el esquema staging. \n
    La tabla temporal es luego cruzada con la tabla ecommdata.skus para dejar solo aquellos skus que existen dentro del workspace de ecommerce y evitar datos innecesarios. \n
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        truncate staging.sku_categorias_datawarehouse_unimarc
        """
    )

    t1 = PythonOperator(
        task_id = "extract_sku_hierarchy_table_from_dw_to_s3",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT SH.SKU_PRODUCT
                    , SH.UMB
                    , SH.GRUPO_DSC 
                    , SH.CAT_DSC 
                    , SH.SEC_DSC 
                    , SH.NEG_DSC 
                    , SH.LIN_DESC
                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` SH
                WHERE SH.SKU_PRODUCT IS NOT NULL
                AND SH.SKU_PRODUCT <> 'REF_S210'
                AND SH.GRUPO_DSC <> 'Sin asignar';
            """,
            "query_name": "DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=30)
    )

    t2 = PythonOperator(
        task_id = "sku_categorias_datawarehouse_unimarc_incremental_load",
        python_callable = _sku_categorias_datawarehouse_unimarc_incremental_load
    )

    t3 = PostgresOperator(
        task_id = "upsert_sku_categoria_datawarehouse",
        postgres_conn_id="postgresql_conn",
        sql="sql/upsert_sku_categorias_datawarehouse.sql"
    )

    t0 >> t1 >> t2 >> t3
