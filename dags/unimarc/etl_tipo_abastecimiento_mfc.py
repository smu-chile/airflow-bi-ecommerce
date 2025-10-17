from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from utils.bigquery_utils import load_custom_bq_query_to_s3

from datetime import datetime

import pendulum

def tipo_abastecimiento_to_postgres(ti,ds):
    print("\ntodo piola mi rey")
    import pandas as pd
    import sqlalchemy
    import numpy as np

    file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + file)
    if not s3_hook.check_for_key(file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file)

    object = s3_hook.get_key(file, bucket_name=s3_bucket)
    column_types = {
        "OU_ID": "str",
        "SKU_PRODUCT": "str",
        "FUENTE_APROV": "str",
        "FTE_APROV_COD": "str"
    }

    df = pd.read_csv(object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    column_names = {
        "OU_ID": "id_tienda",
        "SKU_PRODUCT": "material",
        "FUENTE_APROV": "tipo_abastecimiento",
        "FTE_APROV_COD": "codigo_tipo_abastecimiento"
    }

    df = df.rename(columns=column_names)
    df = df[['material','id_tienda','tipo_abastecimiento']]
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df['tipo_abastecimiento'] = df['tipo_abastecimiento'].replace({'2': 'centralizado', '1': 'directo'})

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.tipo_abastecimiento_mfc")
        df.to_sql(name="tipo_abastecimiento_mfc",
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
    'etl_tipo_abasticimiento_mfc',
    default_args=default_args,
    description="Carga tipo abasticimiento de los productos del MFC.",
    schedule_interval="45 9 * * *",
    start_date=pendulum.datetime(2024, 6, 15, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DW", "ecommdata", "Abastecimiento", "MFC", "PATRICIO"],
) as dag:

    dag.doc_md = """
    carga a s3 y postgres el tipo de abastecimiento de productos del MFC.
    """ 
    
    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """SELECT
                    OU_ID
                    , SKU_PRODUCT
                    , FUENTE_APROV
                    , FTE_APROV_COD
                FROM
                    cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_OU_SKU
                JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY
                        USING (sku_key)
                JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_FUENTE_APROVISIONAMIENTO ON
                    (
                        fuente_aprov_key = FTE_APROV_KEY
                    )
                JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_OU_HIERARCHY
                        USING (ou_key)
                WHERE
                    ou_id = '1917'
            """,
            "query_name": "productos_tipo_abastecimiento",
        }
    )

    t1 = PythonOperator(
        task_id = "tipo_abastecimiento_to_postgres",
        python_callable = tipo_abastecimiento_to_postgres
    )

    t0 >> t1