from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta

def _ventas_dw_incremental_load(ti):
    import pandas as pd
    import sqlalchemy

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
        "VENTA_NETA": "int"
    }

    df = pd.read_csv(ventas_dw_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    df = df[
        "DATE_KEY",
        "PRODUCT_KEY",
        "CENTRO",
        "FECHA",
        "PTR_CODPROD",
        "CANAL_VENTA",
        "NUM_TRXN",
        "POS",
        "PEDIDO",
        "VENTA_UMV",
        "VENTA_BRUTA",
        "VENTA_NETA"
    ]

    df["id"] = df["DATE_KEY"] + df["CENTRO"] + df["NUM_TRXN"]

    df = df.drop(columns=["DATE_KEY"])

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="ventas_datawarehouse",
                con=engine,         
                schema="staging",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: staging.ventas_datawarehouse")
    
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
    schedule_interval="0 12 * * *",
    start_date=datetime(2020, 8, 1),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "DW", "S3", "workspace", "ventas_datawarehouse"],
) as dag:

    dag.doc_md = """
    Extract costs data from Datawarehouse to consolidate
    a single costs table on Postgres workspace.
    """ 
    t0 = PythonOperator(
        task_id = "extract_last_7_days_from_dw",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT *
                FROM NZ_BU.ECOMERCE.VW_FACT_VENTA_E_COMMERCE
                WHERE FECHA BETWEEN TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') - INTERVAL '7 days'
                                    AND TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') 
            """,
            "query_name": "NZ_BU.ECOMERCE.VW_FACT_VENTA_E_COMMERCE"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60)
    )

    t1 = PythonOperator(
        task_id = "ventas_dw_staging_load",
        python_callable = _ventas_dw_incremental_load
    )

    t0 >> t1