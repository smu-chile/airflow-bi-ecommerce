from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

def _get_table_stock_from_S3(ts, ti):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    stock_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    orders_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _save_table_stock(ts, ti):
    import pandas as pd
    import sqlalchemy

    df = _get_table_stock_from_S3(ts, ti)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock_unimarc",
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
    'etl_stock_staging_unimarc',
    default_args=default_args,
    description="Extracción y carga de tabla stock desde Janis Unimarc a S3 y staging.",
    schedule="0 */4 * * *",
    start_date=datetime(2022, 6, 16),
    catchup=False,
    tags=["DATA", "Janis", "stock", "unimarc"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock desde Janis Unimarc a S3 y staging.
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "stock"}
    )

    t1 = PostgresOperator(
        task_id = "truncate_staging_table",
        conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_unimarc
        """,
    )

    t2 = PythonOperator(
        task_id = "save_table_stock",
        python_callable = _save_table_stock,
    )

t0 >> t1 >> t2
