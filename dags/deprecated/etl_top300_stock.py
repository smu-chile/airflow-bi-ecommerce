from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

def _get_table_stock_janis_from_S3(ts, ti):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    stock_file = f"janis/replica/stock/{curr_datetime}_stock.csv"
    print(stock_file)
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    orders_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")
    return df

def _get_table_stock_vtex_from_S3(ts, ti):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = "staging/"+'stock_vtex'+"/"+curr_datetime
    stock_file = prefix+'stock_vtex'+".csv"
    print(stock_file)
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+stock_file)
    if not s3_hook.check_for_key(stock_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_file)

    orders_object = s3_hook.get_key(stock_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")
    return df

def _save_table_stock_janis(ts, ti):
    import pandas as pd
    import sqlalchemy
    import numpy as np

    df = _get_table_stock_janis_from_S3(ts, ti)
    df = df[['id', 'item_id', 'store_id','warehouse_id', 'stock', 'min_stock', 'infinite_stock', 'date_published', 'date_modified', 'operation_type']]
    df = df.loc[df['stock'] > 0]
    df["date_published"] = pd.to_datetime(df["date_published"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["date_modified"] = pd.to_datetime(df["date_modified"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df_array = np.array_split(df,5)

    for i in df_array:

        i.to_sql(name="stock_unimarc_2",
                    con=engine,         
                    schema="staging",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
    
    return

def _save_table_stock_vtex(ts, ti):
    import pandas as pd
    import sqlalchemy

    df = _get_table_stock_vtex_from_S3(ts, ti)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock_vtex_unimarc_2",
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
    'etl_top300_stock',
    default_args=default_args,
    description="Extracción y carga de tabla stock top 300 desde Vtex y Janis.",
    schedule="0 */4 * * *",
    start_date=datetime(2022, 7, 19),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "vtex", "janis", "staging", "unimarc", "vtex_stock", "janis_stock", "stock", "top300"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla stock top 300 desde Vtex y Janis.
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_janis_staging_table",
        conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_unimarc_2
        """,
    )

    t1 = PostgresOperator(
        task_id = "truncate_vtex_staging_table",
        conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_vtex_unimarc_2
        """,
    )

    t2 = PostgresOperator(
        task_id = "truncate_stock_top300_staging",
        conn_id="postgresql_conn",
        sql="""
        TRUNCATE staging.stock_top300
        """,
    )


    t3 = PythonOperator(
        task_id = "save_table_stock_janis",
        python_callable = _save_table_stock_janis,
    )

    t4 = PythonOperator(
        task_id = "save_table_stock_vtex",
        python_callable = _save_table_stock_vtex,
    )

    t5 = PostgresOperator(
        task_id = "stock_top300_staging",
        conn_id="postgresql_conn",
        sql = "sql/stock_top300_staging.sql"
    )

    t6 = PostgresOperator(
        task_id = "stock_top300_insert",
        conn_id="postgresql_conn",
        sql = "sql/stock_top300.sql"
    )


t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6
