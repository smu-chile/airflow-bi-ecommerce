from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

import pendulum

def load_ranking_productos_tienda_to_postgres(ds):
    import pandas as pd
    import numpy as np
    import io
    import os
    import sqlalchemy
    from io import StringIO

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(curr_working_directory+f"/dags/unimarc/sql/ranking_productos_tienda.sql", "r") as query_file:
        ranking_productos_tienda_query = query_file.read()
    
    ranking_productos_tienda_query = ranking_productos_tienda_query.replace("{ds}", ds)

    print("Base query:")
    print(ranking_productos_tienda_query)

    df_ranking_productos_tienda= pd.read_sql_query(ranking_productos_tienda_query, pg_connection)
    
    print(f"Number of records extracted: {len(df_ranking_productos_tienda.index)}")
    df_ranking_productos_tienda.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        df_ranking_productos_tienda.to_sql(name="ranking_productos_tienda",
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
    'etl_ranking_productos_tienda',
    default_args=default_args,
    description="Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de ranking de SKUs de ultimos 30 dias segmentados por tienda",
    schedule_interval="0 7 1 * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "stock", "Unimarc", "ventas_ecommerce_dw", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de stock de top 100 SKUs segmentados por tienda\n
    """ 

    t0 = PostgresOperator(
        task_id = "truncate_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        truncate ecommdata.ranking_productos_tienda
        """,
    )

    t1 = PythonOperator(
        task_id = "load_ranking_productos_tienda_to_postgres",
        python_callable = load_ranking_productos_tienda_to_postgres,
    )

    t0 >> t1