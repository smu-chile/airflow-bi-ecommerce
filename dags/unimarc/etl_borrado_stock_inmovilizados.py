from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _send_stock_0_to_janis(ds):
    import pandas as pd
    import sqlalchemy
    import requests
    import json

    print(f"date: {ds}")
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    query = """
        select SUBSTRING(hvd.ref_id,1,18) as material
        , hvd.id_tienda as id_tienda
        from ecommdata.historia_venta_dw hvd
        inner join ecommdata.productos p on hvd.ref_id = p.ref_id
        left join catalogo.categoria_tienda_inmovilizada cti on hvd.id_tienda = cti.id_tienda and p.id_categoria = cti.categoria
        where case 
            when hvd.venta_ayer is true then 0
            when hvd.venta_2 is true then 1
            when hvd.venta_3 is true then 2
            when hvd.venta_4 is true then 3
            when hvd.venta_5 is true then 4
            when hvd.venta_6 is true then 5
            when hvd.venta_7 is true then 6
            when hvd.venta_8 is true then 7
            when hvd.venta_9 is true then 8
            when hvd.venta_10 is true then 9
            else 10
        end = cti.dias_sin_venta 
    """
    df = pd.read_sql(query, engine)

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }

    payload=[]
    for ind in df.index:
        material = str(df['material'][ind]).zfill(18)
        id_tienda = str(int(df['id_tienda'][ind])).zfill(4)
        row = {"IdSku": material, "Quantity": 0, "Store": id_tienda}
        payload.append(row)
    payload = json.dumps(payload)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_borrado_stock_inmovilizados',
    default_args=default_args,
    description="Borrado de Stock Janis en base a historia de ventas en dw y parametros entregados en tabla catalogo.categoria_tienda_inmovilizada.",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 30, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "ecommdata", "catalogo", "inmovilizados", "stock", "OPS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Borrado de Stock Janis en base a historia de ventas en dw y parametros entregados en tabla catalogo.categoria_tienda_inmovilizada.
    """ 
    
    t0 = PostgresOperator(
        task_id = "truncate_table_sales_history",
        conn_id="postgresql_conn",
        sql="""
            truncate ecommdata.historia_venta_dw
        """
    )
    
    t1 = PostgresOperator(
        task_id = "load_table_sales_history",
        conn_id= "postgresql_conn",
        sql= "sql/load_table_sales_history.sql"
    )

    t2 = PythonOperator(
        task_id = "send_stock_0_to_janis",
        python_callable = _send_stock_0_to_janis
    )

    t0 >> t1 >> t2
