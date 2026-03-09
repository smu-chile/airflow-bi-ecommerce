from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_custom_query_to_s3

from datetime import datetime

def _create_initial_order_item_promo_addinfo_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    order_item_prom_add_info_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_item_prom_add_info_file)
    if not s3_hook.check_for_key(order_item_prom_add_info_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_item_prom_add_info_file)

    order_item_prom_add_info_object = s3_hook.get_key(order_item_prom_add_info_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_item_prom_add_info_object.get()["Body"])
    df = df[[
        "id", 
        "order_item_promotion", 
        "field",  
        "value" 
    ]]  

    # # Ensure correct datatypes:
    df["id"] = df["id"].astype("int")
    df["order_item_promotion"] = df["order_item_promotion"].astype("int")
    df["field"] = df["field"].astype("str", errors="ignore")
    df["value"] = df["value"].astype("str", errors="ignore")

    columns_rename = {
        "order_item_promotion": "orden_producto_promocion",
        "field": "campo",
        "value": "valor"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.orden_producto_promocion_extrainfo"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="orden_producto_promocion_extrainfo",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.orden_producto_promocion_extrainfo")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_orden_producto_promocion_extrainfo_initial_load',
    default_args=default_args,
    description="Extracción y carga inicial de tabla orden_producto_promocion_extrainfo desde Janis Replica hasta Workspace.",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "orden_producto_promocion_extrainfo"],
) as dag:

    dag.doc_md = """
    Extracción y carga inicial de tabla de orden_producto_promocion_extrainfo de Janis.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT woipai.*
                FROM janis_jackie.wms_orders AS wo
                JOIN janis_jackie.wms_order_items AS woi
                ON woi.order_id = wo.id
                JOIN janis_jackie.wms_order_item_promotions AS woip
                ON woip.order_item = woi.id
                JOIN janis_jackie.wms_order_item_promotions_additional_info woipai
                ON woipai.order_item_promotion = woip.id
                WHERE wo.id >= 168886
            """,
            "query_name": "wms_order_item_promotions_additional_info",
        }
    )

    t1 = PythonOperator(
        task_id = "create_initial_orden_producto_promociones_table",
        python_callable = _create_initial_order_item_promo_addinfo_table
    )

    t0 >> t1
