from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_custom_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _full_load_bodegas_table(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    order_item_proms_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_item_proms_file)
    if not s3_hook.check_for_key(order_item_proms_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_item_proms_file)

    order_item_proms_object = s3_hook.get_key(order_item_proms_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_item_proms_object.get()["Body"])
    df = df[[
            "id",
            "nombre",
            "dock",
            "id_tienda",
            "id_janis",
            "dock_activo"
    ]]  

    # # Ensure correct datatypes:
    df["id"] = df["id"].astype("str")
    df["nombre"] = df["nombre"].astype("str")
    df["dock"] = df["dock"].astype("int", errors="ignore")
    df["id_tienda"] = df["id_tienda"].astype("int", errors="ignore")
    df["id_janis"] = df["id_janis"].astype("int", errors="ignore")
    df["dock_activo"] = df["dock_activo"].astype("bool")

    df["id_tienda"] = df["id_tienda"].apply(lambda x: "{:04}".format(int(x)) if pd.notnull(x) else x) 

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.bodegas"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df = df.drop_duplicates(subset=["id"])
    df.to_sql(name="bodegas",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.bodegas")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_bodegas_full_load',
    default_args=default_args,
    description="Extracción y carga tabla bodegas y su relación con la tabla tiendas desde Janis Replica hasta Workspace.",
    schedule_interval="0 5 * * *",
    start_date=pendulum.datetime(2022, 3, 15, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "bodegas", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga tabla bodegas y su relación con la tabla tiendas desde Janis Replica hasta Workspace.\n
    Dado que se trata de una tabla pequeña y que no aumentará considerablemente de tamaño en el tiempo, se ha decidido
    utilizar el modelo de carga full_load.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT wlw.ref_id as id
                , wlw.name as nombre
                , wlwd.dock
                , ws.ref_id as id_tienda
                , wlw.id as id_janis
                , CASE 
                    when wld.status = 3 then true
                    when wld.status = 1 then true
                    else false
                END as dock_activo
                from wms_logistic_warehouses wlw 
                left join wms_logistic_warehouse_docks wlwd 
                    on wlwd.warehouse = wlw.id
                left join wms_logistic_docks wld 
                        on wld.id = wlwd.dock 
                left join wms_logistic_dock_stores wlds 
                    on wlds.dock = wlwd.dock 
                left join wms_stores ws 
                    on wlds.sales_channel = ws.sales_channel;
            """,
            "query_name": "wms_logistic_warehouses",
        }
    )

    t1 = PythonOperator(
        task_id = "full_load_bodehas_table",
        python_callable = _full_load_bodegas_table
    )

    t0 >> t1
