from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.janis_utils import load_full_table_to_s3
from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta

def _create_final_store_table(ti):
    """
    Read S3 files with store tables from Janis and Datawarehouse.
    Join them with Pandas, give format and save result to Postgres. 
    """
    # Prefer local import at Task level for better DAG run time.
    import numpy as np
    import pandas as pd
    import sqlalchemy

    dw_stores_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_full_load_to_s3"])[0]
    dw_hierarchy_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_hierarchy_full_load_to_s3"])[0]
    janis_file_name = ti.xcom_pull(key="return_value", task_ids=["janis_stores_full_load_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(dw_stores_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_stores_file_name)
    if not s3_hook.check_for_key(dw_hierarchy_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_hierarchy_file_name)
    if not s3_hook.check_for_key(janis_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % janis_file_name)
    
    dw_stores_s3_object = s3_hook.get_key(dw_stores_file_name, bucket_name=s3_bucket)
    df_dw_stores = pd.read_csv(dw_stores_s3_object.get()["Body"])

    dw_hierarchy_s3_object = s3_hook.get_key(dw_hierarchy_file_name, bucket_name=s3_bucket)
    df_dw_hierarchy = pd.read_csv(dw_hierarchy_s3_object.get()["Body"])
    
    janis_s3_object = s3_hook.get_key(janis_file_name, bucket_name=s3_bucket)
    df_j = pd.read_csv(janis_s3_object.get()["Body"])

    print("Stores DW:")
    print(len(df_dw_stores.index))

    print("Hierarchy DW:")
    print(len(df_dw_hierarchy.index))

    print("Stores Janis:")
    print(len(df_j.index))

    df_j = df_j[["id", "title", "ref_id", "sales_channel", "lat", "lng", "street_name", "street_number", 
                 "city", "state", "neighborhood", "status", "date_modified", "date_created"]]
    df_j = df_j.rename(columns={"title": "nombre_tienda_janis",
                                "ref_id": "id_sap",
                                "sales_channel": "canal_venta_vtex",
                                "lat": "latitud",
                                "lng": "longitud",
                                "street_name": "calle",
                                "street_number": "numero",
                                "city": "ciudad",
                                "state": "region",
                                "neighborhood": "comuna",
                                "date_modified": "fecha_modificacion",
                                "date_created": "fecha_creacion"})
    # Join Hierarchy table
    df_dw = pd.merge(df_dw_stores, df_dw_hierarchy, left_on="STORE_KEY", right_on="STORE_KEY", how="left")
    print(df_dw.columns)
    df_dw = df_dw[["STORE_ID_x", "STORE_NAME", "FLRSP_AREA_x", "GERENTE_ZONA"]]
    df_dw = df_dw.rename(columns={"STORE_NAME": "nombre_tienda_DW",
                                "FLRSP_AREA_x": "m2_sala_DW",
                                "GERENTE_ZONA": "gerente_zona_DW"})
    
    df = pd.merge(df_j, df_dw, left_on="id_sap", right_on="STORE_ID_x", how="left")
    df = df[["id",
            "nombre_tienda_janis",
            "nombre_tienda_DW",
            "id_sap",
            "canal_venta_vtex",
            "latitud",
            "longitud",
            "calle",
            "numero",
            "ciudad",
            "region",
            "comuna",
            "gerente_zona_DW",
            "m2_sala_DW",
            "status",
            "fecha_modificacion",
            "fecha_creacion"]]

    # Fix date formats
    df["date_modified"] = pd.to_datetime(df["fecha_modificacion"], unit="s")
    df["date_created"] = pd.to_datetime(df["fecha_creacion"], unit="s")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="tiendas",
                con=engine,         
                schema="ecommdata",         
                if_exists='replace',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return

default_args = {
    "owner": "dw_test",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'stores_table_etl',
    default_args=default_args,
    description="Extraction and transformation of store data.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "DW", "S3", "Janis", "Workspace", "Tiendas"],
) as dag:

    dag.doc_md = """
    Extract store data from Janis replica and Datawarehouse to consolidate
    a single store table on Postgres workspace.
    """ 
    t0 = PythonOperator(
        task_id = "netezza_vm_dim_store_full_load_to_s3",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t1 = PythonOperator(
        task_id = "netezza_vm_dim_store_hierarchy_full_load_to_s3",
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "DWC_SMU.SMU.VW_DIM_STORE_HIERARCHY"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t2 = PythonOperator(
        task_id = "janis_stores_full_load_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_stores"}
    )

    t3 = PythonOperator(
        task_id = "save_transformed_store_table",
        python_callable = _create_final_store_table
    )

    [t0, t1, t2] >> t3
