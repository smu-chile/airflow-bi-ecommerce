from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

    dw_stores_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_full_load_to_s3"])[0]
    dw_hierarchy_file_name = ti.xcom_pull(key="return_value", task_ids=["netezza_vm_dim_store_hierarchy_full_load_to_s3"])[0]
    janis_file_name = ti.xcom_pull(key="return_value", task_ids=["janis_stores_full_load_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    df_j = df_j.rename(columns={"id": "id_janis",
                                "title": "nombre_tienda_janis",
                                "ref_id": "id",
                                "sales_channel": "canal_venta_vtex",
                                "lat": "latitud",
                                "lng": "longitud",
                                "street_name": "calle",
                                "street_number": "numero",
                                "date_modified": "fecha_modificacion",
                                "date_created": "fecha_creacion"})
    
    # Cast datatypes
    df_j["id_janis"] = df_j["id_janis"].astype("int", errors="ignore")
    df_j["canal_venta_vtex"] = df_j["canal_venta_vtex"].astype("int", errors="ignore")

    # Join Hierarchy table
    df_dw_hierarchy = df_dw_hierarchy[["STORE_KEY", "GERENTE_TIENDA", "GERENTE_ZONA"]]
    df_dw = pd.merge(df_dw_stores, df_dw_hierarchy, left_on="STORE_KEY", right_on="STORE_KEY", how="left")
    print(df_dw.columns)
    df_dw = df_dw[["STORE_ID", "STORE_NAME", "FLRSP_AREA", "GERENTE_TIENDA", "GERENTE_ZONA", "STE_ID", "CITY_ID", "COUNTY_DESC"]]
    df_dw = df_dw.rename(columns={"STORE_NAME": "nombre_tienda",
                                "FLRSP_AREA": "m2_sala",
                                "GERENTE_TIENDA": "gerente_zonal",
                                "GERENTE_ZONA": "gerente_operaciones",
                                "STE_ID": "region",
                                "CITY_ID": "ciudad",
                                "COUNTY_DESC": "comuna"})
    
    # df_dw["STORE_ID_x"] = df_dw["STORE_ID_x"].str.lstrip("0")
    df_j["id"] = df_j["id"].astype("string").str.pad(4, "left", '0')
    df = pd.merge(df_j, df_dw, left_on="id", right_on="STORE_ID", how="left")
    df = df[["id",
            "nombre_tienda_janis",
            "nombre_tienda",
            "id_janis",
            "canal_venta_vtex",
            "latitud",
            "longitud",
            "calle",
            "numero",
            "ciudad",
            "region",
            "comuna",
            "gerente_zonal",
            "gerente_operaciones",
            "m2_sala",
            "status",
            "fecha_modificacion",
            "fecha_creacion"]]

    # Fix date formats
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").astype("str")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").astype("str")

    # Extra column
    df["glosa"] = df["id"] + " - " + df["nombre_tienda"]

    # Fix columns
    df["numero"] = df["numero"].astype("string").str.replace(".0", "", regex=False)
    df["region"] = df["region"].astype("string").str.replace(".0", "", regex=False)

    columns = ["nombre_tienda_janis",
                "nombre_tienda",
                "id_janis",
                "canal_venta_vtex",
                "latitud",
                "longitud",
                "calle",
                "numero",
                "ciudad",
                "region",
                "comuna",
                "gerente_zonal",
                "gerente_operaciones",
                "m2_sala",
                "status",
                "fecha_modificacion",
                "fecha_creacion",
                "glosa"]
    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value in ["NULL", "NaT"]:
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(fixed_records)
    print(f"Number of records: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.tiendas (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'stores_table_etl',
    default_args=default_args,
    description="Extraction and transformation of store data.",
    schedule="0 7 * * *",
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
