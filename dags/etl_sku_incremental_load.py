from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_load_table_s3, load_custom_query_to_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _measurement_unit_full_load(ti, ts):

    janis_query = f"""
        SELECT *
        FROM janis_jackie.measurement_units
    """
    print(janis_query)

    file_name = load_custom_query_to_s3(ts, query=janis_query, query_name="measurement_units")

    return file_name

def _incremental_load_skus_table(ti):
    import numpy as np
    import pandas as pd
    
    skus_file = ti.xcom_pull(key="return_value", task_ids=["incremental_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+skus_file)
    if not s3_hook.check_for_key(skus_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % skus_file)

    skus_object = s3_hook.get_key(skus_file, bucket_name=s3_bucket)

    df = pd.read_csv(skus_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    measurement_unit_file = ti.xcom_pull(key="return_value", task_ids=["measurement_unit_full_load"])[0]
    print("Searching file: "+measurement_unit_file)
    if not s3_hook.check_for_key(measurement_unit_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % measurement_unit_file)
    
    measurement_unit_object = s3_hook.get_key(measurement_unit_file, bucket_name=s3_bucket)
    df_mu = pd.read_csv(measurement_unit_object.get()["Body"])

    df_mu = df_mu[["id","ref_unit"]]
    
    df_mu_columns_rename = {
        "id": "measurement_unit",
        "ref_unit": "unidad_de_venta"
    }
    df_mu = df_mu.rename(columns=df_mu_columns_rename)
    
    df = df.merge(df_mu, on="measurement_unit", how="left")

    df_mu_columns_rename = {
        "measurement_unit": "measurement_unit_un",
        "unidad_de_venta": "unidad_de_medida_ppum"
    }

    df_mu = df_mu.rename(columns=df_mu_columns_rename)

    df = df.merge(df_mu, on="measurement_unit_un", how="left")


    # Select only relevant columns:
    df = df[[
        "id",
        "ref_id",
        "vtex_id",
        "stock_erp_id",
        "ean", 
        "product",
        "name", 
        "unit_multiplier_un",
        "unit_multiplier", 
        "pack_units", 
        "date_created",
        "date_modified",
        "unidad_de_venta",
        "unidad_de_medida_ppum"
            ]]

    # Fix date types and timezone:
    print("Fixing date datatype columns...")
    df["date_created"] = pd.to_datetime(df["date_created"], errors="ignore", unit="s")
    df["date_created"] = df["date_created"].dt.tz_localize('UTC').dt.tz_convert('America/Santiago')
    df["date_modified"] = pd.to_datetime(df["date_modified"], errors="ignore", unit="s")
    df["date_modified"] = df["date_modified"].dt.tz_localize('UTC').dt.tz_convert('America/Santiago')

    # Rename columns to match workspace schema:
    columns_rename = {
        "stock_erp_id": "erp_id",
        "ean": "ean_primario",
        "product": "id_producto",
        "name": "nombre_sku",
        "unit_multiplier_un": "PPUM",
        "unit_multiplier": "multiplicador_unidad_medida",
        "pack_units": "unidades_pack",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion",
        "unidad_de_venta": "unidad_de_venta",
        "unidad_de_medida_ppum": "unidad_de_medida_ppum"
    }
    df = df.rename(columns=columns_rename)
    df["erp_id"] = df["erp_id"].astype("string").str.replace(".0", "", regex=False).str.zfill(18)

    columns = [
        "ref_id",
        "vtex_id",
        "erp_id",
        "ean_primario",
        "id_producto",
        "nombre_sku",
        "PPUM",
        "multiplicador_unidad_medida",
        "unidades_pack",
        "fecha_creacion",
        "fecha_modificacion",
        "unidad_de_venta",
        "unidad_de_medida_ppum"
    ]

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
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.skus (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
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
    'etl_skus_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla skus desde Janis Replica hasta Workspace.",
    schedule_interval="0 3 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "skus", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de skus de Janis a Workspace. \n
    UPSERT incremental basado en fecha_modificacion.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "skus", 
            "updated_at_field": "fecha_modificacion"
        }
    )

    t0a = PythonOperator(
        task_id = "measurement_unit_full_load",
        python_callable = _measurement_unit_full_load
    )

    t1 = PythonOperator(
        task_id = "incremental_load_table_to_s3",
        python_callable = incremental_load_table_s3,
        op_kwargs = {
            "table_name": "skus", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified",
            "from_unixtime": True
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_skus_table",
        python_callable = _incremental_load_skus_table
    )

    t0 >> t0a >> t1 >> t2
