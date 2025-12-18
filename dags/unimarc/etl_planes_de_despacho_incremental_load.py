from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


from utils.janis_utils import incremental_unixtime_load_table_s3, load_full_table_to_s3
from utils.postgres_utils import get_max_updated_at_value, is_empty_table
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _evaluate_full_load(ti, schema, table_name):
    if is_empty_table(schema, table_name):
        ti.xcom_push(key="load_method", value="full_load")
        return "load_full_table_to_s3"
    else:
        ti.xcom_push(key="load_method", value="incremental_load")
        return "get_max_updated_at_date"

def _staging_ventanas_de_despacho_table(ti):
    import pandas as pd
    import sqlalchemy
    
    load_method = ti.xcom_pull(key="load_method", task_ids=["evaluate_full_load"])[0]
    print(f"Load method: {load_method}")
    if load_method == "full_load":
        planning_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    else:
        planning_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+planning_file)
    if not s3_hook.check_for_key(planning_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % planning_file)

    planning_object = s3_hook.get_key(planning_file, bucket_name=s3_bucket)
    df = pd.read_csv(planning_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
            "id",
            "store",
            "carrier",
            "quantity",
            "quota",
            "quantity_new",
            "quantity_picking",
            "quantity_picked",
            "quantity_invoiced",
            "quantity_shipped",
            "quantity_delivered",
            "date_start",
            "date_end",
            "edited",
            "is_blocked",
            "block_date",
            "status",
            "date_created",
            "date_modified"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "store": "id_janis_tienda",
        "carrier": "id_transportadora",
        "quantity": "cantidad",
        "quota": "cuota",
        "quantity_new": "cantidad_nuevo",
        "quantity_picking": "cantidad_en_picking",
        "quantity_picked": "cantidad_pickeada",
        "quantity_invoiced": "cantidad_facturada",
        "quantity_shipped": "cantidad_despachada",
        "quantity_delivered": "cantidad_entregada",
        "date_start": "fecha_inicio",
        "date_end": "fecha_fin",
        "edited": "editado",
        "is_blocked": "bloqueado",
        "block_date": "fecha_bloqueo",
        "status": "estado",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_fin"] = pd.to_datetime(df["fecha_fin"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_bloqueo"] = pd.to_datetime(df["fecha_bloqueo"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    df = df.astype({
        "editado": "bool",
        "bloqueado": "bool",
        "fecha_inicio": "string",
        "fecha_fin": "string",
        "fecha_creacion": "string",
        "fecha_bloqueo": "string",
        "fecha_modificacion": "string",
    }, errors="ignore")

    print("Number of records to be staged: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="monitor_despacho_unimarc",
                con=engine,         
                schema="staging",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: staging.monitor_despacho_unimarc")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_ventanas_de_despacho_unimarc_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla monitor_despacho desde Janis Replica hasta Workspace.",
    schedule_interval="*/30 * * * *",
    start_date=pendulum.datetime(2022, 7, 10, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "janis", "ecommdata", "monitor_despacho", "unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de monitor_despacho de Janis a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = BranchPythonOperator(
        task_id = "evaluate_full_load",
        python_callable = _evaluate_full_load,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "monitor_despacho"
        }
    )

    t1 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "monitor_despacho", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "wms_logistic_delivery_planning", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t3 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_logistic_delivery_planning"}
    )

    t4 = PythonOperator(
        task_id = "staging_ventanas_de_despacho_table",
        python_callable = _staging_ventanas_de_despacho_table,
        trigger_rule = "none_failed"
    )

    t5 = PostgresOperator(
        task_id = "ventanas_de_despacho_incremental_load",
        postgres_conn_id="postgresql_conn",
        sql="sql/upsert_ventanas_de_despacho.sql",
    )

    t6 = PostgresOperator(
        task_id = "clear_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE staging.monitor_despacho_unimarc;",
    )

    t0 >> t1 >> t2
    t0 >> t3
    t2 >> t4  
    t3 >> t4 >> t5 >> t6
