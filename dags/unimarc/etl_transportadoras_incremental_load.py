from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable


from utils.janis_utils import incremental_unixtime_load_table_s3, load_full_table_to_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

import pendulum

def _staging_transportadoras_table(ti):
    import pandas as pd
    import sqlalchemy
    
    carriers_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    dock_carrier_relation_file = ti.xcom_pull(key="return_value", task_ids=["extract_dock_carrier_relation_table"])[0]
    logistic_company_relation_file = ti.xcom_pull(key="return_value",task_ids=["extract_logistic_company_relation_table"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+carriers_file)
    if not s3_hook.check_for_key(carriers_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % carriers_file)

    carriers_object = s3_hook.get_key(carriers_file, bucket_name=s3_bucket)
    df_carriers = pd.read_csv(carriers_object.get()["Body"])
    if len(df_carriers.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df_carriers.index)}")

    print("Searching file: "+dock_carrier_relation_file)
    if not s3_hook.check_for_key(dock_carrier_relation_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dock_carrier_relation_file)

    dock_carriers_object = s3_hook.get_key(dock_carrier_relation_file, bucket_name=s3_bucket)
    df_dock_carriers = pd.read_csv(dock_carriers_object.get()["Body"])
    if len(df_dock_carriers.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df_dock_carriers.index)}")

    print("Searching file: "+logistic_company_relation_file)
    if not s3_hook.check_for_key(logistic_company_relation_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % logistic_company_relation_file)
    
    logistic_company_object = s3_hook.get_key(logistic_company_relation_file, bucket_name=s3_bucket)
    df_logistic_company = pd.read_csv(logistic_company_object.get()["Body"])
    if len(df_logistic_company.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df_logistic_company.index)}")


    # Select only relevant columns:
    df_carriers = df_carriers[[
            "id",
            "ref_id",
            "name",
            "type",
            "shipping_type",
            "scheduled",
            "delivery_max_range",
            "quota",
            "logistic_company",
            "status",
            "date_created",
            "user_created",
            "date_modified",
            "user_modified",
            "description",
            "integration_lock"
            ]]
    
    df_logistic_company = df_logistic_company[[
        "id",
        "name"
        ]]
    
    logistic_company_rename = {
        "id": "logistic_company_id",
        "name": "logistic_company_name"
    }

    df_logistic_company = df_logistic_company.rename(columns=logistic_company_rename)

    df = df_carriers.merge(df_dock_carriers, how="left", left_on="id", right_on="carrier").drop(columns=["carrier"])
    print(f"Number of records after left join: {len(df.index)}")
    print(df.info())
    print(df_logistic_company.info())
    df = df.merge(df_logistic_company, how="left", left_on="logistic_company", right_on="logistic_company_id").drop(columns=["logistic_company"])
    print(f"Number of records after left join: {len(df.index)}")
    print(df.info())

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id_janis",
        "ref_id": "id",
        "name": "nombre",
        "type": "tipo",
        "shipping_type": "tipo_despacho",
        "scheduled": "agendado",
        "delivery_max_range": "rango_maximo_despacho",
        "quota": "cuota",
        "status": "estado",
        "date_created": "fecha_creacion",
        "user_created": "creado_por",
        "date_modified": "fecha_modificacion",
        "user_modified": "modificado_por",
        "description": "descripcion",
        "integration_lock": "integration_lock",
        "dock": "dock",
        "logistic_company_id": "id_compañia_logistica",
        "logistic_company_name": "nombre_compañia_logistica"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    df = df.astype({
        "agendado": "bool",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
    }, errors="ignore")

    # Filter null ref_ids
    print("Filtering null ref_ids:")
    df = df[df["id"].notnull()]

    print("Number of records to be staged: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="transportadoras_unimarc",
                con=engine,         
                schema="staging",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: staging.transportadoras_unimarc")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_transportadoras_unimarc_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla transportadoras desde Janis Replica hasta Workspace.",
    schedule_interval="30 4 * * *",
    start_date=pendulum.datetime(2022, 7, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "janis", "ecommdata", "transportadoras", "unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de transportadora de Janis a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "extract_dock_carrier_relation_table",
        python_callable = load_full_table_to_s3,
        op_kwargs = {
            "table_name": "wms_logistic_dock_carriers"
        }
    )

    t1 = PythonOperator(
        task_id = "extract_logistic_company_relation_table",
        python_callable = load_full_table_to_s3,
        op_kwargs = {
            "table_name": "wms_logistic_companies"
        }
    )

    t2 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "transportadoras", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t3 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "wms_logistic_carriers", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t4 = PythonOperator(
        task_id = "staging_transportadoras_table",
        python_callable = _staging_transportadoras_table
    )

    t5 = PostgresOperator(
        task_id = "upsert_transportadoras",
        postgres_conn_id="postgresql_conn",
        sql="sql/upsert_transportadoras.sql",
    )

    t6 = PostgresOperator(
        task_id = "clear_staging_table",
        postgres_conn_id="postgresql_conn",
        sql="TRUNCATE staging.transportadoras_unimarc;",
    )

    t2 >> t3 >> t4
    t0 >> t4
    t1 >> t4
    t4 >> t5 >> t6
