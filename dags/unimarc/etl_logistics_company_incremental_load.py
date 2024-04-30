from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime, timedelta

def _create_final_logistic_company_table(ti):
    """
    Read S3 files with logistic_company tables from Janis and save result to Postgres. 
    """
    # Prefer local import at Task level for better DAG run time.
    import numpy as np
    import pandas as pd

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    janis_file_name = ti.xcom_pull(key="return_value", task_ids=["janis_logistic_companies_full_load_to_s3"])[0]

    if not s3_hook.check_for_key(janis_file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % janis_file_name)
    
    janis_s3_object = s3_hook.get_key(janis_file_name, bucket_name=s3_bucket)
    df_j = pd.read_csv(janis_s3_object.get()["Body"])

    print("logistic_companies Janis:")
    print(len(df_j.index))

    df_j = df_j[["id", "ref_id", "name", "status", "date_created", "date_modified"]]
    df_j = df_j.rename(columns={"id": "id_janis",
                                "ref_id": "id",
                                "name": "nombre_compañia_logistica",
                                "status": "estado",
                                "date_created": "fecha_creacion",
                                "date_modified": "fecha_modificacion"})
    
    # Cast datatypes
    df_j["id_janis"] = df_j["id_janis"].astype("int", errors="ignore")
    
    df_j["id"] = df_j["id"].astype("string").str.pad(4, "left", '0')
    df = df_j[["id",
            "id_janis",
            "nombre_compañia_logistica",
            "estado",
            "fecha_creacion",
            "fecha_modificacion"]]

    # Fix date formats
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").astype("str")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").astype("str")

    columns = ["id_janis",
                "nombre_compañia_logistica",
                "estado",
                "fecha_creacion",
                "fecha_modificacion"]
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
        INSERT INTO ecommdata.compañia_logistica (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
    """
    print("incremental_query:\n"+incremental_query)
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
    'etl_compania_logistica_unimarc_incremental_load',
    default_args=default_args,
    description="Extraction and transformation of logistic_company data unimarc.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 7, 28, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "S3", "Janis", "Workspace", "compañia_logistica", "Unimarc", "SERGIO"],
) as dag:

    dag.doc_md = """
    Extract logistic_company data from Janis replica and Datawarehouse to consolidate
    a single logistic_company table on Postgres workspace.
    """ 

    t0 = PythonOperator(
        task_id = "janis_logistic_companies_full_load_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_logistic_companies"}
    )

    t1 = PythonOperator(
        task_id = "save_transformed_logistic_company_table",
        python_callable = _create_final_logistic_company_table
    )

    t0 >> t1
