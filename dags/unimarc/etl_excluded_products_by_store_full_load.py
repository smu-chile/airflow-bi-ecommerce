from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime

import pendulum

def _full_load_excluded_products_by_store_table(ti):
    import numpy as np
    import pandas as pd
    
    excluded_products_by_store_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+excluded_products_by_store_file)
    if not s3_hook.check_for_key(excluded_products_by_store_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % excluded_products_by_store_file)

    excluded_products_by_store_object = s3_hook.get_key(excluded_products_by_store_file, bucket_name=s3_bucket)

    df = pd.read_csv(excluded_products_by_store_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")


    # Rename columns to match workspace schema:
    columns_rename = {
        "product": "ref_id",
        "store": "id_tienda",
    }
    df = df.rename(columns=columns_rename)

    df = df.astype({
        "ref_id": "string",
        "id_tienda": "string",
    }, errors="ignore")

    columns = [
        "id_tienda",
    ]

    columns_query = ",".join(columns)
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
    insert_query = """
        INSERT INTO ecommdata.productos_excluidos_por_tienda (ref_id,"""+columns_query+""")
        VALUES ("""+values_query+""")
    """
    print(insert_query)

    truncate_query = "TRUNCATE ecommdata.productos_excluidos_por_tienda"
    update_query = """
        BEGIN TRANSACTION;
        UPDATE ecommdata.productos_excluidos_por_tienda pet
        SET ref_id = s.ref_id
        FROM ecommdata.skus s
        WHERE pet.ref_id::int = s.id;
        UPDATE ecommdata.productos_excluidos_por_tienda pet
        SET id_tienda = t.id
        FROM ecommdata.tiendas t
        WHERE pet.id_tienda::int = t.id_janis;
        COMMIT;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(truncate_query)
    cursor.executemany(insert_query, fixed_records)
    cursor.execute(update_query)
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
    'etl_productos_excluidos_por_tienda_full_load',
    default_args=default_args,
    description="Extracción y carga de tabla productos_excluidos_por_tienda desde Janis Unimarc Replica hasta Workspace.",
    schedule_interval="0 4 * * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "productos_excluidos_por_tienda", "Unimarc", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de productos_excluidos_por_tienda de Janis Unimarc a Workspace. \n
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "excluded_products_by_store"}
    )

    t1 = PythonOperator(
        task_id = "full_load_excluded_products_by_store_table",
        python_callable = _full_load_excluded_products_by_store_table
    )

    t0 >> t1
