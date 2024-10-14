from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_customer_addresses_table(ti):
    import numpy as np
    import pandas as pd
    
    customer_addresses_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ customer_addresses_file)
    if not s3_hook.check_for_key(customer_addresses_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % customer_addresses_file)

    customer_addresses_object = s3_hook.get_key(customer_addresses_file, bucket_name=s3_bucket)

    df = pd.read_csv(customer_addresses_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df = df.astype({
        "id" : "int",
        "customer" : "int",
        "erp_id" : "string",
        "vtex_id" : "string",
        "address_name" : "string",
        "type" : "int",
        "postal_code" : "string",
        "country" : "string",
        "city" : "string",
        "state" : "string",
        "street_type" : "int",
        "street" : "string",
        "number" : "string",
        "lat" : "float",
        "lng" :  "float",
        "neighborhood" : "string",
        "complement" : "string",
        "reference" : "string",
        "receiver" : "string",
        "status" : "int",
        "user_created" : "int",
        "date_created" : "int",
        "user_modified" : "int",
        "date_modified" : "int",
    }, errors="ignore")

    columns = [
        "customer",
        "erp_id",
        "vtex_id",
        "address_name",
        "type",
        "postal_code",
        "country",
        "city",
        "state",
        "street_type",
        "street",
        "number",
        "lat",
        "lng",
        "neighborhood",
        "complement",
        "reference",
        "receiver",
        "status",
        "user_created",
        "date_created",
        "user_modified",
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
    print(f"Number of records to lo.ad: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_alvi.customer_addresses (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
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
    'etl_customer_addresses_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla customer_addresses desde Janis Alvi Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "customer_addresses", "alvi", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de clientes de Janis Alvi a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "customer_addresses", 
            "updated_at_field": "date_modified",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "customer_addresses", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_customer_addresses_table",
        python_callable = _incremental_load_customer_addresses_table
    )

    t0 >> t1 >> t2