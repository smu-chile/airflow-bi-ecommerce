from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_customers_table(ti):
    import numpy as np
    import pandas as pd
    
    customers_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+ customers_file)
    if not s3_hook.check_for_key(customers_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % customers_file)

    customers_object = s3_hook.get_key(customers_file, bucket_name=s3_bucket)

    df = pd.read_csv(customers_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df = df.astype({
        "id" : "int",
        "vtex_id" : "string",
        "user_id" : "string",
        "erp_id" : "string",
        "customer_manager" : "int",
        "doc_type" : "string",
        "doc" : "string",
        "email" : "string",
        "message_address" : "string",
        "alternative_email" : "string",
        "firstname" : "string",
        "lastname" : "string",
        "normalize_fullname" : "string",
        "mothers_lastname" : "string",
        "birthdate" : "int",
        "phone" :  "string",
        "phone_alt" : "string",
        "phone_alt2" : "string",
        "gender" : "int",
        "company_code" : "string",
        "company_name" : "string",
        "employee_id" : "string",
        "membership_number" : "string",
        "points_card" : "string",
        "normalize_status" : "int",
        "status" : "int",
        "flags_client" : "int",
        "notify" : "int",
        "ecom_id_update_pending" : "int",
        "user_created" : "int",
        "date_created" : "int",
        "user_modified" : "int",
        "date_modified" : "int",
        "is_new" : "int"
    }, errors="ignore")

    columns = [
        "vtex_id",
        "user_id",
        "erp_id",
        "customer_manager",
        "doc_type",
        "doc",
        "email",
        "message_address",
        "alternative_email",
        "firstname",
        "lastname",
        "normalize_fullname",
        "mothers_lastname",
        "birthdate",
        "phone",
        "phone_alt",
        "phone_alt2",
        "gender",
        "company_code",
        "company_name",
        "employee_id",
        "membership_number",
        "points_card",
        "normalize_status",
        "status",
        "flags_client",
        "notify",
        "ecom_id_update_pending",
        "user_created",
        "date_created",
        "user_modified",
        "date_modified",
        "is_new"
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
        INSERT INTO ecommdata_alvi.customers (id,"""+columns_query+""") 
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
    'etl_customers_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla customers desde Janis Alvi Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "customers", "alvi", "MATIAS"],
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
            "table_name": "customers", 
            "updated_at_field": "date_modified",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "customers", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_customers_table",
        python_callable = _incremental_load_customers_table
    )

    t0 >> t1 >> t2
