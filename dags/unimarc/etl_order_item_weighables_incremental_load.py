from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_custom_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _check_empty_table(ti):
    import pandas as pd
    import sqlalchemy

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    query_count_weighables = """
        SELECT count(1) as count
        FROM ecommdata.orden_producto_pesables;
    """
    df = pd.read_sql(query_count_weighables, engine)
    count = df["count"][0]

    print(f"Number of records found: {count}")

    if count == 0:
        print("Empty table. Starting full load process...")
        ti.xcom_push(key="load_path", value="load_full_table")
        return "load_full_table"
    else:
        print("Table is not empty. Starting incremental load process...")
        ti.xcom_push(key="load_path", value="get_order_item_weighables_from_janis")
        return "wait_for_orders_s3_file"

def _get_new_orders_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _delete_order_item_weighables_from_postgres(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["seq_id"].tolist()
    if len(order_ids) == 0:
        return
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        DELETE FROM ecommdata.orden_producto_pesables AS opp
        WHERE opp.id_orden IN {query_order_ids} 
    """
    print(query)
    print("Deleting old rows...")
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data deleted.")
    return

def _get_order_item_weighables_from_janis(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "empty"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT woiw.*, woi.ref_id, wo.seq_id
        FROM janis_jackie.wms_orders AS wo
        INNER JOIN janis_jackie.wms_order_items woi
        ON woi.order_id = wo.id
        INNER JOIN janis_jackie.wms_order_item_weighables AS woiw
        ON woi.id = woiw.order_item
        WHERE wo.id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_item_weighables")
    return s3_object_name

def _order_item_weighables_table_incremental_load(ts, ti):
    import numpy as np
    import pandas as pd
    
    xcom_input_task = ti.xcom_pull(key="load_path", task_ids=["check_empty_table"])[0]
    order_item_weighables_file = ti.xcom_pull(key="return_value", task_ids=[xcom_input_task])[0]

    if ti.xcom_pull(key="return_value", task_ids=['get_order_item_weighables_from_janis'])[0] == "empty":
        return

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_item_weighables_file)
    if not s3_hook.check_for_key(order_item_weighables_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_item_weighables_file)

    order_item_weighables_object = s3_hook.get_key(order_item_weighables_file, bucket_name=s3_bucket)

    df = pd.read_csv(order_item_weighables_object.get()["Body"])
    df = df[[
        "id",
        "seq_id",
        "order_item",
        "ean",
        "weight",
        "price",
        "ref_id"
    ]]  

    # # Ensure correct datatypes:
    df["id"] = df["id"].astype("int")
    df["seq_id"] = df["seq_id"].astype("int")
    df["order_item"] = df["order_item"].astype("int")
    df["ean"] = df["ean"].astype("str", errors="ignore")
    df["weight"] = df["weight"].astype("int", errors="ignore")
    df["price"] = df["price"].astype("float", errors="ignore")

    columns_rename = {
        "seq_id": "id_orden",
        "order_item": "id_orden_producto",
        "weight": "peso",
        "price": "precio"
    }

    df = df.rename(columns=columns_rename)

    columns = ["id_orden", "id_orden_producto", "ean", "peso", "precio", "ref_id"]

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
        INSERT INTO ecommdata.orden_producto_pesables (id,"""+columns_query+""") 
        VALUES ("""+values_query+""");
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
    'etl_orden_producto_pesables_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_producto_pesables desde Janis Replica Unimarc hasta Workspace.",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2022, 2, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "orden_producto_pesables", "unimarc", "cyber", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_producto_pesables de Janis Unimarc a Workspace. \n
    UPSERT incremental basado registros creados por el etl de la tabla ordenes.
    """ 
    t0 = BranchPythonOperator(
        task_id = "check_empty_table",
        python_callable = _check_empty_table
    )

    t1 = PythonOperator(
        task_id = "load_full_table",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT woiw.*, woi.ref_id, wo.seq_id
                FROM janis_jackie.wms_orders AS wo
                INNER JOIN janis_jackie.wms_order_items woi
                ON woi.order_id = wo.id
                INNER JOIN janis_jackie.wms_order_item_weighables AS woiw
                ON woi.id = woiw.order_item
            """,
            "query_name": "wms_order_item_weighables",
        }
    )

    t2 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "janis/replica/wms_orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_orders.csv",
        bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket'),
        aws_conn_id = "aws_s3_connection",
        timeout = 1800
    )

    t3 = PythonOperator(
        task_id = "get_order_item_weighables_from_janis",
        python_callable = _get_order_item_weighables_from_janis
    )

    t4 = PythonOperator(
        task_id = "orden_producto_pesables_incremental_load",
        python_callable = _order_item_weighables_table_incremental_load,
        trigger_rule = "none_failed"
    )

    t5 = PythonOperator(
        task_id = "delete_order_item_weighables_to_overwrite",
        python_callable = _delete_order_item_weighables_from_postgres
    )

    t0 >> t1
    t0 >> t2 >> [t5, t3] >> t4
    t1 >> t4
