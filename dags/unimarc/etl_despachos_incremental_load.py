from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.janis_utils import load_custom_query_to_s3
from utils.postgres_utils import is_empty_table

from datetime import datetime, timedelta

def _evaluate_full_load(ti, schema, table_name):
    if is_empty_table(schema, table_name):
        ti.xcom_push(key="load_method", value="full_load")
        return "load_full_table_to_s3"
    else:
        ti.xcom_push(key="load_method", value="incremental_load")
        return "wait_for_orders_s3_file"

def _get_new_orders_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df


def _get_order_shipping_from_janis(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "empty"
        return s3_object_name
    if len(order_ids) > 2500:
        print("ERROR: id list is too long. Rec: TRUNCATE and performe a full load.")
        raise Exception("ERROR: id list is too long. Rec: TRUNCATE and performe a full load.")
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT wo.seq_id, wos.*
        FROM janis_jackie.wms_order_shipping as wos 
        LEFT JOIN janis_jackie.wms_orders wo
        ON wo.id = wos.order_id
        WHERE wos.order_id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_shipping")
    return s3_object_name

def _order_shipping_table_incremental_load(ts, ti):
    import numpy as np
    import pandas as pd
    
    load_method = ti.xcom_pull(key="load_method", task_ids=["evaluate_full_load"])[0]
    print(f"Load method: {load_method}")
    if load_method == "full_load":
        shipping_file = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]
    else:
        shipping_file = ti.xcom_pull(key="return_value", task_ids=["get_order_shipping_from_janis"])[0]

    if shipping_file == "empty":
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+shipping_file)
    if not s3_hook.check_for_key(shipping_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % shipping_file)

    shipping_object = s3_hook.get_key(shipping_file, bucket_name=s3_bucket)

    df = pd.read_csv(shipping_object.get()["Body"])
    df = df[[
        "id",
        "seq_id",
        "city",
        "state",
        "country",
        "neighborhood",
        "lat",
        "lng",
        "carrier_id",
        "shipping_estimate",
        "shipping_date",
        "original_shipping_date",
        "shipping_window_start",
        "shipping_window_end",
        "shipped_date_start",
        "shipped_date_end"
    ]]  

    column_types = {
        "id": "int",
        "seq_id": "int",
        "city": "string",
        "state": "string",
        "country": "string",
        "neighborhood": "string",
        "lat": "float",
        "lng": "float",
        "carrier_id": "string",
        "shipping_estimate": "int",
        "shipping_date": "int",
        "original_shipping_date": "int",
        "shipping_window_start": "int",
        "shipping_window_end": "int",
        "shipped_date_start": "int",
        "shipped_date_end": "int"
    }

    # # Ensure correct datatypes:
    df = df.astype(column_types, errors="ignore")

    columns_rename = {
        "id": "id",
        "seq_id": "id_orden",
        "city": "ciudad",
        "state": "region",
        "country": "pais",
        "neighborhood": "comuna",
        "lat": "lat",
        "lng": "lng",
        "carrier_id": "id_transportadora",
        "shipping_estimate": "estimado",
        "shipping_date": "fecha_despacho",
        "original_shipping_date": "fecha_original_despacho",
        "shipping_window_start": "inicio_ventana",
        "shipping_window_end": "termino_ventana",
        "shipped_date_start": "fecha_inicio_despacho",
        "shipped_date_end": "fecha_termino_despacho"
    }

    df = df.rename(columns=columns_rename)

    # Cast date columns:
    df["fecha_despacho"] = pd.to_datetime(df["fecha_despacho"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_original_despacho"] = pd.to_datetime(df["fecha_original_despacho"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["inicio_ventana"] = pd.to_datetime(df["inicio_ventana"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["termino_ventana"] = pd.to_datetime(df["termino_ventana"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_inicio_despacho"] = pd.to_datetime(df["fecha_inicio_despacho"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_termino_despacho"] = pd.to_datetime(df["fecha_termino_despacho"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    df["fecha_despacho"] = df["fecha_despacho"].astype("string")
    df["fecha_original_despacho"] = df["fecha_original_despacho"].astype("string")
    df["inicio_ventana"] = df["inicio_ventana"].astype("string")
    df["termino_ventana"] = df["termino_ventana"].astype("string")
    df["fecha_inicio_despacho"] = df["fecha_inicio_despacho"].astype("string")
    df["fecha_termino_despacho"] = df["fecha_termino_despacho"].astype("string")

    columns = [
        "id_orden",
        "ciudad",
        "region",
        "pais",
        "comuna",
        "lat",
        "lng",
        "id_transportadora",
        "estimado",
        "fecha_despacho",
        "fecha_original_despacho",
        "inicio_ventana",
        "termino_ventana",
        "fecha_inicio_despacho",
        "fecha_termino_despacho"
    ]
    
    df = df[["id"] + columns]

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
        INSERT INTO ecommdata.despachos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") ;
    """
    print(incremental_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres. ecommdata.despachos")

    return

def _upsert_cumplimiento_despacho(ts):
    import os
    curr_working_directory = os.getcwd()
    print(os.getcwd())
    with open(curr_working_directory+"/dags/unimarc/sql/upsert_cumplimiento_despacho.sql", "r") as query_file:
        base_query = query_file.read()

    df_new_orders = _get_new_orders_from_s3(ts)
    order_ids = df_new_orders["seq_id"].tolist()
    chunksize = 500
    iter_range = len(order_ids) // chunksize
    for i in range(iter_range+1):
        order_ids_sublist = order_ids[i*chunksize:(i+1)*chunksize]
        if len(order_ids_sublist) == 0:
            break
        order_ids_sublist_str = ",".join([str(id_order) for id_order in order_ids_sublist])
        cumplimiento_query = base_query.replace("{id_list}", order_ids_sublist_str).replace("{ts}", str(ts))

        # Execute query
        print(cumplimiento_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(cumplimiento_query)
        pg_connection.commit()
        cursor.close()
        pg_connection.close()
        print("Data loaded to Postgres. operaciones_unimarc.cumplimiento_despacho")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_despachos_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla despachos desde Janis Replica Unimarc hasta Workspace.",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2022, 2, 1),
    catchup=True,
    max_active_runs = 1,
    tags=["DATA", "Janis", "ecommdata", "despachos", "unimarc", "cumplimiento_despacho", "MATIAS"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de despachos de Janis Unimarc a Workspace. \n
    UPSERT incremental basado registros creados por el etl de la tabla ordenes.
    """ 
    t0 = BranchPythonOperator(
        task_id = "evaluate_full_load",
        python_callable = _evaluate_full_load,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "despachos"
        }
    )

    t1 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT wo.seq_id, wos.*
                FROM janis_jackie.wms_order_shipping as wos 
                LEFT JOIN janis_jackie.wms_orders wo
                ON wo.id = wos.order_id ;
            """,
            "query_name": "wms_order_shipping",
        }
    )

    t2 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "janis/replica/wms_orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_orders.csv",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 300,
        retries = 3,
        retry_delay = timedelta(minutes=1)
    )

    t2_a = S3KeySensor(
        task_id = "wait_for_order_status_changes_s3_file",
        bucket_key = "janis/replica/wms_order_status_changes/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_order_status_changes.csv",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 300,
        retries = 3,
        retry_delay = timedelta(minutes=1)
    )

    t3 = PythonOperator(
        task_id = "get_order_shipping_from_janis",
        python_callable = _get_order_shipping_from_janis
    )

    t4 = PythonOperator(
        task_id = "despachos_incremental_load",
        python_callable = _order_shipping_table_incremental_load,
        trigger_rule = "none_failed"
    )

    t5 = PostgresOperator(
        task_id = "set_tipo_despacho",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/update_despachos_tipo.sql"
    )

    t6 = PythonOperator(
        task_id = "upsert_cumplimiento_despacho",
        python_callable = _upsert_cumplimiento_despacho
    )

    t7 = PostgresOperator(
        task_id = "update_estados_cumplimiento_despacho",
        postgres_conn_id = "postgresql_conn",
        sql = "sql/update_estados_cumplimiento_despacho.sql"
    )

    t0 >> t1
    t0 >> t2 >> t2_a >> t3 >> t4
    t1 >> t4 >> t5 >> t6 >> t7
