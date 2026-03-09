from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import load_custom_query_to_s3
from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime, timedelta

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
        FROM ecommdata_alvi.orden_productos;
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
        ti.xcom_push(key="load_path", value="get_order_items_from_janis")
        return "wait_for_orders_s3_file"

def _get_new_orders_from_s3(ts):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    orders_file = f"janis/replica_alvi/wms_orders/{curr_datetime}_wms_orders.csv"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _get_order_items_from_janis(ts):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "empty"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT *
        FROM janis_alvicl.wms_order_items AS woi
        WHERE woi.order_id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_items")
    return s3_object_name

def _delete_records_to_update(ts):
    # Delete based on wms_orders.seq_id
    df = _get_new_orders_from_s3(ts)
    order_ids = df["seq_id"].tolist()
    if len(order_ids) == 0:
        return
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        DELETE
        FROM ecommdata_alvi.orden_productos
        WHERE id_orden IN {query_order_ids} 
    """
    print(query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    return

def _order_items_table_incremental_load(ts, ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    df_orders = _get_new_orders_from_s3(ts)
    df_orders = df_orders[["id", "seq_id"]]
    df_orders = df_orders.rename(columns={"id": "original_id"})

    xcom_input_task = ti.xcom_pull(key="load_path", task_ids=["check_empty_table"])[0]
    order_items_file = ti.xcom_pull(key="return_value", task_ids=[xcom_input_task])[0]

    if ti.xcom_pull(key="return_value", task_ids=['get_order_items_from_janis'])[0] == "empty":
        return

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_items_file)
    if not s3_hook.check_for_key(order_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_items_file)

    order_items_object = s3_hook.get_key(order_items_file, bucket_name=s3_bucket)

    column_types = {
        "ref_id": "string",
        "ean": "string",
    } 

    df = pd.read_csv(order_items_object.get()["Body"], dtype=column_types)
    df = df[[
        "id", 
        "order_id",
		"item_index",
		"substitute_of", 
		"sku",
		"product",
		"ref_id",
		"ean",
		"picker",
		"name",
		"list_price",
		"price",
		"selling_price",
		"selling_price_original",
		"quantity",
		"quantity_picked",
		"substitute_type",
		"brand",
		"category",
		"measurement_unit",
		"unit_multiplier"
    ]]  

    df = df.merge(df_orders, how="inner", left_on="order_id", right_on="original_id").drop(columns=["order_id", "original_id"])

    # # Ensure correct datatypes:
    df["item_index"] = df["item_index"].astype("int", errors="ignore")
    df["substitute_of"] = df["substitute_of"].astype("int", errors="ignore")
    df["picker"] = df["picker"].astype("int", errors="ignore")
    df["list_price"] = df["list_price"].astype("int", errors="ignore")
    df["price"] = df["price"].astype("int", errors="ignore")
    df["selling_price"] = df["selling_price"].astype("int", errors="ignore")
    df["selling_price_original"] = df["selling_price_original"].astype("int", errors="ignore")
    df["quantity"] = df["quantity"].astype("int", errors="ignore")
    df["quantity_picked"] = df["quantity_picked"].astype("int", errors="ignore")
    df["substitute_type"] = df["substitute_type"].astype("int", errors="ignore")
    df["brand"] = df["brand"].astype("int", errors="ignore")
    df["category"] = df["category"].astype("int", errors="ignore")
    df["unit_multiplier"] = df["unit_multiplier"].astype("float", errors="ignore")

    columns_rename = {
        "seq_id": "id_orden",
		"item_index": "indice_item",
		"substitute_of": "id_producto_substituido",
		"sku": "sku_vtex_id",
		"product": "producto_vtex_id",
		"picker": "id_picker",
		"name": "descripcion",
		"list_price": "precio_lista",
		"price": "precio",
		"selling_price": "precio_venta",
		"selling_price_original": "precio_venta_original",
		"quantity": "unidades_solicitadas",
		"quantity_picked": "unidades_pickeadas",
		"substitute_type": "id_tipo_substitucion",
		"brand": "id_marca",
		"category": "id_categoria",
		"measurement_unit": "unidad_de_medida",
		"unit_multiplier": "multiplicador_unidad"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="orden_productos",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.orden_productos")

    return

def _create_initial_order_items_table(ti, xcom_name, truncate=True):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    order_items_file = ti.xcom_pull(key="return_value", task_ids=[xcom_name])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_items_file)
    if not s3_hook.check_for_key(order_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_items_file)

    order_items_object = s3_hook.get_key(order_items_file, bucket_name=s3_bucket)

    column_types = {
        "ref_id": "string",
        "ean": "string",
    } 

    df = pd.read_csv(order_items_object.get()["Body"], dtype=column_types)
    df = df[[
        "id", 
        "seq_id",
		"item_index",
		"substitute_of", 
		"sku",
		"product",
		"ref_id",
		"ean",
		"picker",
		"name",
		"list_price",
		"price",
		"selling_price",
		"selling_price_original",
		"quantity",
		"quantity_picked",
		"substitute_type",
		"brand",
		"category",
		"measurement_unit",
		"unit_multiplier"
    ]]  

    # # Ensure correct datatypes:
    df["item_index"] = df["item_index"].astype("int", errors="ignore")
    df["substitute_of"] = df["substitute_of"].astype("int", errors="ignore")
    df["picker"] = df["picker"].astype("int", errors="ignore")
    df["list_price"] = df["list_price"].astype("int", errors="ignore")
    df["price"] = df["price"].astype("int", errors="ignore")
    df["selling_price"] = df["selling_price"].astype("int", errors="ignore")
    df["selling_price_original"] = df["selling_price_original"].astype("int", errors="ignore")
    df["quantity"] = df["quantity"].astype("int", errors="ignore")
    df["quantity_picked"] = df["quantity_picked"].astype("int", errors="ignore")
    df["substitute_type"] = df["substitute_type"].astype("int", errors="ignore")
    df["brand"] = df["brand"].astype("int", errors="ignore")
    df["category"] = df["category"].astype("int", errors="ignore")
    df["unit_multiplier"] = df["unit_multiplier"].astype("float", errors="ignore")

    columns_rename = {
        "seq_id": "id_orden",
		"item_index": "indice_item",
		"substitute_of": "id_producto_substituido",
		"sku": "sku_vtex_id",
		"product": "producto_vtex_id",
		"picker": "id_picker",
		"name": "descipcion",
		"list_price": "precio_lista",
		"price": "precio",
		"selling_price": "precio_venta",
		"selling_price_original": "precio_venta_original",
		"quantity": "unidades_solicitadas",
		"quantity_picked": "unidades_pickeadas",
		"substitute_type": "id_tipo_substitucion",
		"brand": "id_marca",
		"category": "id_categoria",
		"measurement_unit": "unidad_de_medida",
		"unit_multiplier": "multiplicador_unidad"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    if truncate:
        connection = engine.connect()
        truncate_query = "TRUNCATE TABLE ecommdata_alvi.orden_productos"
        connection.execute(text(truncate_query))
        connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="orden_productos",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.orden_productos")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_orden_productos_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla orden_productos desde Janis Alvi Replica hasta Workspace.",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "orden_productos", "Alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de orden_productos de Janis Alvi a Workspace. \n
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
                SELECT woi.*, wo.seq_id
                FROM janis_alvicl.wms_orders AS wo
                INNER JOIN janis_alvicl.wms_order_items woi
                ON woi.order_id = wo.id
            """,
            "query_name": "wms_order_item",
        }
    )

    t2 = PythonOperator(
        task_id = "create_initial_orden_productos_table",
        python_callable = _create_initial_order_items_table,
        op_kwargs = {"truncate": True, "xcom_name": "load_full_table"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t3 = S3KeySensor(
        task_id = "wait_for_orders_s3_file",
        bucket_key = "janis/replica_alvi/wms_orders/{{execution_date.strftime('%Y/%m/%d/%H%M')}}_wms_orders.csv",
        bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket'),
        aws_conn_id = "aws_s3_connection",
        timeout = 1800
    )

    t4 = PythonOperator(
        task_id = "get_order_items_from_janis",
        python_callable = _get_order_items_from_janis
    )

    t5 = PythonOperator(
        task_id = "delete_records_to_update",
        python_callable = _delete_records_to_update
    )

    t6 = PythonOperator(
        task_id = "orden_productos_incremental_load",
        python_callable = _order_items_table_incremental_load
    )

    t0 >> t1 >> t2
    t0 >> t3 >> t4 >> t5 >> t6
