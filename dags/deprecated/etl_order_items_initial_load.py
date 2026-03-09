from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_custom_query_to_s3

from datetime import datetime, timedelta

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

    if truncate:
        connection = engine.connect()
        truncate_query = "TRUNCATE TABLE ecommdata.orden_productos"
        connection.execute(text(truncate_query))
        connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="orden_productos",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.orden_productos")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_orden_productos_initial_load',
    default_args=default_args,
    description="Extracción y carga inicial de tabla orden_productos desde Janis Replica hasta Workspace.",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "orden_productos"],
) as dag:

    dag.doc_md = """
    Extracción y carga inicial de tabla de orden_productos de Janis.
    """ 

    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT woi.*, wo.seq_id
                FROM janis_jackie.wms_order_items AS woi
                JOIN janis_jackie.wms_orders AS wo
                ON woi.order_id = wo.id
                WHERE wo.id >= 168886
                AND wo.id < 215977;
            """,
            "query_name": "wms_order_items",
        }
    )

    t1 = PythonOperator(
        task_id = "create_initial_orden_productos_table",
        python_callable = _create_initial_order_items_table,
        op_kwargs = {"truncate": True, "xcom_name": "load_full_table_to_s3"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t2 = PythonOperator(
        task_id = "load_full_table_to_s3_2",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT woi.*, wo.seq_id
                FROM janis_jackie.wms_order_items AS woi
                JOIN janis_jackie.wms_orders AS wo
                ON woi.order_id = wo.id
                WHERE wo.id >= 215977;
            """,
            "query_name": "wms_order_items",
        }
    )

    t3 = PythonOperator(
        task_id = "create_initial_orden_productos_table_2",
        python_callable = _create_initial_order_items_table,
        op_kwargs = {"truncate": False, "xcom_name": "load_full_table_to_s3_2"},
        retries = 2,
        retry_delay = timedelta(minutes=1)
    )

    t0 >> t1 >> t2 >> t3
