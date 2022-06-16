from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.janis_alvi_utils import load_full_table_to_s3

from datetime import datetime

def _get_table_price_from_S3(ts, ti):
    import pandas as pd

    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    price_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+price_file)
    if not s3_hook.check_for_key(price_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % price_file)

    orders_object = s3_hook.get_key(price_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    return df

def _save_table_price(ts, ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy

    df = _get_table_price_from_S3(ts, ti)
    
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
        "id",
        "item_id", # cruzar para quedar con item_id, ref_id y descr
        "store_id", # id_tienda_janis
        "price",
        "list_price",
        "cost_price",
        "valid_from",
        "valid_to",
        "publish_attempts",
        "publish_last_attempt",
        "publish_next_attempt",
        "blocked_by_audit",
        "status",
        "user_published",
        "user_modified",
        "user_created",
        "date_modified",
        "date_created",
        "date_published"
    ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "item_id": "id_sku_janis", # cruzar para quedar con item_id, ref_id y descr
        "store_id": "id_tienda_janis", # id_tienda_janis
        "price": "precio",
        "list_price": "precio_lista",
        "cost_price": "costo",
        "valid_from": "valido_desde",
        "valid_to": "valido_hasta",
        "publish_attempts": "intentos_publicacion",
        "publish_last_attempt": "ultimo_intento_publicacion",
        "publish_next_attempt": "proximo_intento_publicacion",
        "blocked_by_audit": "bloqueado_por_auditoria",
        "status": "estado",
        "user_published": "publicado_por",
        "user_modified": "modificado_por",
        "user_created": "creado_por",
        "date_modified": "fecha_modificacion",
        "date_created": "fecha_creacion",
        "date_published": "fecha_publicacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["valido_desde"] = pd.to_datetime(df["valido_desde"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["valido_hasta"] = pd.to_datetime(df["valido_hasta"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["ultimo_intento_publicacion"] = pd.to_datetime(df["ultimo_intento_publicacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["proximo_intento_publicacion"] = pd.to_datetime(df["proximo_intento_publicacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_publicacion"] = pd.to_datetime(df["fecha_publicacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["valido_hasta"] = pd.to_datetime(df["valido_hasta"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]

    df = df.astype({
        "id": "int",
        "id_sku_janis": "int",
        "id_tienda_janis": "int",
        "precio": "int",
        "precio_lista": "int",
        "costo": "int",
        "valido_desde": "string",
        "valido_hasta": "string",
        "intentos_publicacion": "int",
        "ultimo_intento_publicacion": "string",
        "proximo_intento_publicacion": "string",
        "bloqueado_por_auditoria": "bool",
        "estado": "int",
        "publicado_por": "int",
        "modificado_por": "int",
        "creado_por": "int",
        "fecha_modificacion": "string",
        "fecha_creacion": "string",
        "fecha_publicacion": "string",
        "fecha_modificacion_unixtime": "string"
    }, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    query_skus = """
        SELECT id, ref_id, nombre_sku
        FROM ecommdata_alvi.skus;
    """
    df_skus = pd.read_sql(query_skus, engine)
    df_skus = df_skus.rename(columns={"id": "id_sku_temp"})

    print(f"Num records prices: {len(df.index)}")
    print(f"Num records skus: {len(df_skus.index)}")

    df = df.merge(df_skus, how="left", left_on="id_sku_janis", right_on="id_sku_temp")
    df = df.drop(columns=["id_sku_temp"])
    print(len(df.index))
    print(df.columns)

    columns = [
        "id_sku_janis",
        "ref_id",
        "nombre_sku",
        "id_tienda_janis", 
        "precio",
        "precio_lista",
        "costo",
        "valido_desde",
        "valido_hasta",
        "intentos_publicacion",
        "ultimo_intento_publicacion",
        "proximo_intento_publicacion",
        "bloqueado_por_auditoria",
        "estado",
        "publicado_por",
        "modificado_por",
        "creado_por",
        "fecha_modificacion",
        "fecha_creacion",
        "fecha_publicacion"
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
    print(f"Number of records to load: {str(len(fixed_records))}")

    connection = engine.connect()
    upsert_query = f""" 
                    INSERT INTO ecommdata_alvi.precios
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                    ON CONFLICT ON CONSTRAINT precios_pk
                    DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""")
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
    'etl_precios_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla precios desde Janis Alvi a S3 y staging.",
    schedule_interval="0 7 * * *",
    start_date=datetime(2022, 6, 16),
    catchup=False,
    tags=["DATA", "Janis", "precios", "alvi"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla precios desde Janis Alvi a S3 y staging.
    """ 
    
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "precios", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )
    
    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "price", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "save_table_price",
        python_callable = _save_table_price,
    )

t0 >> t1 >> t2