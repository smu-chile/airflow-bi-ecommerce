from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

from utils.janis_alvi_utils import incremental_unixtime_load_table_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime
import pendulum

def _incremental_load_attributes_table(ti):
    import numpy as np
    import pandas as pd
    
    attributes_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_atributos_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+attributes_file)
    if not s3_hook.check_for_key(attributes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % attributes_file)

    attributes_object = s3_hook.get_key(attributes_file, bucket_name=s3_bucket)

    df = pd.read_csv(attributes_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "name",
            "category",
            "update_pending",
            "update_error",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "name": "nombre",
        "category": "id_categoria",
        "update_pending": "actualizacion_pendiente",
        "update_error": "error_actualizacion",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["nombre_categoria"] = ""
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "id_categoria": "int",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "actualizacion_pendiente": "bool",
        "error_actualizacion": "bool",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "nombre",
        "id_categoria",
        "nombre_categoria",
        "actualizacion_pendiente",
        "error_actualizacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id",
        "nombre",
        "id_categoria",
        "nombre_categoria",
        "actualizacion_pendiente",
        "error_actualizacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"]]
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
        BEGIN TRANSACTION;
        INSERT INTO ecommdata_alvi.atributos (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
        UPDATE ecommdata_alvi.atributos a
        SET nombre_categoria = c.n1
        FROM ecommdata_alvi.categorias c
        WHERE a.id_categoria = c.id;
        COMMIT;
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

def _incremental_load_attribute_values_table(ti):
    import numpy as np
    import pandas as pd
    
    attribute_values_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_valores_atributo_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+attribute_values_file)
    if not s3_hook.check_for_key(attribute_values_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % attribute_values_file)

    attribute_values_object = s3_hook.get_key(attribute_values_file, bucket_name=s3_bucket)

    df = pd.read_csv(attribute_values_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "attribute",
            "value",
            "update_pending",
            "update_error",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "attribute": "atributo",
        "value": "valor",
        "update_pending": "actualizacion_pendiente",
        "update_error": "error_actualizacion",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "atributo": "int",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "actualizacion_pendiente": "bool",
        "error_actualizacion": "bool",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "atributo",
        "valor",
        "actualizacion_pendiente",
        "error_actualizacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
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
        INSERT INTO ecommdata_alvi.valores_atributo (id,"""+columns_query+""") 
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

def _incremental_load_product_attributes_table(ti):
    import numpy as np
    import pandas as pd
    
    product_attributes_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_atributos_producto_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+product_attributes_file)
    if not s3_hook.check_for_key(product_attributes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % product_attributes_file)

    product_attributes_object = s3_hook.get_key(product_attributes_file, bucket_name=s3_bucket)

    df = pd.read_csv(product_attributes_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["id",
            "product",
            "attribute",
            "attribute_value",
            "value",
            "publish_attempts",
            "publish_last_attempt",
            "user_created",
            "user_modified",
            "date_created",
            "date_modified"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "product": "id_producto_janis",
        "attribute": "id_atributo",
        "attribute_value": "valor_atributo_id",
        "value": "valor",
        "publish_attempts": "intentos_publicacion",
        "publish_last_attempt": "ultimo_intento_publicacion",
        "user_created": "creacion_usuario",
        "user_modified": "modificacion_usuario",
        "date_created": "fecha_creacion",
        "date_modified": "fecha_modificacion"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["ref_id"] = ""
    df["nombre_producto"] = ""
    df["nombre_atributo"] = ""
    df["valor_atributo"] = ""
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["ultimo_intento_publicacion"] = pd.to_datetime(df["ultimo_intento_publicacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

    # Cast numeric values to int

    df = df.astype({
        "id_producto_janis": "int",
        "ultimo_intento_publicacion": "string",
        "fecha_creacion": "string",
        "fecha_modificacion": "string",
        "creacion_usuario": "bool",
        "modificacion_usuario": "bool"
    }, errors="ignore")

    columns = [
        "id_producto_janis",
        "ref_id",
        "nombre_producto",
        "id_atributo",
        "nombre_atributo",
        "valor_atributo_id",
        "valor_atributo",
        "valor",
        "intentos_publicacion",
        "ultimo_intento_publicacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"
    ]

    df = df[["id",
        "id_producto_janis",
        "ref_id",
        "nombre_producto",
        "id_atributo",
        "nombre_atributo",
        "valor_atributo_id",
        "valor_atributo",
        "valor",
        "intentos_publicacion",
        "ultimo_intento_publicacion",
        "creacion_usuario",
        "modificacion_usuario",
        "fecha_creacion",
        "fecha_modificacion",
        "fecha_modificacion_unixtime"]]
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
        INSERT INTO ecommdata_alvi.atributos_producto (id,"""+columns_query+""")
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
    """
    print(incremental_query)
    update_query = """
        BEGIN TRANSACTION;
        UPDATE ecommdata_alvi.atributos_producto ap
        SET ref_id = s.ref_id, nombre_producto = p.nombre
        FROM ecommdata_alvi.skus s
        LEFT JOIN ecommdata_alvi.productos p on s.ref_id = p.ref_id
        WHERE ap.id_producto_janis = s.id;
        UPDATE ecommdata_alvi.atributos_producto ap
        SET nombre_atributo = a.nombre
        FROM ecommdata_alvi.atributos a
        WHERE ap.id_atributo = a.id;
        UPDATE ecommdata_alvi.atributos_producto ap
        SET valor_atributo = va.valor
        FROM ecommdata_alvi.valores_atributo va
        WHERE ap.valor_atributo_id = va.id;
        COMMIT;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
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
    'etl_proceso_atributos_alvi',
    default_args=default_args,
    description="Extracción y carga de tabla atributos, valores_atributo y atributos_producto desde Janis Alvi Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=pendulum.datetime(2022, 8, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "atributos", "valores_atributo", "atributos_producto", "alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla atributos, valores_atributo y atributos_producto desde Janis Alvi Replica hasta Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 

    t0_a = PythonOperator(
        task_id = "get_max_updated_at_date_atributos",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "atributos", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1_a = PythonOperator(
        task_id = "incremental_unixtime_load_table_atributos_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "attributes", 
            "xcom_updated_date_task_id": "get_max_updated_at_date_atributos", 
            "updated_column": "date_modified"
        }
    )

    t2_a = PythonOperator(
        task_id = "incremental_load_attributes_table",
        python_callable = _incremental_load_attributes_table
    )

    t0_b = PythonOperator(
        task_id = "get_max_updated_at_date_valores_atributo",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "valores_atributo", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1_b = PythonOperator(
        task_id = "incremental_unixtime_load_table_valores_atributo_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "attribute_values", 
            "xcom_updated_date_task_id": "get_max_updated_at_date_valores_atributo", 
            "updated_column": "date_modified"
        }
    )

    t2_b = PythonOperator(
        task_id = "incremental_load_attribute_values_table",
        python_callable = _incremental_load_attribute_values_table
    )

    t3 = PythonOperator(
        task_id = "get_max_updated_at_date_atributos_producto",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "atributos_producto", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t4 = PythonOperator(
        task_id = "incremental_unixtime_load_table_atributos_producto_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "product_attributes", 
            "xcom_updated_date_task_id": "get_max_updated_at_date_atributos_producto", 
            "updated_column": "date_modified"
        }
    )

    t5 = PythonOperator(
        task_id = "incremental_load_product_attributes_table",
        python_callable = _incremental_load_product_attributes_table
    )

    t0_a >> t1_a >> t2_a >> t3
    t0_b >> t1_b >> t2_b >> t3
    t3 >> t4 >> t5
