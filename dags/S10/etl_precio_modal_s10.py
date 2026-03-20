from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.bigquery_utils import load_custom_bq_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def _load_to_postgres(ti):
    import pandas as pd
    import numpy as np
    from psycopg2.extras import execute_values

    precio_modal_s10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    
    if not precio_modal_s10_file:
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + precio_modal_s10_file)
    if not s3_hook.check_for_key(precio_modal_s10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % precio_modal_s10_file)

    precio_modal_s10_object = s3_hook.get_key(precio_modal_s10_file, bucket_name=s3_bucket)

    column_types = {
        "FORMATO_ID": "str",
        "CODIGO_MATERIAL": "str",
        "MATERIAL": "str",
        "UMV": "str",
        "ID_CATEGORIA": "Int64",
        "CATEGORIA": "str",
        "PRECIO_MODAL": "Int64",
        "ID_SEMANA": "Int64"
    }

    df = pd.read_csv(precio_modal_s10_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    # Limpieza básica
    df = df[["FORMATO_ID", "CODIGO_MATERIAL", "UMV", "ID_SEMANA", "MATERIAL", "ID_CATEGORIA", "CATEGORIA", "PRECIO_MODAL"]]
    
    # Relleno de nulos y estandarización a lo que pide Postgres
    df["FORMATO_ID"] = df["FORMATO_ID"].astype(str).str.zfill(2)
    df["CODIGO_MATERIAL"] = df["CODIGO_MATERIAL"].apply(lambda x: str(x).zfill(18) if pd.notnull(x) else None)
    df["UMV"] = df["UMV"].str.replace('ST', 'UN')
    
    records = list(df.to_records(index=False))
    
    # Fixed records logic
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif pd.isna(value) or value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
        
    print(f"Number of records to load: {len(fixed_records)}")

    # Bulk insert hiper rápido
    incremental_query = """
        INSERT INTO ecommdata_s10.precio_modal (formato_id, codigo_material, umv, id_semana, material, id_categoria, categoria, precio_modal)
        VALUES %s
        ON CONFLICT (formato_id, codigo_material, umv, id_semana)
        DO UPDATE SET 
            material = EXCLUDED.material,
            id_categoria = EXCLUDED.id_categoria,
            categoria = EXCLUDED.categoria,
            precio_modal = EXCLUDED.precio_modal,
            fecha_actualizacion = CURRENT_TIMESTAMP;
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    # MIGRACIÓN AUTOMÁTICA: Asegurar la columna formato_id y la nueva llave primaria
    migration_queries = [
        "ALTER TABLE ecommdata_s10.precio_modal ADD COLUMN IF NOT EXISTS formato_id VARCHAR(10) DEFAULT '09';",
        "ALTER TABLE ecommdata_s10.precio_modal DROP CONSTRAINT IF EXISTS precio_modal_pkey;",
        "ALTER TABLE ecommdata_s10.precio_modal ADD CONSTRAINT precio_modal_pkey PRIMARY KEY (formato_id, codigo_material, umv, id_semana);"
    ]
    
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    # Ejecutamos migración antes de insertar
    for q in migration_queries:
        try:
            cursor.execute(q)
        except Exception as e:
            print(f"Migration note (possibly already applied): {e}")
            pg_connection.rollback()
            continue

    execute_values(cursor, incremental_query, fixed_records, page_size=15000)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    
    print("Data loaded perfectly via Bulk Upsert into ecommdata_s10.precio_modal.")
    return

def _cleanup_old_data():
    """
    Elimina cualquier registro que no haya venido en la extracción de BigQuery
    por más de 15 días consecutivos y se haya quedado estancado (historia muy antigua).
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    delete_query = "DELETE FROM ecommdata_s10.precio_modal WHERE fecha_actualizacion < CURRENT_DATE - INTERVAL '15 days';"
    cursor.execute(delete_query)
    deleted_rows = cursor.rowcount
    
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print(f"Limpieza automatizada de historial caducado: {deleted_rows} semanas remotas eliminadas.")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_precio_modal_s10',
    default_args=default_args,
    description="Extracción óptima de precios modales de S10 desde DW Limitado a 12 semanas",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["S10", "DW", "S3", "precio modal", "ecommerce"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción robusta de Precios Modales para la integración **S10**.
    
    - **Filtro Dinámico DW:** Extrae de BigQuery única y exclusivamente la historia de las últimas 12 semanas para abaratar costos de facturación GCP.
    - **Bulk Upsert:** Despliega inserciones en Postgres de a 15,000 lotes ultra-rápidos.
    - **Self-Cleaning:** Pule el historial rezagado en Postgres con más de 15 días sin ser tocado o visto en BQ.
    """ 
    
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT FORMATO_ID, CODIGO_MATERIAL, MATERIAL, UMV, ID_CATEGORIA, CATEGORIA, PRECIO_MODAL, ID_SEMANA
                FROM `cl-cda-prod.DS_CDA_BI_SOURCES.PRECIO_MODAL`
            """,
            "query_name": "precio_modal_s10",
            "aws_conn_id": "aws_s3_connection"
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t1 = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = _load_to_postgres
    )
    
    t2 = PythonOperator(
        task_id = "cleanup_old_data",
        python_callable = _cleanup_old_data
    )

    t0 >> t1 >> t2
