from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.bigquery_utils import load_custom_bq_query_to_s3
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum

def _extract_active_stock_from_dw(ts, ti):
    """
    1. Lee de postgres las tiendas S10 activas.
    2. Construye el string SQL usando esas tiendas.
    3. LLama a BigQuery y sube el CSV a S3.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    cursor.execute("SELECT id_tienda FROM ecommdata_s10.tiendas WHERE last_millers_rappi = TRUE;")
    active_stores = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    
    if not active_stores:
        print("No hay tiendas activas marcadas para S10 en ecommdata_s10.tiendas. Deteniendo la extracción.")
        return None
        
    store_ids_list = [f"'{store[0]}'" for store in active_stores]
    store_ids_str = ",".join(store_ids_list)
    print(f"Extrayendo stock para las siguientes tiendas habilitadas: {store_ids_str}")

    query = f"""
        SELECT DISTINCT
            CAST(O.OU_ID AS STRING)                           AS ID_TIENDA,
            CAST(L.DATE_VALUE AS TIMESTAMP)                   AS FECHA_MEDICION_INVENTARIO,
            CAST(H.SKU_PRODUCT AS STRING)                     AS SKU,
            H.SKU_NM                                          AS DESC_SKU,
            CAST(H.UMB AS STRING)                             AS UMB,
            CAST(L.STOCK_UMB_ST AS INT64)                     AS STOCK_UMB,
            CAST(L.IN_STOCK_FOTO AS INT64)                    AS INSTOCK,
            CASE WHEN (COALESCE(L.BLOQUEO_TIENDA,'') = '' OR COALESCE(L.BLOQUEO_FORMATO,'') = '')
                THEN FALSE ELSE TRUE END                     AS BLOQUEOS
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_OU_LOGT_SMY`        L
            JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_OU_HIERARCHY`        O
                ON L.OU_KEY = O.OU_KEY AND O.OU_ID IN ({store_ids_str})
            JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY` H
                ON L.SKU_KEY = H.SKU_KEY
            WHERE
            H.SKU_PRODUCT IS NOT NULL
            AND O.OU_KEY IS NOT NULL
            AND L.CONSIG <> 'X'
            AND L.GDS_PD_TP_ID <> 'VERP'
            AND CAST(L.APLICA_STOCK AS STRING) = 'S'
            AND DATE(L.DATE_VALUE) = DATE_SUB(PARSE_DATE('%Y-%m-%d', '{ts[:10]}'), INTERVAL 1 DAY);
    """
    
    # Reutilizamos la función madura guardando directamente hacia s3
    file_name = load_custom_bq_query_to_s3(
        ts=ts, 
        query=query, 
        query_name="stock_s10", 
        aws_conn_id="aws_s3_connection"
    )
    return file_name

def _load_to_postgres(ti):
    import numpy as np
    import pandas as pd
    
    stock_s10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    
    if not stock_s10_file:
        print("No hay archivo de stock para cargar (probablemente no había tiendas activas).")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + stock_s10_file)
    if not s3_hook.check_for_key(stock_s10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % stock_s10_file)

    stock_s10_object = s3_hook.get_key(stock_s10_file, bucket_name=s3_bucket)

    column_types = {
        "ID_TIENDA": "str",
        "FECHA_MEDICION_INVENTARIO": "str", 
        "SKU": "str",
        "DESC_SKU": "str",
        "UMB": "str",
        "INSTOCK": "bool",
        "BLOQUEOS": "bool"
    }

    df = pd.read_csv(stock_s10_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return
    
    columns = [
        "id_tienda",
        "fecha_carga",
        "material",
        "descripcion_producto",
        "umv",
        "stock",
        "in_stock",
        "bloqueos"
    ]

    df.columns = columns
    df["stock"] = pd.to_numeric(df["stock"], errors='coerce').fillna(0).astype(int)
    df["fecha_carga"] = pd.to_datetime(df["fecha_carga"])
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    # Normalización Universal de Medidas (UOM)
    # ST (Sachet) -> UN (Unidad/Unit)
    # CS/CJA (Case/Caja) -> CJ (Caja)
    def normalize_uom(uom):
        if not uom: return uom
        uom_upper = str(uom).strip().upper()
        if uom_upper in ['ST', 'UN']: return 'UN'
        if uom_upper in ['CS', 'CJ', 'CJA']: return 'CJ'
        return uom_upper

    df["umv"] = df["umv"].apply(normalize_uom)

    # Convertimos los nan booleanos a python None o false para evitar fallos
    df["in_stock"] = df["in_stock"].fillna(False)
    df["bloqueos"] = df["bloqueos"].fillna(False)

    records = list(df.to_records(index=False))
    
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

    # UPSERT en bloque ultra rápido (Bulk Insert)
    incremental_query = """
        INSERT INTO ecommdata_s10.stock (id_tienda, fecha_carga, material, descripcion_producto, umv, stock, in_stock, bloqueos)
        VALUES %s
        ON CONFLICT (id_tienda, fecha_carga, material, umv)
        DO UPDATE SET 
            descripcion_producto = EXCLUDED.descripcion_producto,
            stock = EXCLUDED.stock,
            in_stock = EXCLUDED.in_stock,
            bloqueos = EXCLUDED.bloqueos;
    """

    from psycopg2.extras import execute_values
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    execute_values(cursor, incremental_query, fixed_records, page_size=15000)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    
    print("Data loaded perfectly via Upsert into ecommdata_s10.stock.")
    return

def _cleanup_old_data():
    """Limpia todo el stock mas viejo de 7 días."""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    delete_query = "DELETE FROM ecommdata_s10.stock WHERE fecha_carga < CURRENT_DATE - INTERVAL '7 days';"
    cursor.execute(delete_query)
    
    deleted_rows = cursor.rowcount
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print(f"Limpieza automatizada completada: {deleted_rows} registros eliminados (> 7 días).")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_s10',
    default_args=default_args,
    description="Extracción óptima de stock de S10 desde BigQuery",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2024, 5, 28, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["S10", "DW", "S3", "stock", "ecommerce", "last-millers", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción robusta de Stock para la integración **S10**.
    
    - **Filtro dinámico:** Extrae única y exclusivamente las tiendas flaggeadas como TRUE en `ecommdata_s10.tiendas`.
    - **Upsert:** Manejo natural de inserciones dobles por retrocesos en la fecha.
    - **Retención a 7 días:** Autolimpia registros viejos de la base PostgreSQL ahorrando Gigabytes valiosos.
    """ 
    
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = _extract_active_stock_from_dw
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
