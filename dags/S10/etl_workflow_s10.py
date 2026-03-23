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

    workflow_s10_file = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    
    if not workflow_s10_file:
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + workflow_s10_file)
    if not s3_hook.check_for_key(workflow_s10_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % workflow_s10_file)

    workflow_s10_object = s3_hook.get_key(workflow_s10_file, bucket_name=s3_bucket)

    df = pd.read_csv(workflow_s10_object.get()["Body"], low_memory=False)
    print(f"Number of records found: {len(df.index)}")

    if len(df.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    columns_ordered = [
        "N_PROMOCION", "MATERIAL", "NOMBRE_PROMOCION", "DESC_MATERIAL",
        "ID_EVENTO", "DESCRIPCION_EVENTO_PROMOCIONAL", "ID_MECANICA",
        "DESCRIPCION_MECANICA", "UN_MEDIDA_VENTA", "ORGANIZACION_VENTAS",
        "CANAL_DISTRIBUCION", "EAN", "LINEA", "DESCRIPCION_LINEA",
        "MARCA", "TIPO_PROMOCION", "DESC_PROMOCION", "PRECIO_MODAL",
        "PRECIO_MODAL_TOTAL", "PRECIO_PROMOCIONAL", "PRECIO_TOTAL_PROMOCIONAL",
        "AHORRO", "AHORRO_TOTAL", "CANTIDAD_N", "CANTIDAD_M",
        "FECHA_INICIO_DE_PROMOCION", "FECHA_FIN_DE_PROMOCION"
    ]
    
    # Filtro de columnas de interés
    df = df[columns_ordered]
    
    # Quitar llaves primarias duplicadas dentro del mismo paquete de BQ 
    # para evitar el error CardinalityViolation en el Bulk Upsert de Postgres
    df = df.drop_duplicates(subset=['N_PROMOCION', 'MATERIAL'], keep='last')
    
    # Relleno de nulos para BQ/Postgres compatibilidad
    df = df.where(pd.notnull(df), None)
    
    # IMPORTANTE: Asegurar los 18 caracteres con ceros a la izquierda para el cruce
    df['MATERIAL'] = df['MATERIAL'].astype(str).str.zfill(18)

    # Normalización Universal de Medidas (UOM)
    # ST (Sachet) -> UN (Unidad/Unit)
    # CS/CJA (Case/Caja) -> CJ (Caja)
    def normalize_uom(uom):
        if not uom: return uom
        uom_upper = str(uom).strip().upper()
        if uom_upper in ['ST', 'UN']: return 'UN'
        if uom_upper in ['CS', 'CJ', 'CJA']: return 'CJ'
        return uom_upper

    df["UN_MEDIDA_VENTA"] = df["UN_MEDIDA_VENTA"].apply(normalize_uom)

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

    incremental_query = """
        INSERT INTO ecommdata_s10.workflow (
            n_promocion, material, nombre_promocion, desc_material, id_evento, 
            descripcion_evento_promocional, id_mecanica, descripcion_mecanica, 
            un_medida_venta, organizacion_ventas, canal_distribucion, ean, linea, 
            descripcion_linea, marca, tipo_promocion, desc_promocion, precio_modal, 
            precio_modal_total, precio_promocional, precio_total_promocional, 
            ahorro, ahorro_total, cantidad_n, cantidad_m, fecha_inicio_de_promocion, 
            fecha_fin_de_promocion
        ) 
        VALUES %s
        ON CONFLICT (n_promocion, material)
        DO UPDATE SET 
            nombre_promocion = EXCLUDED.nombre_promocion,
            desc_material = EXCLUDED.desc_material,
            id_evento = EXCLUDED.id_evento,
            descripcion_evento_promocional = EXCLUDED.descripcion_evento_promocional,
            id_mecanica = EXCLUDED.id_mecanica,
            descripcion_mecanica = EXCLUDED.descripcion_mecanica,
            un_medida_venta = EXCLUDED.un_medida_venta,
            organizacion_ventas = EXCLUDED.organizacion_ventas,
            canal_distribucion = EXCLUDED.canal_distribucion,
            ean = EXCLUDED.ean,
            linea = EXCLUDED.linea,
            descripcion_linea = EXCLUDED.descripcion_linea,
            marca = EXCLUDED.marca,
            tipo_promocion = EXCLUDED.tipo_promocion,
            desc_promocion = EXCLUDED.desc_promocion,
            precio_modal = EXCLUDED.precio_modal,
            precio_modal_total = EXCLUDED.precio_modal_total,
            precio_promocional = EXCLUDED.precio_promocional,
            precio_total_promocional = EXCLUDED.precio_total_promocional,
            ahorro = EXCLUDED.ahorro,
            ahorro_total = EXCLUDED.ahorro_total,
            cantidad_n = EXCLUDED.cantidad_n,
            cantidad_m = EXCLUDED.cantidad_m,
            fecha_inicio_de_promocion = EXCLUDED.fecha_inicio_de_promocion,
            fecha_fin_de_promocion = EXCLUDED.fecha_fin_de_promocion,
            fecha_actualizacion = CURRENT_TIMESTAMP;
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    # Ejecución hiperrápida Batch Size
    execute_values(cursor, incremental_query, fixed_records, page_size=15000)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    
    print("Data loaded perfectly via Bulk Upsert into ecommdata_s10.workflow.")
    return

def _cleanup_expired_promotions():
    """Elimina las promociones que ya finalizaron y caducaron hace más de 7 días de la BD"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    # Se eliminan promociones cuya fecha de fin ya transcurrió holgadamente
    delete_query = "DELETE FROM ecommdata_s10.workflow WHERE fecha_fin_de_promocion < CURRENT_DATE - INTERVAL '7 days';"
    cursor.execute(delete_query)
    deleted_rows = cursor.rowcount
    
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print(f"Limpieza automatizada de Promociones Caducadas: {deleted_rows} filas depuradas.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_workflow_s10',
    default_args=default_args,
    description="Extracción óptima de promociones de S10 desde BigQuery",
    # M10 solía agendarse solo los días 1 y 15 ("15 8 1,15 * *"). 
    # Para S10 lo ponemos DIARIO ("15 8 * * *") para reaccionar al instante a nuevas promociones
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["S10", "BQ", "S3", "workflow", "promociones", "ecommerce"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción robusta de Promociones (Workflow) aislada para la integración **S10**.
    
    - **Aislamiento de M10:** Saca a M10 del camino proveyendo independencia y seguridad.
    - **Bulk Upsert:** Despliega inserciones en Postgres de a 15,000 lotes ultra-rápidos (Adiós al executemany lento).
    - **Programación Diaria:** En vez de actualizar 2 veces al mes como en M10, este corre a diario para estar 100% fresco contra BQ.
    """ 
    
    t0 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT * 
                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW` wf
                WHERE wf.ORGANIZACION_VENTAS = '3000'
                  AND wf.REGISTRO_VALIDO = 'X'
            """,
            "query_name": "workflow_s10",
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
        python_callable = _cleanup_expired_promotions
    )

    t0 >> t1 >> t2
