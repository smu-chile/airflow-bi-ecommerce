from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import datetime, timedelta
import logging
import requests
import json
import io

# Mapeo de día VTEX (0=Domingo, 1=Lunes, ..., 6=Sábado) a formato estándar de la tabla
DIAS_MAP = {
    1: (1, "Lunes"),
    2: (2, "Martes"),
    3: (3, "Miércoles"),
    4: (4, "Jueves"),
    5: (5, "Viernes"),
    6: (6, "Sábado"),
    0: (7, "Domingo"),
}


def extraer_ventanas_vtex_to_s3(ds):
    """
    Consulta las políticas de envío desde la API de Logística de VTEX Unimarc,
    extrae los horarios de entrega programada configurados (ventanas activas e inactivas),
    cruza con el maestro de tiendas de ecommdata.tiendas y sube el archivo procesado a Amazon S3.
    """
    import pandas as pd

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ventanas_activas/{exec_date}/"
    filename = f"ventanas_activas/{exec_date}/ventanas_activas_{date_aux}.csv"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    # 1. Obtener maestro de tiendas de Unimarc para mapeo de nombre_tienda
    logging.info("Consultando maestro de tiendas en ecommdata.tiendas...")
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query_tiendas = "SELECT id, nombre_tienda_janis FROM ecommdata.tiendas WHERE id IS NOT NULL;"
    df_tiendas = pg_hook.get_pandas_df(query_tiendas)

    tiendas_map = {}
    for _, row in df_tiendas.iterrows():
        try:
            t_id = str(row["id"]).strip().zfill(4)
            tiendas_map[t_id] = row["nombre_tienda_janis"]
        except Exception:
            pass
    logging.info(f"Maestro de tiendas cargado con {len(tiendas_map)} tiendas mapeables.")

    # 2. Consultar Shipping Policies desde la API de Logística de VTEX
    vtex_app_key = Variable.get("X_VTEX_API_AppKey")
    vtex_app_token = Variable.get("X_VTEX_API_AppToken")

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-VTEX-API-AppKey": vtex_app_key,
        "X-VTEX-API-AppToken": vtex_app_token,
    }

    url_base = "https://unimarc.vtexcommercestable.com.br/api/logistics/pvt/shipping-policies"
    policies = []
    page = 1
    per_page = 500

    while True:
        url = f"{url_base}?page={page}&perPage={per_page}"
        logging.info(f"Consultando VTEX Shipping Policies: {url}")
        res = requests.get(url, headers=headers, timeout=20)
        res.raise_for_status()

        data = res.json()
        items = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if not items:
            break

        policies.extend(items)
        if len(items) < per_page:
            break
        page += 1

    logging.info(f"Total de políticas de envío obtenidas desde VTEX: {len(policies)}")

    # 3. Procesar las ventanas de entrega programada de cada política
    filas = []
    for policy in policies:
        pid = policy.get("id")
        pname = policy.get("name")
        is_active = "Si" if policy.get("isActive") else "No"

        # Extraer id_tienda a partir del prefijo de la transportadora como string de 4 dígitos (ej. '0005', '0017')
        prefix_str = str(pid).split("-")[0].strip()
        id_tienda = prefix_str.zfill(4) if prefix_str.isdigit() else prefix_str

        nombre_tienda = tiendas_map.get(id_tienda, pname)

        sched_settings = policy.get("deliveryScheduleSettings")
        if sched_settings and sched_settings.get("useDeliverySchedule"):
            day_schedules = sched_settings.get("dayOfWeekForDelivery", [])
            for day_sched in day_schedules:
                dow = day_sched.get("dayOfWeek")
                dia_num, dia_str = DIAS_MAP.get(dow, (dow, str(dow)))

                ranges = day_sched.get("deliveryRanges", [])
                for rng in ranges:
                    start_time = rng.get("startTime")
                    end_time = rng.get("endTime")
                    rango_horario = f"{start_time} - {end_time}"

                    filas.append({
                        "id_tienda": id_tienda,
                        "nombre_tienda": nombre_tienda,
                        "id_transportadora": pid,
                        "transportadora": pname,
                        "dia_num": dia_num,
                        "dia": dia_str,
                        "inicio": start_time,
                        "fin": end_time,
                        "rango_horario": rango_horario,
                        "activa_vtex": is_active,
                        "fecha_actualizacion": pendulum.now("America/Santiago").to_datetime_string(),
                    })

    if not filas:
        logging.warning("No se encontraron ventanas horarias configuradas en las políticas de VTEX.")
        return "empty"

    df_final = pd.DataFrame(filas)
    logging.info(f"Total de ventanas extraídas y estructuradas: {len(df_final)}")

    # Asegurar el orden exacto de las columnas de la tabla en Postgres
    columnas_orden = [
        "id_tienda",
        "nombre_tienda",
        "id_transportadora",
        "transportadora",
        "dia_num",
        "dia",
        "inicio",
        "fin",
        "rango_horario",
        "activa_vtex",
        "fecha_actualizacion",
    ]
    df_final = df_final[columnas_orden]

    # 4. Guardar archivo en Amazon S3
    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    s3_hook.load_string(
        buffer.getvalue(),
        key=filename,
        bucket_name=s3_bucket,
        replace=True,
        encrypt=False,
    )
    logging.info(f"Archivo subido exitosamente a S3: s3://{s3_bucket}/{filename}")

    return filename


def cargar_ventanas_to_postgres(ti):
    """
    Descarga el archivo procesado de S3 y ejecuta TRUNCATE + INSERT en forecast_and_planning.ventanas_activas.
    """
    import pandas as pd
    import sqlalchemy

    filename = ti.xcom_pull(key="return_value", task_ids=["extraer_ventanas_vtex_to_s3"])[0]

    if not filename or filename == "empty":
        logging.info("No hay registros para cargar en Postgres. Tarea finalizada.")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception(f"El archivo {filename} no existe en S3 {s3_bucket}.")

    s3_obj = s3_hook.get_key(filename, bucket_name=s3_bucket)
    df = pd.read_csv(s3_obj.get()["Body"])

    if df.empty:
        logging.info("El DataFrame desde S3 está vacío. Finalizando tarea.")
        return

    logging.info(f"Preparando carga de {len(df)} registros en forecast_and_planning.ventanas_activas...")

    column_types = {
        "id_tienda": "string",
        "nombre_tienda": "string",
        "id_transportadora": "string",
        "transportadora": "string",
        "dia_num": "int64",
        "dia": "string",
        "inicio": "string",
        "fin": "string",
        "rango_horario": "string",
        "activa_vtex": "string",
        "fecha_actualizacion": "datetime64[ns]",
    }
    df = df.astype(column_types, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("ALTER TABLE forecast_and_planning.ventanas_activas ALTER COLUMN id_tienda TYPE text;")
        conn.execute("ALTER TABLE forecast_and_planning.ventanas_activas ADD COLUMN IF NOT EXISTS fecha_actualizacion timestamp without time zone;")
        logging.info("Truncando tabla forecast_and_planning.ventanas_activas...")
        conn.execute("TRUNCATE forecast_and_planning.ventanas_activas")
        logging.info("Insertando nuevas ventanas extraídas...")
        df.to_sql(
            name="ventanas_activas",
            con=conn,
            schema="forecast_and_planning",
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )

    logging.info("Carga completada exitosamente en forecast_and_planning.ventanas_activas.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_ventanas_activas_vtex",
    default_args=default_args,
    description="Extracción y carga diaria de ventanas activas e inactivas de despacho desde VTEX Shipping Policies a Postgres",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "VTEX", "ventanas", "forecast_and_planning", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### ETL Ventanas Activas VTEX Unimarc
    
    1. **Extracción**:
       - Consume la API de Logística de VTEX (`/api/logistics/pvt/shipping-policies`).
       - Extrae los rangos de entrega programada (`deliveryScheduleSettings`) y su estado (`activa_vtex`).
       - Asocia `id_tienda` como string de 4 dígitos (`0005`, `0017`, etc.) y `nombre_tienda` oficial mediante cruce con `ecommdata.tiendas`.
       - Agrega timestamp de procesamiento `fecha_actualizacion` (`America/Santiago`).
    
    2. **Almacenamiento en S3**:
       - Sube un snapshot diario a Amazon S3 (`ventanas_activas/YYYY/MM/DD/...`).
    
    3. **Carga en PostgreSQL**:
       - Asegura las columnas `id_tienda` (text) y `fecha_actualizacion` (timestamp).
       - Realiza `TRUNCATE` e inserción en `forecast_and_planning.ventanas_activas`.
    """

    t0 = PythonOperator(
        task_id="extraer_ventanas_vtex_to_s3",
        python_callable=extraer_ventanas_vtex_to_s3,
    )

    t1 = PythonOperator(
        task_id="cargar_ventanas_to_postgres",
        python_callable=cargar_ventanas_to_postgres,
    )

    t0 >> t1
