from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import timedelta
import pendulum


def _scroll_customers_vtex(ds):
    # -----------------------------------------------------------------------------
    # Función 1: Scroll a VTEX y guarda resultado en archivo JSON temporal --------
    # -----------------------------------------------------------------------------

    import requests
    import json
    import tempfile
    import os
    import logging

    #Descarga clientes creados el día ds-1 desde VTEX y devuelve path al JSON.
    logical_date = pendulum.parse(ds).date()
    target_date = logical_date.subtract(days=1)

    logging.info(f"🔍 Extrayendo clientes para fecha {target_date}")

    account = Variable.get("VTEX_ACCOUNT_NAME") 
    env = Variable.get("VTEX_ENV")
    entity = "AD"

    base_url = f"https://{account}.{env}.com.br/api/dataentities/{entity}/scroll"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.vtex.ds.v10+json",
        "x-vtex-api-appKey": Variable.get("X_VTEX_API_AppKey"),
        "x-vtex-api-appToken": Variable.get("X_VTEX_API_AppToken"),
    }

    #Se hace el filtro basado en fecha de creación de la dirección, en horario UTC (20:00->19:59 CLT)
    start_iso = f"{target_date}T00:00:00Z"
    end_iso = f"{target_date}T23:59:59Z"
    logging.info(f"🔄 Rango de fechas: {start_iso} a {end_iso}")
    params = {
        "_where": f"createdIn between {start_iso} and {end_iso}",
        "_size": "1000",
        "_fields": "_all",
    }

    logging.info(f"🌐 Realizando scroll a {base_url} con params: {params}")
    resp = requests.get(base_url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = resp.headers.get("x-vtex-md-token")

    # Scroll mientras venga token, necesario en caso de haber más de 1000 registros 
    while token:
        scroll = requests.get(base_url, headers=headers, params={"_token": token, "_fields": "_all"}, timeout=30)
        if scroll.status_code == 204 or not scroll.text.strip():
            break
        scroll.raise_for_status()
        chunk = scroll.json()
        if not chunk:
            break
        data.extend(chunk)
        token = scroll.headers.get("x-vtex-md-token")

    if not data:
        logging.info("No hay datos para la fecha.")
        return None

    fd, temp_path = tempfile.mkstemp(prefix=f"vtex_clientes_{target_date}", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as tmp: # Guardar en archivo temporal
        json.dump(data, tmp, ensure_ascii=False)

    logging.info(f"📄 Guardados {len(data)} registros en {temp_path}")
    return temp_path


def _load_customers_to_pg(ti):
        
    # -----------------------------------------------------------------------------
    # Función 2: Carga del archivo temporal ---------------------------------------
    # -----------------------------------------------------------------------------
    import sqlalchemy
    from sqlalchemy import MetaData, Table
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    import logging
    import json
    import re
    import os

    temp_path = ti.xcom_pull(task_ids="scroll_customers")
    if not temp_path:
        logging.info("Nada que cargar, salto la etapa de insert.")
        return

    with open(temp_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # Mapeo JSON -> columnas tabla
    field_map = {
        "addressName": "addressname",
        "addressType": "addresstype",
        "city": "city",
        "complement": "complement",
        "country": "country",
        "countryfake": "countryfake",
        "geoCoordinate": "geocoordinate",
        "neighborhood": "neighborhood",
        "number": "number",
        "postalCode": "postalcode",
        "receiverName": "receivername",
        "reference": "reference",
        "ssoId": "ssoid",
        "state": "state",
        "street": "street",
        "userId": "userid",
        "id": "id",
        "accountId": "accountid",
        "accountName": "accountname",
        "dataEntityId": "dataentityid",
        "createdBy": "createdby",
        "createdIn": "createdin",
        "updatedBy": "updatedby",
        "updatedIn": "updatedin",
        "lastInteractionBy": "lastinteractionby",
        "lastInteractionIn": "lastinteractionin",
        "followers": "followers",
        "tags": "tags",
        "auto_filter": "auto_filter",
    }

    def sanitize(v):
        if isinstance(v, str):
            v = re.sub(r"[\n\r\t]+", " ", v)
            v = re.sub(" +", " ", v).strip()
            return v or None
        return v

    rows = []
    for r in raw:
        if not r.get("id"):
            continue
        clean = {}
        for j_key, db_col in field_map.items():
            val = r.get(j_key)
            if db_col == "geocoordinate" and isinstance(val, list):
                val = json.dumps(val)
            elif isinstance(val, (list, dict)):
                val = json.dumps(val)
            clean[db_col] = sanitize(val)
        rows.append(clean)

    if not rows:
        logging.info("No quedaron filas válidas luego de limpiar, se borra temp y se sale.")
        os.remove(temp_path)
        return

    # --- Upsert a Postgres ---
    hook = PostgresHook(postgres_conn_id="postgresql_conn")
    engine = hook.get_sqlalchemy_engine()
    md = MetaData(schema="clientes")
    direcciones = Table("direcciones_clientes_vtex", md, autoload_with=engine)

    stmt = pg_insert(direcciones).values(rows)
    excluded = stmt.excluded
    update_cols = {
        c.name: getattr(excluded, c.name)
        for c in direcciones.columns
        if c.name != "id"
    }
    stmt = stmt.on_conflict_do_update(index_elements=["id"], set_=update_cols)

    with engine.begin() as conn:
        result = conn.execute(stmt)
        logging.info(f"🚀 Upsert completado: {result.rowcount} filas afectadas")

    os.remove(temp_path)
    logging.info("🧹 Archivo temporal eliminado")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    dag_id="etl_clientes_vtex_diario",
    description="Carga diaria (ds-1) de clientes VTEX a Postgres.",
    schedule_interval="0 17 * * *",
    start_date=pendulum.datetime(2025, 7, 28, tz="America/Santiago"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["clientes", "daily", "datos", "direcciones", "FRANCISCO", "VTEX"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Baja las direcciones de los clientes de ayer (ds-1) desde VTEX y los upserta en la tabla
    `clientes.direcciones_clientes_vtex`.
    """

    t0 = PythonOperator(
        task_id="scroll_customers",
        python_callable=_scroll_customers_vtex,
    )

    t1 = PythonOperator(
        task_id="load_customers_to_pg",
        python_callable=_load_customers_to_pg,
    )

    t0 >> t1
