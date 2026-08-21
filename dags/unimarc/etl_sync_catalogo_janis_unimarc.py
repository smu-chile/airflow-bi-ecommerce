from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.slack_utils import dag_failure_slack, dag_success_slack
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import sqlalchemy
import time
import io
import os


def fetch_products_page(session, url, headers, page, page_size):
    """Consulta una página de productos desde la API de Janis."""
    page_headers = headers.copy()
    page_headers["X-Janis-Page"] = str(page)
    page_headers["X-Janis-Page-Size"] = str(page_size)

    response = session.get(url, headers=page_headers, timeout=60)
    if response.status_code != 200:
        raise Exception(
            f"Fallo al consultar la API de Janis. Código: {response.status_code} | Respuesta: {response.text}"
        )
    return page, response.json()


# -------------------------------------------------------------------------
# TAREA 1: Extraer catálogo desde la API de Janis (Paralelo + Safe Backoff)
# -------------------------------------------------------------------------
def _extraer_catalogo_janis_api(**kwargs):
    ts_nodash = kwargs.get("ts_nodash", str(int(time.time())))
    t_start = time.time()

    # 1. Obtener credenciales de Janis Unimarc
    janis_api_key = Variable.get("JANIS_API_KEY")
    janis_api_secret = Variable.get("JANIS_API_SECRET")
    janis_client = Variable.get("JANIS_CLIENT")
    janis_base_url = Variable.get("JANIS_API_URL")

    if not all([janis_api_key, janis_api_secret, janis_client, janis_base_url]):
        raise Exception(
            "Faltan variables de configuración de Janis (JANIS_API_URL, JANIS_API_KEY, JANIS_API_SECRET, JANIS_CLIENT)."
        )

    url = f"{janis_base_url.rstrip('/')}/products"
    print(f"🚀 Iniciando extracción multihilo del catálogo Unimarc desde la API de Janis: {url}")

    # Configurar sesión HTTP con retries exponenciales ante 429/5xx y pool de conexiones optimizado
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=25, pool_maxsize=25))
    session.mount("http://", HTTPAdapter(max_retries=retries, pool_connections=25, pool_maxsize=25))

    headers = {
        "janis-api-key": janis_api_key,
        "janis-api-secret": janis_api_secret,
        "janis-client": janis_client,
        "Connection": "keep-alive",
    }

    products_extracted = []
    page = 1
    page_size = 100
    batch_size = 20
    max_workers = 20
    stop_fetching = False

    # Bucle de paginación multihilo (lotes paralelos de 20 páginas)
    while not stop_fetching:
        pages_to_fetch = list(range(page, page + batch_size))
        batch_results = {}

        print(
            f"  Consultando lote de páginas en paralelo: {pages_to_fetch[0]} a {pages_to_fetch[-1]} (workers={max_workers})..."
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_products_page, session, url, headers, p, page_size): p
                for p in pages_to_fetch
            }

            for future in as_completed(futures):
                p_num = futures[future]
                try:
                    p_num, data = future.result()
                    batch_results[p_num] = data
                except Exception as e:
                    print(f"❌ Error extrayendo la página {p_num}: {e}")
                    raise e

        # Procesamos los resultados en orden correlativo de página
        for p_num in sorted(pages_to_fetch):
            data = batch_results.get(p_num)

            if not data or len(data) == 0:
                print(f"🏁 Página vacía {p_num} recibida. Finalizando extracción.")
                stop_fetching = True
                break

            for prod in data:
                product_name = prod.get("Name", "")
                product_description = prod.get("Description", "")
                product_active = prod.get("IsActive", False)
                show_without_stock = prod.get("ShowWithoutStock", False)
                product_category = str(prod.get("Category", "")).strip() if prod.get("Category") is not None else ""
                stores = prod.get("Stores", [])
                items = prod.get("Items", [])

                stores_list = [str(s).strip() for s in stores if s is not None and str(s).strip() != ""]
                cant_tiendas = len(stores_list)
                tiendas_str = ",".join(stores_list)

                for item in items:
                    sku_ref_id = item.get("IdSku")
                    sku_active = item.get("IsActive", False)
                    is_active = bool(product_active and sku_active and cant_tiendas > 0)

                    if sku_ref_id:
                        products_extracted.append(
                            {
                                "ref_id": sku_ref_id,
                                "nombre_producto": product_name,
                                "descripcion": product_description,
                                "activo": is_active,
                                "show_without_stock": bool(show_without_stock),
                                "id_categoria": product_category,
                                "cant_tiendas": cant_tiendas,
                                "tiendas": tiendas_str,
                            }
                        )

        page += batch_size
        if not stop_fetching:
            time.sleep(0.05)

    elapsed = time.time() - t_start
    print(f"\n==================================================")
    print(f"✅ Extracción completada en {elapsed:.2f} segundos ({elapsed/60:.2f} min).")
    print(f"Total registros raw extraídos: {len(products_extracted)}")
    print(f"==================================================\n")

    if len(products_extracted) == 0:
        raise Exception("No se extrajeron registros desde la API de Janis.")

    # Guardar en archivo parquet temporal para el siguiente paso
    raw_file = f"/tmp/janis_raw_unimarc_{ts_nodash}.parquet"
    df_raw = pd.DataFrame(products_extracted)
    df_raw.to_parquet(raw_file, index=False)
    print(f"Datos crudos guardados temporalmente en: {raw_file}")

    return raw_file


# -------------------------------------------------------------------------
# TAREA 2: Transformar y deduplicar a 1 fila por SKU
# -------------------------------------------------------------------------
def _transformar_catalogo_unimarc(**kwargs):
    ti = kwargs["ti"]
    ts_nodash = kwargs.get("ts_nodash", str(int(time.time())))
    raw_file = ti.xcom_pull(task_ids="extraer_catalogo_janis_api")

    if not raw_file or not os.path.exists(raw_file):
        raise FileNotFoundError(f"Archivo raw no encontrado: {raw_file}")

    print(f"Leyendo archivo de extracción: {raw_file}")
    df = pd.read_parquet(raw_file)
    total_raw = len(df)

    # 1. Limpieza de nulos y estandarización
    df = df.dropna(subset=["ref_id"])
    df["ref_id"] = df["ref_id"].astype(str).str.strip()
    df["nombre_producto"] = df["nombre_producto"].fillna("").astype(str).str.strip()
    df["descripcion"] = df["descripcion"].fillna("").astype(str).str.strip()
    df["activo"] = df["activo"].fillna(False).astype(bool)
    df["show_without_stock"] = df["show_without_stock"].fillna(False).astype(bool)
    df["id_categoria"] = df["id_categoria"].fillna("").astype(str).str.strip()
    df["cant_tiendas"] = df["cant_tiendas"].fillna(0).astype(int)
    df["tiendas"] = df["tiendas"].fillna("").astype(str)

    # 2. Deduplicación por SKU (1 fila por producto/SKU)
    df_unique = df.drop_duplicates(subset=["ref_id"], keep="first")
    total_unique = len(df_unique)

    # Métricas de catálogo
    total_activos = (df_unique["activo"] == True).sum()
    total_inactivos = (df_unique["activo"] == False).sum()
    con_tiendas = (df_unique["cant_tiendas"] > 0).sum()
    sin_tiendas = (df_unique["cant_tiendas"] == 0).sum()

    print(f"\n==================================================")
    print(f"📊 RESUMEN TRANSFORMACIÓN CATÁLOGO UNIMARC:")
    print(f"  - Total registros procesados: {total_raw}")
    print(f"  - Total SKUs únicos: {total_unique}")
    print(f"  - SKUs activos: {total_activos}")
    print(f"  - SKUs inactivos: {total_inactivos}")
    print(f"  - SKUs con tiendas asignadas: {con_tiendas}")
    print(f"  - SKUs sin tiendas (huérfanos): {sin_tiendas}")
    print(f"==================================================\n")

    transformed_file = f"/tmp/janis_transformed_unimarc_{ts_nodash}.parquet"
    df_unique.to_parquet(transformed_file, index=False)

    # Limpiar archivo crudo temporal
    try:
        os.remove(raw_file)
    except Exception:
        pass

    return transformed_file


# -------------------------------------------------------------------------
# TAREA 3: Carga atómica a Postgres con COPY y ANALYZE
# -------------------------------------------------------------------------
def _cargar_postgres_productos_janis(**kwargs):
    ti = kwargs["ti"]
    transformed_file = ti.xcom_pull(task_ids="transformar_catalogo_unimarc")

    if not transformed_file or not os.path.exists(transformed_file):
        raise FileNotFoundError(f"Archivo transformado no encontrado: {transformed_file}")

    print(f"Leyendo catálogo transformado desde: {transformed_file}")
    df = pd.read_parquet(transformed_file)
    total_rows = len(df)

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(
        conn_url,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"connect_timeout": 30},
    )

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS ecommdata.productos_janis_api (
            ref_id TEXT,
            nombre_producto TEXT,
            descripcion TEXT,
            activo BOOLEAN,
            show_without_stock BOOLEAN,
            id_categoria TEXT,
            cant_tiendas INT,
            tiendas TEXT
        );
        ALTER TABLE ecommdata.productos_janis_api ADD COLUMN IF NOT EXISTS id_categoria TEXT;
        ALTER TABLE ecommdata.productos_janis_api ADD COLUMN IF NOT EXISTS cant_tiendas INT;
        ALTER TABLE ecommdata.productos_janis_api ADD COLUMN IF NOT EXISTS tiendas TEXT;
    """

    print("Preparando buffer en memoria para COPY atómico...")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    print(f"Ejecutando TRUNCATE y COPY de {total_rows} filas en ecommdata.productos_janis_api...")
    t_copy = time.time()
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            cursor.execute("TRUNCATE TABLE ecommdata.productos_janis_api;")
            columns_str = ",".join(df.columns)
            cursor.copy_expert(
                f"COPY ecommdata.productos_janis_api ({columns_str}) FROM STDIN WITH CSV DELIMITER '\t' NULL '\\N'",
                buffer,
            )
        raw_conn.commit()
    finally:
        raw_conn.close()

    print(f"✅ COPY atómico finalizado en {time.time() - t_copy:.2f} segundos.")

    # Actualizar estadísticas del motor PostgreSQL
    with engine.begin() as conn:
        print("Actualizando estadísticas de la tabla (ANALYZE)...")
        conn.execute("ANALYZE ecommdata.productos_janis_api;")

    engine.dispose()

    # Limpiar archivo temporal
    try:
        os.remove(transformed_file)
    except Exception:
        pass

    print(f"✅ Carga a PostgreSQL finalizada exitosamente ({total_rows} SKUs guardados).")


# -------------------------------------------------------------------------
# DAG DEFINITION & ORCHESTRATION
# -------------------------------------------------------------------------
default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_sync_catalogo_janis_unimarc",
    default_args=default_args,
    description="Extracción y sincronización del catálogo Unimarc desde API Janis a Postgres (cada 6h) y trigger de conciliación VTEX",
    schedule_interval="0 */6 * * *",  # Ejecuta cada 6 horas (00:00, 06:00, 12:00, 18:00)
    start_date=pendulum.datetime(2026, 8, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "Unimarc", "API", "catalogo", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ## Sincronización Catálogo Unimarc desde API Janis (Modularizado)

    Pipeline modularizado en 4 etapas:
    1. `extraer_catalogo_janis_api`: Descarga multihilo segura (15 workers + Backoff 429/5xx) de la API Janis `/products`.
    2. `transformar_catalogo_unimarc`: Deduplica por SKU, calcula `cant_tiendas` y formatea columnas a 1 fila por producto/SKU.
    3. `cargar_postgres_productos_janis`: Ejecuta TRUNCATE + COPY atómico (1 seg) en `ecommdata.productos_janis_api`.
    4. `trigger_conciliacion_vtex`: Dispara automáticamente el DAG `etl_conciliacion_activos_janis_vtex`.
    """

    t1 = PythonOperator(
        task_id="extraer_catalogo_janis_api",
        python_callable=_extraer_catalogo_janis_api,
    )

    t2 = PythonOperator(
        task_id="transformar_catalogo_unimarc",
        python_callable=_transformar_catalogo_unimarc,
    )

    t3 = PythonOperator(
        task_id="cargar_postgres_productos_janis",
        python_callable=_cargar_postgres_productos_janis,
    )

    t4 = TriggerDagRunOperator(
        task_id="trigger_conciliacion_vtex",
        trigger_dag_id="etl_conciliacion_activos_janis_vtex",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    t1 >> t2 >> t3 >> t4
