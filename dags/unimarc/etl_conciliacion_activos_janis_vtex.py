from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.slack_utils import upload_bytes_to_slack, send_text_message, dag_success_slack, dag_failure_slack
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import sqlalchemy
import io
import time


# Canal de Slack de destino productivo (donde se envían las cargas de tienda)
SLACK_CHANNEL_CONCILIACION = "token_slack_carga_tiendas"


def check_product_has_images(session, vtex_root_url, vtex_base_url, headers, prod_id):
    """
    Verifica si un producto tiene al menos una imagen cargada en alguno de sus SKUs en VTEX.
    """
    if not prod_id:
        return False
    try:
        url_skus = f"{vtex_base_url}/sku/stockkeepingunitByProductId/{prod_id}"
        r_skus = session.get(url_skus, headers=headers, timeout=15)
        if r_skus.status_code == 200 and r_skus.json():
            for sku in r_skus.json():
                sku_id = sku.get("Id")
                url_file = f"{vtex_root_url}/api/catalog/pvt/stockkeepingunit/{sku_id}/file"
                r_file = session.get(url_file, headers=headers, timeout=15)
                if r_file.status_code == 200 and len(r_file.json()) > 0:
                    return True  # Tiene al menos 1 imagen
            return False
        return False
    except Exception as e:
        print(f"⚠️ Error al verificar imágenes del producto {prod_id}: {e}")
        return False


def check_vtex_product_status(session, vtex_root_url, vtex_base_url, headers, ref_id):
    """
    Consulta a la API de VTEX para verificar si un producto/SKU está activo o apagado.
    Si está apagado, valida inmediatamente si tiene o no imágenes asociadas.
    """
    if not ref_id:
        return None

    ref_id_clean = str(ref_id).strip()
    url_prod = f"{vtex_base_url}/products/productgetbyrefid/{ref_id_clean}"

    try:
        resp = session.get(url_prod, headers=headers, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            is_active = data.get("IsActive", False)
            prod_id = data.get("Id")
            prod_name = data.get("Name", "")

            if not is_active:
                # Validar si tiene imágenes en VTEX
                has_images = check_product_has_images(session, vtex_root_url, vtex_base_url, headers, prod_id)
                return {
                    "ref_id": ref_id_clean,
                    "estado_vtex": "Producto Inactivo (IsActive=False)",
                    "tiene_imagen": has_images,
                    "nombre_producto": prod_name,
                }
            return None  # Está activo en VTEX

        elif resp.status_code == 404:
            # Fallback a nivel de SKU
            url_sku = f"{vtex_base_url}/sku/stockkeepingunitbyrefid/{ref_id_clean}"
            resp_sku = session.get(url_sku, headers=headers, timeout=20)
            if resp_sku.status_code == 200:
                data_sku = resp_sku.json()
                sku_active = data_sku.get("IsActive", False)
                prod_active = data_sku.get("ProductIsActive", False)
                prod_id = data_sku.get("ProductId")
                prod_name = data_sku.get("NameComplete", "")

                if not sku_active or not prod_active:
                    has_images = check_product_has_images(session, vtex_root_url, vtex_base_url, headers, prod_id)
                    return {
                        "ref_id": ref_id_clean,
                        "estado_vtex": "SKU/Producto Inactivo en VTEX",
                        "tiene_imagen": has_images,
                        "nombre_producto": prod_name,
                    }
                return None  # SKU activo
            elif resp_sku.status_code == 404:
                # No existe en VTEX (sin imagen)
                return {
                    "ref_id": ref_id_clean,
                    "estado_vtex": "No Existe en VTEX (404)",
                    "tiene_imagen": False,
                    "nombre_producto": "",
                }
            else:
                print(f"⚠️ Error al consultar SKU {ref_id_clean} en VTEX: {resp_sku.status_code}")
                return None
        else:
            print(f"⚠️ Error HTTP {resp.status_code} al consultar {ref_id_clean} en VTEX")
            return None

    except Exception as e:
        print(f"❌ Excepción consultando {ref_id_clean} en VTEX: {e}")
        return None


# -------------------------------------------------------------------------
# TAREA 1: Extraer productos activos de Janis con categorías válidas
# -------------------------------------------------------------------------
def _extraer_activos_janis(**kwargs):
    print("Iniciando extracción de SKUs activos de Janis con categoría comercial válida...")
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    query_janis_activos = """
        SELECT DISTINCT j.ref_id
        FROM ecommdata.productos_janis_api j
        JOIN ecommdata.skus s 
            ON j.ref_id = s.ref_id
        JOIN ecommdata.productos p 
            ON s.id_producto = p.id
        JOIN ecommdata.categorias c 
            ON p.id_categoria = c.id
        WHERE j.activo IS TRUE
          AND j.ref_id IS NOT NULL
          AND j.ref_id <> ''
          AND (
              c.n1 IS NOT NULL
              AND c.n1 NOT ILIKE '%No Trabajar%'
              AND c.n1 NOT ILIKE '%Integraci%'
              AND c.n1 NOT ILIKE '%Inactiv%'
              AND c.n1 NOT ILIKE '%Fizzmod%'
              AND COALESCE(c.status, 'activo') = 'activo'
          );
    """

    cursor.execute(query_janis_activos)
    rows = cursor.fetchall()
    lista_ref_ids = [str(r[0]).strip() for r in rows if r[0]]
    cursor.close()
    conn.close()

    print(f"✅ Total SKUs activos en Janis con categoría válida: {len(lista_ref_ids)}")
    return lista_ref_ids


# -------------------------------------------------------------------------
# TAREA 2: Auditar estado e imágenes en VTEX en paralelo (20 Workers)
# -------------------------------------------------------------------------
def _auditar_estado_vtex(**kwargs):
    ti = kwargs["ti"]
    lista_ref_ids = ti.xcom_pull(task_ids="extraer_activos_janis")

    if not lista_ref_ids:
        print("No se recibieron ref_ids desde la tarea anterior.")
        return []

    total_janis_activos = len(lista_ref_ids)
    print(f"Iniciando auditoría en VTEX para {total_janis_activos} items (20 workers concurrentes)...")

    vtex_account = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    vtex_env = Variable.get("VTEX_ENV", default_var="vtexcommercestable")
    vtex_app_key = Variable.get("X_VTEX_API_AppKey")
    vtex_app_token = Variable.get("X_VTEX_API_AppToken")

    vtex_root_url = f"https://{vtex_account}.{vtex_env}.com.br"
    vtex_base_url = f"{vtex_root_url}/api/catalog_system/pvt"

    vtex_headers = {
        "X-VTEX-API-AppKey": vtex_app_key,
        "X-VTEX-API-AppToken": vtex_app_token,
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

    # Sesión con pool optimizado y backoff inteligente ante 429
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=25, pool_maxsize=25))

    items_a_encender = []
    max_workers = 20

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_ref = {
            executor.submit(check_vtex_product_status, session, vtex_root_url, vtex_base_url, vtex_headers, ref_id): ref_id
            for ref_id in lista_ref_ids
        }

        completed_count = 0
        for future in as_completed(future_to_ref):
            completed_count += 1
            if completed_count % 500 == 0 or completed_count == total_janis_activos:
                print(f"  Verificados {completed_count}/{total_janis_activos} productos en VTEX...")

            res = future.result()
            if res:
                items_a_encender.append(res)

    print(f"\n==================================================")
    print(f"Total auditados en VTEX: {total_janis_activos}")
    print(f"Total detectados APAGADOS en VTEX: {len(items_a_encender)}")
    print(f"==================================================\n")

    return items_a_encender


# -------------------------------------------------------------------------
# TAREA 3: Filtrar por imágenes, registrar sin imagen en BD y notificar Slack
# -------------------------------------------------------------------------
def _notificar_slack(**kwargs):
    ti = kwargs["ti"]
    items_a_encender = ti.xcom_pull(task_ids="auditar_estado_vtex")
    lista_ref_ids = ti.xcom_pull(task_ids="extraer_activos_janis")

    total_janis_activos = len(lista_ref_ids) if lista_ref_ids else 0
    fecha_chile = pendulum.now("America/Santiago")
    fecha_str = str(fecha_chile.date())

    if not items_a_encender:
        msg = f"✅ *[Unimarc]* Conciliación Janis vs VTEX finalizada: Todos los *{total_janis_activos}* productos activos en Janis se encuentran correctamente encendidos en VTEX."
        send_text_message(SLACK_CHANNEL_CONCILIACION, msg)
        print("Sin discrepancias. Mensaje de confirmación enviado a Slack.")
        return

    # Separar en dos grupos: Con imagen vs Sin imagen
    items_con_imagen = [x for x in items_a_encender if x.get("tiene_imagen") is True]
    items_sin_imagen = [x for x in items_a_encender if x.get("tiene_imagen") is not True]

    print(f"📊 Desglose de productos apagados en VTEX:")
    print(f"  - Con imagen (aptos para encender): {len(items_con_imagen)}")
    print(f"  - Sin imagen (omitidos de encendido): {len(items_sin_imagen)}")

    # 1. Registrar productos sin imagen en Postgres (Snapshot actual sin duplicados)
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    create_tbl_sql = """
        CREATE TABLE IF NOT EXISTS ecommdata.productos_sin_imagen_vtex (
            ref_id TEXT,
            material TEXT,
            nombre_producto TEXT,
            motivo TEXT,
            fecha_consulta TIMESTAMP
        );
    """
    with engine.begin() as conn:
        conn.execute(create_tbl_sql)
        conn.execute("TRUNCATE TABLE ecommdata.productos_sin_imagen_vtex;")

    if len(items_sin_imagen) > 0:
        print(f"Registrando {len(items_sin_imagen)} productos sin imagen en ecommdata.productos_sin_imagen_vtex...")
        df_sin_img = pd.DataFrame(items_sin_imagen)
        df_sin_img["material"] = df_sin_img["ref_id"].apply(lambda r: str(r).split("-")[0].zfill(18) if "-" in str(r) else str(r).zfill(18))
        df_sin_img["motivo"] = "Sin imagen en VTEX"
        df_sin_img["fecha_consulta"] = fecha_chile.format("YYYY-MM-DD HH:mm:ss")

        df_sin_img_db = df_sin_img[["ref_id", "material", "nombre_producto", "motivo", "fecha_consulta"]].drop_duplicates(subset=["ref_id"])
        df_sin_img_db.to_sql(
            name="productos_sin_imagen_vtex",
            con=engine,
            schema="ecommdata",
            if_exists="append",
            index=False,
            chunksize=2000,
        )
        print(f"✅ Snapshot de {len(df_sin_img_db)} productos sin imagen guardado en Postgres exitosamente.")

    engine.dispose()

    # 2. Si no hay productos con imagen para encender, notificar y salir
    if len(items_con_imagen) == 0:
        msg = (
            f"ℹ️ *[Unimarc - Conciliación VTEX]*\n"
            f"Se detectaron *{len(items_sin_imagen)}* productos apagados en VTEX, pero *ninguno tiene imágenes asociadas*.\n"
            f"⚠️ Se omitió la generación de archivos de encendido y se registraron en `ecommdata.productos_sin_imagen_vtex`."
        )
        send_text_message(SLACK_CHANNEL_CONCILIACION, msg)
        return

    # 3. Generar DataFrames y CSVs SOLO para los que SÍ tienen imagen
    ref_ids_a_encender = pd.DataFrame(items_con_imagen)["ref_id"].unique()

    # Formato Productos: refId;publish;updatePending;visible;active
    productos_rows = [f"{ref_id};1;1;1;1" for ref_id in ref_ids_a_encender]
    df_carga_prod = pd.DataFrame({"refId;publish;updatePending;visible;active": productos_rows})

    # Formato SKUs: refId;publish;updatePending;active
    skus_rows = [f"{ref_id};1;1;1" for ref_id in ref_ids_a_encender]
    df_carga_skus = pd.DataFrame({"refId;publish;updatePending;active": skus_rows})

    # Convertir a CSV en memoria
    buf_prod = io.StringIO()
    buf_skus = io.StringIO()
    df_carga_prod.to_csv(buf_prod, index=False)
    df_carga_skus.to_csv(buf_skus, index=False)

    bytes_prod = buf_prod.getvalue().encode("utf-8")
    bytes_skus = buf_skus.getvalue().encode("utf-8")

    file_prod = f"encendido_productos_vtex_{fecha_str}.csv"
    file_skus = f"encendido_skus_vtex_{fecha_str}.csv"

    # Construir comentarios para Slack con aviso de omitidos si aplica
    omision_msg = ""
    if len(items_sin_imagen) > 0:
        omision_msg = f"\n⚠️ *Aviso:* Se omitieron *{len(items_sin_imagen)}* productos/SKUs del archivo ya que no tienen imágenes en VTEX (guardados en `productos_sin_imagen_vtex`)."

    comment_prod = (
        f"📎<!channel> *[Unimarc - Encendido Masivo VTEX]*\n"
        f"Se detectaron *{len(ref_ids_a_encender)}* productos/SKUs con imagen activos en Janis pero *apagados en VTEX*.{omision_msg}\n"
        f"Archivo de productos: `{file_prod}` :cat0:"
    )
    comment_skus = (
        f"📎<!channel> *[Unimarc - Encendido Masivo VTEX]*\n"
        f"Archivo de SKUs: `{file_skus}` :cat0:"
    )

    print(f"Subiendo {file_prod} a Slack ({SLACK_CHANNEL_CONCILIACION})...")
    upload_bytes_to_slack(
        file_name=file_prod,
        data_bytes=bytes_prod,
        channel_var_name=SLACK_CHANNEL_CONCILIACION,
        initial_comment=comment_prod,
    )

    print(f"Subiendo {file_skus} a Slack ({SLACK_CHANNEL_CONCILIACION})...")
    upload_bytes_to_slack(
        file_name=file_skus,
        data_bytes=bytes_skus,
        channel_var_name=SLACK_CHANNEL_CONCILIACION,
        initial_comment=comment_skus,
    )

    print("✅ Archivos y notificación subidos a Slack exitosamente.")


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
    "etl_conciliacion_activos_janis_vtex",
    default_args=default_args,
    description="Cruce entre catálogo activo de Janis y VTEX con filtro de imágenes (disparado por etl_sync_catalogo_janis_unimarc)",
    schedule_interval=None,  # Disparado por TriggerDagRunOperator tras sincronizar catálogo
    start_date=pendulum.datetime(2026, 8, 19, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "VTEX", "Unimarc", "conciliacion", "catalogo", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ## Conciliación Catálogo Activo Janis vs VTEX (Encendido Masivo con Filtro de Imágenes)

    Pipeline modularizado en 3 etapas:
    1. `extraer_activos_janis`: Consulta productos activos en Janis con categorías válidas desde Postgres.
    2. `auditar_estado_vtex`: Audita en tiempo real contra la API de VTEX (20 workers) detectando apagados y validando imágenes.
    3. `notificar_slack`: 
       - Omite del archivo de encendido aquellos productos que **no tienen imagen en VTEX**.
       - Guarda el registro histórico de los sin imagen en `ecommdata.productos_sin_imagen_vtex` con timestamp local de Chile.
       - Sube los 2 CSVs con los productos válidos y alerta a Slack (`SLACK_CHANNEL_CONCILIACION`).
    """

    t1 = PythonOperator(
        task_id="extraer_activos_janis",
        python_callable=_extraer_activos_janis,
    )

    t2 = PythonOperator(
        task_id="auditar_estado_vtex",
        python_callable=_auditar_estado_vtex,
    )

    t3 = PythonOperator(
        task_id="notificar_slack",
        python_callable=_notificar_slack,
    )

    t1 >> t2 >> t3
