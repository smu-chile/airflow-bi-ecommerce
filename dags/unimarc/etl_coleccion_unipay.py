from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import time
from io import BytesIO
from collections import defaultdict

# from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Colección VTEX de destino para Unipay
COLLECTION_ID = 10432


def get_vtex_headers():
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    return {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
    }


def get_collection_skus(id_coleccion, account_name, environment):
    """Obtiene el conjunto de SKUs actuales de una colección en VTEX"""
    skus = set()
    page = 1
    headers_api = get_vtex_headers()
    while True:
        url = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/products?page={page}&pageSize=50"
        r = requests.get(url, headers=headers_api)
        if r.status_code == 200:
            data = r.json()
            items = data.get('Data', []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            if not items:
                break
            for item in items:
                sku_id = item.get('SkuId') or item.get('id') or item.get('SKU')
                if sku_id:
                    try:
                        skus.add(str(int(float(sku_id))))
                    except (ValueError, TypeError):
                        pass
            page += 1
            if page > 100:
                break
        else:
            break
    return skus


def remove_skus_from_collection(vtex_ids, id_coleccion, account_name, environment):
    """Excluye la lista de SKUs obsoletos de la colección VTEX utilizando el endpoint oficial importexclude"""
    headers_multiform = get_vtex_headers()
    headers_multiform.pop('Content-Type', None)
    vtex_ids = list(set(vtex_ids))
    if not vtex_ids:
        return

    max_length = 1000
    product_batches = [vtex_ids[i:i + max_length] for i in range(0, len(vtex_ids), max_length)]

    for batch in product_batches:
        batch_list = [[int(vtex_id), '', '', ''] for vtex_id in batch]
        df = pd.DataFrame(batch_list, columns=['SKU', 'PRODUCT', 'SKUREFID', 'PRODUCTREFID'])
        df = df.drop_duplicates(subset=["SKU"])

        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        files = {'file': ('collection_products.xls', output, 'application/vnd.ms-excel')}
        url_exclude = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/stockkeepingunit/importexclude"

        print(f"🧹 Enviando POST a {url_exclude} (importexclude) con {len(batch)} SKUs a excluir...")
        r = requests.post(url_exclude, headers=headers_multiform, files=files)
        time.sleep(1)
        print(f"Status importexclude: {r.status_code} | Response: {r.text[:300]}")


def load_collection(vtex_ids, id_coleccion, account_name, environment):
    """Envía la lista de SKUs a la colección VTEX especificada utilizando el endpoint oficial importinsert"""
    headers_multiform = get_vtex_headers()
    headers_multiform.pop('Content-Type', None)
    vtex_ids = list(set(vtex_ids))
    if not vtex_ids:
        print(f"No hay SKUs válidos para cargar en la colección {id_coleccion}")
        return

    max_length = 1000
    product_batches = [vtex_ids[i:i + max_length] for i in range(0, len(vtex_ids), max_length)]

    for batch in product_batches:
        batch_list = [[int(vtex_id), '', '', ''] for vtex_id in batch]
        df = pd.DataFrame(batch_list, columns=['SKU', 'PRODUCT', 'SKUREFID', 'PRODUCTREFID'])
        df = df.drop_duplicates(subset=["SKU"])

        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        files = {'file': ('collection_products.xls', output, 'application/vnd.ms-excel')}
        url_load_collection = f"https://{account_name}.{environment}.com.br/api/catalog/pvt/collection/{id_coleccion}/stockkeepingunit/importinsert"

        collection_not_loaded = True
        retries = 0
        while collection_not_loaded and (retries < 5):
            print(f"Enviando POST a {url_load_collection} (importinsert) con {len(batch)} SKUs (intento {retries+1})")
            r = requests.post(url_load_collection, headers=headers_multiform, files=files)
            print(f"Status: {r.status_code} | Response: {r.text[:300]}")

            if r.status_code in [200, 201, 202, 204]:
                print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                collection_not_loaded = False
            else:
                retries += 1
                time.sleep(2)


def load_ofertas_unipay(**kwargs):
    """
    Consulta promociones vigentes de Unipay desde ecommdata.workflow_promociones,
    filtrando por condiciones de dto_tuc/precio_tuc, y guarda los SKUs vigentes
    en la tabla ecommdata.ofertas_unipay para sincronización con la colección VTEX 10432.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    query = f"""
    WITH unipay_materials AS (
        SELECT DISTINCT wp.material
        FROM ecommdata.workflow_promociones wp
        LEFT JOIN ecommdata.lista8 l8 ON wp.material = l8.material
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND wp.id_mecanica NOT IN (124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50)
          AND wp.tipo_promocion <> 3
          AND wp.n_promocion NOT IN (5552152024, 4040162024, 5552792024, 5552852024, 4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 1120012025)
          AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
          AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
          AND wp.nombre_promocion::text NOT ILIKE '%917%'
          AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
          AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
          AND wp.nombre_promocion::text !~* 'L(0[0-9]{{2}}|[1-9][0-9]{{0,2}})'
          AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
          AND (
              wp.dto_tuc::numeric > 1 OR
              (
                  wp.precio_tuc::numeric > 2000 AND
                  wp.precio_tuc::numeric >= (COALESCE(wp.precio_modal, l8.precio_regular, 0) * 0.30)
              )
          )
    )
    SELECT DISTINCT
        s.vtex_id
    FROM ecommdata.workflow_promociones wp
    JOIN unipay_materials um ON wp.material = um.material
    JOIN ecommdata.productos p ON p.material = wp.material
    JOIN ecommdata.skus s ON s.ref_id = p.ref_id
    LEFT JOIN ecommdata.lista8 l8 ON wp.material = l8.material
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      AND s.vtex_id IS NOT NULL
      AND s.vtex_id::text <> ''
      AND s.vtex_id::text ~ '^[0-9]+$'
      AND wp.id_mecanica NOT IN (124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50)
      AND wp.tipo_promocion <> 3
      AND wp.n_promocion NOT IN (5552152024, 4040162024, 5552792024, 5552852024, 4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 1120012025)
      AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
      AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
      AND wp.nombre_promocion::text NOT ILIKE '%917%'
      AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
      AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
      AND wp.nombre_promocion::text !~* 'L(0[0-9]{{2}}|[1-9][0-9]{{0,2}})'
      AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
      AND (
          wp.dto_tuc::numeric > 1 OR
          (
              wp.precio_tuc::numeric > 2000 AND
              wp.precio_tuc::numeric >= (COALESCE(wp.precio_modal, l8.precio_regular, 0) * 0.30)
          )
      )
    """

    records = pg_hook.get_records(query)
    print(f"📊 Registros de promociones Unipay vigentes encontrados con VTEX ID: {len(records)}")

    # Recolectar vtex_ids válidos
    final_skus = set()
    for r in records:
        try:
            final_skus.add(str(int(float(r[0]))))
        except (ValueError, TypeError):
            continue

    print(f"🔎 Total de SKUs únicos con promociones Unipay: {len(final_skus)}")

    # Recrear tabla
    drop_create_sql = """
    DROP TABLE IF EXISTS ecommdata.ofertas_unipay;
    CREATE TABLE ecommdata.ofertas_unipay (
        sku_id      VARCHAR(50) NOT NULL,
        fecha_carga TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (sku_id)
    );
    """
    pg_hook.run(drop_create_sql)

    if not final_skus:
        print("⚠️ No hay SKUs Unipay vigentes para guardar.")
        return

    # Insertar SKUs
    insert_sql = """
    INSERT INTO ecommdata.ofertas_unipay (sku_id, fecha_carga)
    VALUES (%s, CURRENT_TIMESTAMP)
    ON CONFLICT (sku_id) DO UPDATE SET fecha_carga = EXCLUDED.fecha_carga;
    """
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    records_to_insert = [(s,) for s in final_skus]
    cur.executemany(insert_sql, records_to_insert)
    conn.commit()
    cur.close()
    conn.close()
    print(f"🗄️ Se guardaron {len(final_skus)} SKUs en 'ecommdata.ofertas_unipay'.")


def sync_unipay_vtex(**kwargs):
    """
    Obtiene los SKUs de ecommdata.ofertas_unipay y sincroniza con la colección VTEX 10432
    (excluye obsoletos e inserta los SKUs Unipay vigentes).
    """
    account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    environment = Variable.get("VTEX_ENV", default_var="vtexcommercestable")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    query = """
    SELECT DISTINCT sku_id
    FROM ecommdata.ofertas_unipay
    WHERE sku_id IS NOT NULL
      AND TRIM(sku_id) <> '';
    """
    records = pg_hook.get_records(query)

    vtex_ids_nuevos = {str(int(float(r[0]))) for r in records if r[0]}
    print(f"📦 SKUs únicos a cargar en Colección Unipay ({COLLECTION_ID}): {len(vtex_ids_nuevos)}")

    if not vtex_ids_nuevos:
        print(f"⚠️ No se encontraron SKUs válidos para la colección Unipay ({COLLECTION_ID}).")
        return

    # 1. Obtener SKUs actuales de la colección en VTEX
    skus_actuales = get_collection_skus(COLLECTION_ID, account_name, environment)
    print(f"🔎 SKUs actualmente en colección {COLLECTION_ID}: {len(skus_actuales)}")

    # 2. Excluir SKUs obsoletos que ya no estén vigentes
    skus_a_excluir = skus_actuales - vtex_ids_nuevos
    if skus_a_excluir:
        print(f"🧹 Excluyendo {len(skus_a_excluir)} SKUs obsoletos de colección {COLLECTION_ID}...")
        remove_skus_from_collection(list(skus_a_excluir), COLLECTION_ID, account_name, environment)
    else:
        print(f"✨ No hay SKUs obsoletos en colección {COLLECTION_ID}.")

    # 3. Importar SKUs vigentes mediante importinsert
    print(f"🚀 Insertando {len(vtex_ids_nuevos)} SKUs vigentes en colección {COLLECTION_ID}...")
    load_collection(list(vtex_ids_nuevos), COLLECTION_ID, account_name, environment)


with DAG(
    'etl_cargar_coleccion_unipay',
    default_args=default_args,
    description="Carga de SKUs con promociones Unipay vigentes en la colección VTEX 10432.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "promociones", "Unimarc", "vtex", "colecciones", "unipay"],
    # on_success_callback=dag_success_slack,
    # on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de SKUs con promociones Unipay vigentes en la tabla `ecommdata.ofertas_unipay`
    y sincronización con la colección VTEX 10432.

    Filtros aplicados:
    - Promociones activas hoy (fecha_inicio <= HOY <= fecha_fin)
    - Excluye mecánicas y n_promociones específicas de membresía
    - Filtra por condición de descuento TUC: dto_tuc > 1 OR precio_tuc con umbral mínimo

    Flujo:
    1. `load_ofertas_unipay`: Extrae SKUs vigentes y los guarda en PostgreSQL.
    2. `sync_unipay_vtex`: Sincroniza la colección VTEX (excluye obsoletos, inserta vigentes).
    """

    t_load = PythonOperator(
        task_id="load_ofertas_unipay",
        python_callable=load_ofertas_unipay,
    )

    t_sync = PythonOperator(
        task_id="sync_unipay_vtex",
        python_callable=sync_unipay_vtex,
    )

    t_load >> t_sync
