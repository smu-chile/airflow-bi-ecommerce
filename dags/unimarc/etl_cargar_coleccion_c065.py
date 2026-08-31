from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pendulum
import requests
import pandas as pd
import json
import time
from io import BytesIO
from collections import defaultdict

# Configuración VTEX y Colección C065 por defecto
COLLECTION_ID = 8413

# from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

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
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    headers_multiform = {
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
        'Accept': 'application/json'
    }
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
        time.sleep(5)
        print(f"Status importexclude: {r.status_code} | Response: {r.text[:300]}")

def load_collection(vtex_ids, id_coleccion, account_name, environment):
    """Envía la lista de SKUs a la colección VTEX especificada utilizando el endpoint oficial importinsert"""
    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey", default_var="vtexappkey-unimarc-QTILMS")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken", default_var="XKJEUCQRZQCQYRDBBENGIVIIOOFBMPRMVTJSYBRDSNLCPXEKWCVKFHVBCAPGWPFKYPVEHLCANKOUKFTRJUFJTHGNYIPXLKGCIBEZCZDLZVWVSARWGXXBKWQQIHFZVFOD")
    headers_multiform = {
        'X-VTEX-API-AppKey': X_VTEX_API_AppKey,
        'X-VTEX-API-AppToken': X_VTEX_API_AppToken,
        'Accept': 'application/json'
    }
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
            time.sleep(10)
            print(f"Status: {r.status_code} | Response: {r.text[:300]}")
            
            try:
                if r.text and r.text.strip():
                    try:
                        node = json.loads(r.text)
                        if isinstance(node, dict) and node.get('TotalProductsProcessed') == len(batch):
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE.")
                            collection_not_loaded = False
                        elif r.status_code in [200, 201, 202, 204]:
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                            collection_not_loaded = False
                        else:
                            retries += 1
                    except json.JSONDecodeError:
                        if r.status_code in [200, 201, 202, 204]:
                            print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                            collection_not_loaded = False
                        else:
                            retries += 1
                elif r.status_code in [200, 201, 202, 204]:
                    print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                    collection_not_loaded = False
                else:
                    retries += 1
            except Exception as err:
                if r.status_code in [200, 201, 202, 204]:
                    print(f"✅ COLECCIÓN {id_coleccion} CARGADA EXITOSAMENTE (Status {r.status_code}).")
                    collection_not_loaded = False
                else:
                    print(f"Error al procesar respuesta: {err}")
                    retries += 1

def calc_effective_unit_price(row):
    """
    Calcula el precio promocional unitario efectivo para una promoción en workflow_promociones.
    Se prioriza wp.precio_promocional como parámetro principal.
    row: (vtex_id, n_promocion, nombre_promocion, canal_distribucion, tipo_promocion, precio_modal, precio_promocional, precio_total_promocional, cantidad_n, porcentaje_de_descuento)
    """
    p_modal = float(row[5]) if row[5] is not None else None
    p_prom = float(row[6]) if row[6] is not None else None
    p_tot_prom = float(row[7]) if row[7] is not None else None
    cant_n = float(row[8]) if row[8] is not None else 1.0
    pct_desc = float(row[9]) if row[9] is not None else None

    # Priorizar wp.precio_promocional si está especificado y es mayor que 0
    if p_prom is not None and p_prom > 0:
        return p_prom

    tipo = row[4]
    if tipo == 1:  # Porcentaje descuento sin p_prom
        if pct_desc is not None and p_modal is not None:
            return round(p_modal * (1.0 - pct_desc), 2)
    elif tipo == 7:  # Oferta por volumen Nx$ sin p_prom
        if p_tot_prom is not None and cant_n > 0:
            return round(p_tot_prom / cant_n, 2)
    elif tipo in [2, 8]:  # NxM o 2da unidad
        if pct_desc is not None and p_modal is not None:
            effective_pct = pct_desc / (cant_n if cant_n > 0 else 2.0)
            return round(p_modal * (1.0 - effective_pct), 2)

    return p_modal

def load_boton_ofertas_c065(**kwargs):
    """
    Consulta promociones vigentes de Canal 70 desde ecommdata.workflow_promociones,
    filtra SKUs con mejores ofertas exclusivas y actualiza la tabla ecommdata.boton_ofertas_c065.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    query = """
    WITH c70_skus AS (
        SELECT DISTINCT s.vtex_id
        FROM ecommdata.workflow_promociones wp
        JOIN ecommdata.skus s ON s.ref_id::text = (
            (wp.material::text || '-'::text) || CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text
        )
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND wp.canal_distribucion = '70'
          AND s.vtex_id IS NOT NULL
          AND wp.id_mecanica <> ALL (ARRAY [124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59,50])
          AND wp.tipo_promocion <> 3
          AND wp.n_promocion NOT IN (
              5720882025, 5552152024, 4040162024, 5552792024, 5552852024, 
              4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 
              1120012025, 4000952026, 4000182025, 4000602026, 4000652026, 
              1120232025, 5551272026, 5510102026, 1020032026
          )
          AND wp.nombre_promocion::text !~~ '%ZONA%'::text
          AND wp.nombre_promocion::text !~~ '%MFC%'::text
          AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
          AND wp.nombre_promocion::text !~~ '%917%'::text
          AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
          AND wp.nombre_promocion::text !~~ '%LOC%'::text
          AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
          AND wp.nombre_promocion::text !~~ '%HUACHALALUME%'::text
    )
    SELECT 
        s.vtex_id,
        wp.n_promocion,
        wp.nombre_promocion,
        wp.canal_distribucion,
        wp.tipo_promocion,
        wp.precio_modal,
        wp.precio_promocional,
        wp.precio_total_promocional,
        wp.cantidad_n,
        wp.porcentaje_de_descuento
    FROM ecommdata.workflow_promociones wp
    JOIN ecommdata.skus s ON s.ref_id::text = (
        (wp.material::text || '-'::text) || CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text
    )
    JOIN c70_skus c ON c.vtex_id = s.vtex_id
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      AND wp.id_mecanica <> ALL (ARRAY [124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59,50])
      AND wp.tipo_promocion <> 3
      AND wp.n_promocion NOT IN (
          5720882025, 5552152024, 4040162024, 5552792024, 5552852024, 
          4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 
          1120012025, 4000952026, 4000182025, 4000602026, 4000652026, 
          1120232025, 5551272026, 5510102026, 1020032026
      )
      AND wp.nombre_promocion::text !~~ '%ZONA%'::text
      AND wp.nombre_promocion::text !~~ '%MFC%'::text
      AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
      AND wp.nombre_promocion::text !~~ '%917%'::text
      AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
      AND wp.nombre_promocion::text !~~ '%LOC%'::text
      AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
      AND wp.nombre_promocion::text !~~ '%HUACHALALUME%'::text;
    """

    records = pg_hook.get_records(query)
    print(f"📊 Registros de promociones vigentes encontrados con VTEX ID: {len(records)}")

    sku_promos = defaultdict(list)
    for r in records:
        try:
            vtex_id = str(int(float(r[0])))
        except (ValueError, TypeError):
            continue
        eff_price = calc_effective_unit_price(r)
        canal = str(r[3]).strip() if r[3] is not None else ""
        sku_promos[vtex_id].append({
            'n_prom': r[1],
            'name': r[2],
            'canal': canal,
            'eff_price': eff_price
        })

    c70_skus = set()
    excluded_skus = set()
    final_skus = set()

    for vtex_id, promos in sku_promos.items():
        c70_promos = [p for p in promos if p['canal'] == '70']
        other_promos = [p for p in promos if p['canal'] != '70']

        if c70_promos:
            c70_skus.add(vtex_id)
            c70_prices = [p['eff_price'] for p in c70_promos if p['eff_price'] is not None]
            min_c70_price = min(c70_prices) if c70_prices else None

            other_prices = [p['eff_price'] for p in other_promos if p['eff_price'] is not None]
            min_other_price = min(other_prices) if other_prices else None

            if min_c70_price is not None and min_other_price is not None and min_other_price < min_c70_price:
                excluded_skus.add(vtex_id)
            else:
                final_skus.add(vtex_id)

    print(f"🔎 SKUs vigentes asociados a promociones Canal 70: {len(c70_skus)}")
    print(f"🚫 SKUs excluidos por tener otra promo no exclusiva con mejor precio: {len(excluded_skus)}")
    print(f"✅ SKUs validados a incluir en Colección C065: {len(final_skus)}")

    skus_list = list(final_skus)
    if not skus_list:
        print("⚠️ No hay SKUs para guardar en PostgreSQL.")
        return

    # Guardar en ecommdata.boton_ofertas_c065
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS ecommdata.boton_ofertas_c065 (
        sku_id VARCHAR(50) PRIMARY KEY,
        fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    pg_hook.run(create_table_sql)
    pg_hook.run("TRUNCATE TABLE ecommdata.boton_ofertas_c065;")

    insert_sql = """
    INSERT INTO ecommdata.boton_ofertas_c065 (sku_id, fecha_carga)
    VALUES (%s, CURRENT_TIMESTAMP)
    ON CONFLICT (sku_id) DO UPDATE SET fecha_carga = EXCLUDED.fecha_carga;
    """
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    records_to_insert = [(s,) for s in skus_list]
    cur.executemany(insert_sql, records_to_insert)
    conn.commit()
    cur.close()
    conn.close()
    print(f"🗄️ Se guardaron exitosamente {len(skus_list)} SKUs en la tabla 'ecommdata.boton_ofertas_c065' en PostgreSQL.")

def sync_coleccion_c065_vtex(**kwargs):
    """Obtiene los SKUs de ecommdata.boton_ofertas_c065 y sincroniza la colección C065 en VTEX."""
    account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    environment = Variable.get("VTEX_ENV", default_var="vtexcommercestable")
    collection_id = int(Variable.get("COLECCION_C065_COLLECTION_ID", default_var=COLLECTION_ID))

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query = "SELECT DISTINCT sku_id FROM ecommdata.boton_ofertas_c065 WHERE sku_id IS NOT NULL AND TRIM(sku_id) <> '';"
    records = pg_hook.get_records(query)

    vtex_ids_nuevos = set([str(int(float(r[0]))) for r in records if r[0]])
    print(f"📦 SKUs únicos a cargar en colección C065 ({collection_id}): {len(vtex_ids_nuevos)}")

    if not vtex_ids_nuevos:
        print(f"⚠️ No se encontraron SKUs válidos en ecommdata.boton_ofertas_c065 para actualizar la colección {collection_id}.")
        return

    # 1. Obtener SKUs actuales de la colección en VTEX
    skus_actuales = get_collection_skus(collection_id, account_name, environment)
    print(f"🔎 SKUs actualmente en la colección {collection_id}: {len(skus_actuales)}")

    # 2. Excluir SKUs obsoletos que ya no estén vigentes
    skus_a_excluir = skus_actuales - vtex_ids_nuevos
    if skus_a_excluir:
        print(f"🧹 Excluyendo {len(skus_a_excluir)} SKUs obsoletos de la colección {collection_id}...")
        remove_skus_from_collection(list(skus_a_excluir), collection_id, account_name, environment)
    else:
        print(f"✨ No hay SKUs obsoletos a excluir en la colección {collection_id}.")

    # 3. Importar SKUs vigentes mediante importinsert
    print(f"🚀 Insertando {len(vtex_ids_nuevos)} SKUs vigentes en la colección {collection_id}...")
    load_collection(list(vtex_ids_nuevos), collection_id, account_name, environment)


with DAG(
    'etl_cargar_coleccion_c065',
    default_args=default_args,
    description="Carga de colección C065 con promociones de Canal 70 en ecommdata.boton_ofertas_c065 y sincronización con colección VTEX",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "promociones", "Unimarc", "vtex", "colecciones", "C065"],
    # on_success_callback=dag_success_slack,
    # on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de promociones vigentes de Canal 70 en la tabla ecommdata.boton_ofertas_c065 y actualización de la colección VTEX C065 (ID 8413).
    """

    t0 = PythonOperator(
        task_id="load_boton_ofertas_c065",
        python_callable=load_boton_ofertas_c065,
    )

    t1 = PythonOperator(
        task_id="sync_coleccion_c065_vtex",
        python_callable=sync_coleccion_c065_vtex,
    )

    t0 >> t1
