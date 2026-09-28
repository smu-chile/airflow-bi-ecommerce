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

# from utils.slack_utils import dag_success_slack, dag_failure_slack

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

# Mapeo de Colecciones por Día con sus n_promociones asignadas y sufijo para task_id
COLECCIONES_DIA = {
    10422: {
        "nombre": "Lunes",
        "task_suffix": "lunes",
        "n_promociones": {1120032024, 1120012024, 1120022024},
    },
    10426: {
        "nombre": "Martes",
        "task_suffix": "martes",
        "n_promociones": {1120062024, 1120042024, 1120052024},
    },
    10427: {
        "nombre": "Miércoles",
        "task_suffix": "miercoles",
        "n_promociones": {1120092024, 1120082024},
    },
    10428: {
        "nombre": "Viernes",
        "task_suffix": "viernes",
        "n_promociones": {1120062025, 1120052025, 1120012025, 1120222025, 1120262025, 1120092025, 1120082025, 1120022025, 1120232025, 1120272025},
    },
    10429: {
        "nombre": "Sábado",
        "task_suffix": "sabado",
        "n_promociones": {1120142025, 1120042025, 1120162025, 1120152025, 1120032025, 1120132025, 1120112025, 1120122025},
    },
}

# Todos los n_promocion válidos (unión de todas las colecciones)
ALL_MEMBERSHIP_PROMOS = set().union(*[v["n_promociones"] for v in COLECCIONES_DIA.values()])


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




def load_boton_ofertas_mem(**kwargs):
    """
    Consulta promociones vigentes de Membresía (filtradas por Lista 8) desde ecommdata.workflow_promociones,
    filtra SKUs con mejores ofertas exclusivas por colección y actualiza la tabla ecommdata.boton_ofertas_mem
    segmentando por coleccion_id (colección VTEX de destino por día).
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    all_n_promociones_str = ', '.join(str(n) for n in sorted(ALL_MEMBERSHIP_PROMOS))

    query = f"""
    WITH mem_materials AS (
        SELECT DISTINCT wp.material
        FROM ecommdata.workflow_promociones wp
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND wp.id_mecanica <> ALL (ARRAY [124,36, 67, 72, 99, 37, 51, 93, 53, 96, 77, 59, 50])
          AND wp.tipo_promocion <> 3
          AND wp.n_promocion IN ({all_n_promociones_str})
          AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
          AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
          AND wp.nombre_promocion::text NOT ILIKE '%UNIPAY%'
          AND wp.nombre_promocion::text NOT ILIKE '%917%'
          AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
          AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
          AND wp.nombre_promocion::text !~* 'L(0[0-9]{{2}}|[1-9][0-9]{{0,2}})'
          AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
    )
    SELECT DISTINCT
        s.vtex_id,
        wp.n_promocion
    FROM ecommdata.workflow_promociones wp
    JOIN mem_materials mm ON wp.material = mm.material
    JOIN ecommdata.productos p ON p.material = wp.material
    JOIN ecommdata.skus s ON s.ref_id = p.ref_id
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      AND s.vtex_id IS NOT NULL
      AND s.vtex_id::text <> ''
      AND s.vtex_id::text ~ '^[0-9]+$'
      AND wp.id_mecanica <> ALL (ARRAY [124,36, 67, 72, 99, 37, 51, 93, 53, 96, 77, 59, 50])
      AND wp.tipo_promocion <> 3
      AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
      AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
      AND wp.nombre_promocion::text NOT ILIKE '%UNIPAY%'
      AND wp.nombre_promocion::text NOT ILIKE '%917%'
      AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
      AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
      AND wp.nombre_promocion::text !~* 'L(0[0-9]{{2}}|[1-9][0-9]{{0,2}})'
      AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
    """

    records = pg_hook.get_records(query)
    print(f"📊 Registros de promociones vigentes encontrados con VTEX ID: {len(records)}")

    # Agrupar: vtex_id -> set de n_prom activos
    sku_n_proms = defaultdict(set)
    for r in records:
        try:
            vtex_id = str(int(float(r[0])))
        except (ValueError, TypeError):
            continue
        sku_n_proms[vtex_id].add(r[1])

    print(f"🔎 Total de SKUs únicos con promociones membresía: {len(sku_n_proms)}")

    # Para cada colección, incluir todos los SKUs que tengan al menos un n_prom asignado a ese día
    coleccion_skus = {}  # coleccion_id -> set de vtex_id
    for coleccion_id, config in COLECCIONES_DIA.items():
        n_prom_col = config["n_promociones"]
        nombre_dia = config["nombre"]

        final_skus = {
            vtex_id
            for vtex_id, n_proms in sku_n_proms.items()
            if n_proms & n_prom_col  # intersección: al menos 1 n_prom de esta colección
        }

        coleccion_skus[coleccion_id] = final_skus
        print(f"📅 Colección {coleccion_id} ({nombre_dia}): {len(final_skus)} SKUs finales")

    # Recrear tabla con schema actualizado (incluye coleccion_id en la PK)
    drop_create_sql = """
    DROP TABLE IF EXISTS ecommdata.boton_ofertas_mem;
    CREATE TABLE ecommdata.boton_ofertas_mem (
        sku_id       VARCHAR(50)  NOT NULL,
        coleccion_id INTEGER      NOT NULL,
        fecha_carga  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (sku_id, coleccion_id)
    );
    """
    pg_hook.run(drop_create_sql)

    # Insertar SKUs por colección en un solo batch
    insert_sql = """
    INSERT INTO ecommdata.boton_ofertas_mem (sku_id, coleccion_id, fecha_carga)
    VALUES (%s, %s, CURRENT_TIMESTAMP)
    ON CONFLICT (sku_id, coleccion_id) DO UPDATE SET fecha_carga = EXCLUDED.fecha_carga;
    """
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    total_inserted = 0

    for coleccion_id, skus in coleccion_skus.items():
        nombre_dia = COLECCIONES_DIA[coleccion_id]["nombre"]
        if skus:
            records_to_insert = [(s, coleccion_id) for s in skus]
            cur.executemany(insert_sql, records_to_insert)
            total_inserted += len(records_to_insert)
            print(f"   ✅ {len(records_to_insert)} SKUs insertados para colección {coleccion_id} ({nombre_dia})")
        else:
            print(f"   ⚠️ Sin SKUs para colección {coleccion_id} ({nombre_dia})")

    conn.commit()
    cur.close()
    conn.close()
    print(f"🗄️ Total de registros guardados en 'ecommdata.boton_ofertas_mem': {total_inserted}")


def sync_coleccion_vtex(coleccion_id, **kwargs):
    """
    Obtiene los SKUs de ecommdata.boton_ofertas_mem para la colección indicada
    y sincroniza con la colección VTEX correspondiente (excluye obsoletos e inserta vigentes).
    """
    account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
    environment = Variable.get("VTEX_ENV", default_var="vtexcommercestable")
    nombre_dia = COLECCIONES_DIA[coleccion_id]["nombre"]

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    query = """
    SELECT DISTINCT sku_id
    FROM ecommdata.boton_ofertas_mem
    WHERE coleccion_id = %s
      AND sku_id IS NOT NULL
      AND TRIM(sku_id) <> '';
    """
    records = pg_hook.get_records(query, parameters=(coleccion_id,))

    vtex_ids_nuevos = {str(int(float(r[0]))) for r in records if r[0]}
    print(f"📦 [{nombre_dia}] SKUs únicos a cargar en Colección {coleccion_id}: {len(vtex_ids_nuevos)}")

    if not vtex_ids_nuevos:
        print(f"⚠️ [{nombre_dia}] No se encontraron SKUs válidos para la colección {coleccion_id}.")
        return

    # 1. Obtener SKUs actuales de la colección en VTEX
    skus_actuales = get_collection_skus(coleccion_id, account_name, environment)
    print(f"🔎 [{nombre_dia}] SKUs actualmente en colección {coleccion_id}: {len(skus_actuales)}")

    # 2. Excluir SKUs obsoletos que ya no estén vigentes
    skus_a_excluir = skus_actuales - vtex_ids_nuevos
    if skus_a_excluir:
        print(f"🧹 [{nombre_dia}] Excluyendo {len(skus_a_excluir)} SKUs obsoletos de colección {coleccion_id}...")
        remove_skus_from_collection(list(skus_a_excluir), coleccion_id, account_name, environment)
    else:
        print(f"✨ [{nombre_dia}] No hay SKUs obsoletos en colección {coleccion_id}.")

    # 3. Importar SKUs vigentes mediante importinsert
    print(f"🚀 [{nombre_dia}] Insertando {len(vtex_ids_nuevos)} SKUs vigentes en colección {coleccion_id}...")
    load_collection(list(vtex_ids_nuevos), coleccion_id, account_name, environment)


with DAG(
    'etl_cargar_coleccion_membresia',
    default_args=default_args,
    description=(
        "Carga de Colecciones Membresía por día: "
        "Lunes=10422, Martes=10426, Miércoles=10427, Viernes=10428, Sábado=10429. "
        "Fuente: promociones vigentes de Canal 70 en ecommdata.workflow_promociones."
    ),
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "promociones", "Unimarc", "vtex", "colecciones", "membresia"],
    # on_success_callback=dag_success_slack,
    # on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Carga de promociones vigentes de Canal 70 de membresía (oro, diamante, platino) en la tabla
    `ecommdata.boton_ofertas_mem` (con columna `coleccion_id`) y actualización de 5 colecciones VTEX por día:

    | Collection ID | Día       | n_promociones                                                                                                                                 |
    |---------------|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------|
    | 10422         | Lunes     | 1120032024, 1120012024, 1120022024                                                                                                            |
    | 10426         | Martes    | 1120062024, 1120042024, 1120052024                                                                                                            |
    | 10427         | Miércoles | 1120092024, 1120082024                                                                                                                        |
    | 10428         | Viernes   | 1120062025, 1120052025, 1120012025, 1120222025, 1120262025, 1120092025, 1120082025, 1120022025, 1120232025, 1120272025, 1120112025, 1120122025 |
    | 10429         | Sábado    | 1120142025, 1120042025, 1120162025, 1120152025, 1120032025, 1120132025, 1120112025, 1120122025                                                |

    Flujo:
    1. `load_boton_ofertas_mem`: Extrae SKUs vigentes y los guarda en PostgreSQL segmentados por colección.
    2. `sync_coleccion_*` (x5 en paralelo): Sincroniza cada colección VTEX de forma independiente.
    """

    t_load = PythonOperator(
        task_id="load_boton_ofertas_mem",
        python_callable=load_boton_ofertas_mem,
    )

    sync_tasks = []
    for col_id, col_config in COLECCIONES_DIA.items():
        t_sync = PythonOperator(
            task_id=f"sync_coleccion_{col_config['task_suffix']}",
            python_callable=sync_coleccion_vtex,
            op_kwargs={"coleccion_id": col_id},
        )
        sync_tasks.append(t_sync)

    t_load >> sync_tasks
