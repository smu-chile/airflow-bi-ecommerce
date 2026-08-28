from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime, timedelta
import pendulum
import requests
import time
import logging
import re

# ---------------------------------------------------------------------------
# Helpers reutilizados del ecosistema de DAGs de bundles
# ---------------------------------------------------------------------------

def query_to_df(query):
    import pandas as pd
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()
    return results

def get_vtex_headers():
    return {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": Variable.get("X_VTEX_API_AppKey"),
        "X-VTEX-API-AppToken": Variable.get("X_VTEX_API_AppToken")
    }

def retry_request(method, url, max_retries=8, **kwargs):
    backoff_factor = 2
    for i in range(max_retries):
        try:
            if method == 'GET':
                resp = requests.get(url, **kwargs)
            elif method == 'POST':
                resp = requests.post(url, **kwargs)
            elif method == 'PUT':
                resp = requests.put(url, **kwargs)
            elif method == 'DELETE':
                resp = requests.delete(url, **kwargs)
            else:
                raise ValueError("Method not supported")

            if resp.status_code in [200, 201, 204]:
                return resp
            else:
                logging.warning(f"VTEX API returned {resp.status_code} for {url}. Text: {resp.text[:300]}")
                if resp.status_code in [400, 404]:
                    return resp
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")

        if i < max_retries - 1:
            sleep_time = backoff_factor ** (i + 1)
            logging.info(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)

    raise Exception(f"Max retries exceeded for {method} {url}")

def get_base_price(vtex_id):
    """Obtiene el basePrice de un SKU desde la API de Pricing de VTEX."""
    url = f"https://api.vtex.com/unimarc/pricing/prices/{vtex_id}"
    resp = retry_request('GET', url, headers=get_vtex_headers(), timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        return data.get("basePrice")
    else:
        raise Exception(f"Failed to get basePrice for vtex_id={vtex_id}. Status: {resp.status_code}")

# ---------------------------------------------------------------------------
# Tarea principal: detectar promos y cargar precios fijos en VTEX
# ---------------------------------------------------------------------------

def cargar_promociones_precio_fijo_bundles():
    """
    Detecta promociones de precio fijo (tipo_promocion=4) vigentes HOY para los
    SKUs de bebida que tienen un bundle retornable activo, calcula el precio del
    bundle (precio_promo_bebida + precio_envase) y lo inserta como precio fijo
    en la lista de precio correspondiente en VTEX.

    NO crea promociones en VTEX. Solo inserta el precio en la price table
    mediante POST /pricing/prices/{sku}/fixed/{tradePolicy}.
    """
    import pandas as pd

    # ------------------------------------------------------------------
    # 1. Obtener promociones de precio fijo vigentes para SKUs retornables
    # ------------------------------------------------------------------
    query_promos = """
        SELECT
            wp.n_promocion,
            wp.nombre_promocion,
            wp.tipo_promocion,
            wp.precio_promocional,
            wp.precio_modal,
            wp.umv,
            s.multiplicador_unidad_medida,
            wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion,
            (wp.material::text || '-'::text) || CASE
                WHEN wp.umv::text = 'ST' THEN 'UN'
                WHEN wp.umv::text = 'CS' THEN 'CJ'
                ELSE wp.umv
            END AS ref_id_original,
            s.vtex_id AS vtex_id_original,
            b.sku_bundle,
            s_bund.vtex_id AS vtex_id_bundle
        FROM ecommdata.workflow_promociones wp
        INNER JOIN ecommdata.skus s ON s.ref_id = (
            (wp.material::text || '-'::text) || CASE
                WHEN wp.umv::text = 'ST' THEN 'UN'
                WHEN wp.umv::text = 'CS' THEN 'CJ'
                ELSE wp.umv
            END
        )
        INNER JOIN ecommdata.sku_bundles_retornables b
            ON b.sku_original = s.ref_id AND b.active = true
        INNER JOIN ecommdata.skus s_bund ON s_bund.ref_id = b.sku_bundle
        LEFT JOIN ecommdata.lista8 l8
            ON ((l8.material::text || '-'::text) || l8.umv::text) = (
                (wp.material::text || '-'::text) || CASE
                    WHEN wp.umv::text = 'ST' THEN 'UN'
                    WHEN wp.umv::text = 'CS' THEN 'CJ'
                    ELSE wp.umv
                END
            )
        LEFT JOIN ecommdata.stock_mfc sm
            ON (LPAD(sm.material::text, 18, '0') || '-' || sm.unidad_venta::text) = (
                (wp.material::text || '-' || CASE
                    WHEN wp.umv::text = 'ST' THEN 'UN'
                    WHEN wp.umv::text = 'CS' THEN 'CJ'
                    ELSE wp.umv
                END)::text
            )
        WHERE
            wp.tipo_promocion = 4
            AND wp.fecha_inicio_de_promocion <= current_date
            AND wp.fecha_fin_de_promocion >= current_date
            AND wp.id_mecanica <> ALL (ARRAY[124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50])
            AND wp.tipo_promocion <> 3
            AND wp.nombre_promocion::text !~~ '%ZONA%'
            AND wp.nombre_promocion::text !~~ '%MFC%'
            AND wp.nombre_promocion::text !~~ '%UNIPAY%'
            AND wp.nombre_promocion::text !~~ '%917%'
            AND wp.nombre_promocion::text !~~ '%ESTADO%'
            AND wp.nombre_promocion::text !~~ '%LOC%'
            AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
            AND wp.nombre_promocion::text !~~ '%HUACHALALUME%'
            AND wp.nombre_promocion::text !~~ '%LOCAL%'
            AND wp.nombre_promocion::text !~~ '%MEMB%'
            AND wp.nombre_promocion::text !~~ '%CYBER%'
            AND wp.nombre_promocion::text !~~ '%CUMPLEANOS%'
            AND wp.nombre_promocion::text !~~ '%BLACK%'
            AND s.vtex_id IS NOT NULL
            AND s_bund.vtex_id IS NOT NULL
            AND (
                ((l8.material::text || '-'::text) || l8.umv::text) IS NOT NULL
                OR sm.stock >= 1
            )
        GROUP BY
            wp.n_promocion, wp.nombre_promocion, wp.tipo_promocion,
            wp.precio_promocional, wp.precio_modal, wp.umv,
            s.multiplicador_unidad_medida, wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion, wp.material, s.ref_id, s.vtex_id,
            b.sku_bundle, s_bund.vtex_id
        ORDER BY wp.precio_promocional ASC
    """

    df_promos = query_to_df(query_promos)

    if df_promos.empty:
        logging.info("No se encontraron promociones de precio fijo vigentes para bundles retornables.")
        return

    logging.info(f"Se encontraron {len(df_promos)} registros de promociones precio fijo para bundles retornables.")

    # ------------------------------------------------------------------
    # 2. Procesar cada promo: calcular precio y cargar en VTEX
    # ------------------------------------------------------------------
    headers = get_vtex_headers()
    envase_price_cache = {}  # Cache para no repetir llamadas API por el mismo envase

    contadores = {"ok": 0, "error": 0}
    log_detalle = []

    for _, row in df_promos.iterrows():
        vtex_id_original = str(int(row['vtex_id_original']))
        vtex_id_bundle   = str(int(row['vtex_id_bundle']))
        n_promocion      = row['n_promocion']
        nombre_promocion = row['nombre_promocion']
        precio_promo_raw = float(row['precio_promocional'])
        precio_modal     = float(row['precio_modal'])
        umv              = str(row['umv']).strip().upper()
        multiplicador    = float(row['multiplicador_unidad_medida']) if row['multiplicador_unidad_medida'] else 1.0
        fecha_inicio     = str(row['fecha_inicio_de_promocion'])[:10]
        fecha_fin        = str(row['fecha_fin_de_promocion'])[:10]

        # Nombre limpio de la trade policy (igual que carga_promociones.py)
        trade_policy_name = re.sub(r'[^a-zA-Z0-9]', '', nombre_promocion)

        logging.info(
            f"[PROCESANDO] Promo {n_promocion} ('{nombre_promocion}') → "
            f"Bundle vtex_id={vtex_id_bundle} (bebida original={vtex_id_original}) | "
            f"Trade Policy='{trade_policy_name}'"
        )

        try:
            # Ajuste KG/KGV: multiplicar por el factor de unidad de medida
            if umv in ('KG', 'KGV'):
                precio_promo_bebida = round(precio_promo_raw * multiplicador, 0)
                precio_modal_bebida = round(precio_modal * multiplicador, 0)
                logging.info(
                    f"[PROMO {n_promocion}] SKU pesable ({umv}): "
                    f"precio_promo_raw={precio_promo_raw} x multiplicador={multiplicador} "
                    f"= {precio_promo_bebida}"
                )
            else:
                precio_promo_bebida = precio_promo_raw
                precio_modal_bebida = precio_modal

            # ----------------------------------------------------------
            # 3. Obtener componentes del kit para identificar el envase
            # ----------------------------------------------------------
            kit_url = f"https://unimarc.myvtex.com/api/catalog/pvt/stockkeepingunitkit?parentSkuId={vtex_id_bundle}"
            resp_kit = retry_request('GET', kit_url, headers=headers, timeout=30)

            if resp_kit.status_code != 200:
                raise Exception(
                    f"No se pudieron obtener los componentes del kit para bundle={vtex_id_bundle}. "
                    f"Status={resp_kit.status_code}"
                )

            components = resp_kit.json()
            if len(components) != 2:
                logging.error(
                    f"[PROMO {n_promocion}] ALERTA: el bundle {vtex_id_bundle} tiene "
                    f"{len(components)} componente(s) en lugar de 2. Omitiendo."
                )
                contadores["error"] += 1
                continue

            # Identificar el envase (componente distinto a la bebida original)
            vtex_id_envase = None
            for comp in components:
                if str(comp['StockKeepingUnitId']) != vtex_id_original:
                    vtex_id_envase = str(comp['StockKeepingUnitId'])
                    break

            if not vtex_id_envase:
                raise Exception(
                    f"No se pudo identificar el envase en el bundle {vtex_id_bundle}. "
                    f"Componentes: {[c['StockKeepingUnitId'] for c in components]}"
                )

            # ----------------------------------------------------------
            # 4. Obtener basePrice del envase (con caché)
            # ----------------------------------------------------------
            if vtex_id_envase not in envase_price_cache:
                envase_base_price = get_base_price(vtex_id_envase)
                if envase_base_price is None:
                    raise Exception(
                        f"El envase vtex_id={vtex_id_envase} no tiene basePrice en VTEX."
                    )
                envase_price_cache[vtex_id_envase] = float(envase_base_price)
                logging.info(f"[Cache] Precio base del envase vtex_id={vtex_id_envase}: ${envase_price_cache[vtex_id_envase]}")

            envase_price = envase_price_cache[vtex_id_envase]

            # ----------------------------------------------------------
            # 5. Calcular precio promocional del bundle
            # ----------------------------------------------------------
            precio_promo_bundle = int(round(precio_promo_bebida + envase_price, 0))
            precio_modal_bundle = int(round(precio_modal_bebida + envase_price, 0))

            logging.info(
                f"[PROMO {n_promocion}] Calculo: "
                f"bebida_promo=${precio_promo_bebida} + envase=${envase_price} "
                f"= ${precio_promo_bundle} | listPrice del bundle=${precio_modal_bundle}"
            )

            # ----------------------------------------------------------
            # 6. Insertar precio fijo en la lista de precio de VTEX
            #    NOTA: Esto NO crea una promocion VTEX. Solo inserta el
            #    precio en la price table.
            # ----------------------------------------------------------
            zona_horaria = "T00:00:00-04:00"
            url_fixed = (
                f"https://api.vtex.com/unimarc/pricing/prices"
                f"/{vtex_id_bundle}/fixed/{trade_policy_name}"
            )
            payload_fixed = [{
                "value": precio_promo_bundle,
                "listPrice": precio_modal_bundle,
                "minQuantity": 1,
                "dateRange": {
                    "from": f"{fecha_inicio}{zona_horaria}",
                    "to":   f"{fecha_fin}{zona_horaria}"
                }
            }]

            resp_fixed = retry_request(
                'POST', url_fixed,
                json=payload_fixed,
                headers=headers,
                timeout=30
            )

            if resp_fixed.status_code in [200, 201, 204]:
                msg = (
                    f"[OK] Promo {n_promocion} ('{nombre_promocion}') "
                    f"→ Bundle vtex_id={vtex_id_bundle} "
                    f"→ Lista '{trade_policy_name}' "
                    f"→ Precio=${precio_promo_bundle} "
                    f"(bebida_promo=${precio_promo_bebida} + envase=${envase_price}) "
                    f"| listPrice=${precio_modal_bundle} "
                    f"| Vigencia: {fecha_inicio} → {fecha_fin}"
                )
                logging.info(msg)
                log_detalle.append({"estado": "OK", "detalle": msg})
                contadores["ok"] += 1
            else:
                msg = (
                    f"[ERROR HTTP] Promo {n_promocion} → Bundle {vtex_id_bundle} "
                    f"→ Lista '{trade_policy_name}' "
                    f"→ HTTP {resp_fixed.status_code}: {resp_fixed.text[:300]}"
                )
                logging.error(msg)
                log_detalle.append({"estado": "ERROR", "detalle": msg})
                contadores["error"] += 1

        except Exception as e:
            msg = (
                f"[EXCEPCION] Promo {n_promocion} → Bundle {vtex_id_bundle}: {str(e)}"
            )
            logging.error(msg)
            log_detalle.append({"estado": "EXCEPCION", "detalle": msg})
            contadores["error"] += 1
            continue  # Error aislado: continua con el siguiente bundle

    # ------------------------------------------------------------------
    # 7. Resumen final en logs
    # ------------------------------------------------------------------
    logging.info(
        f"\n{'='*60}\n"
        f"RESUMEN — ETL Promociones Bundles Retornables\n"
        f"  OK  (precio insertado) : {contadores['ok']}\n"
        f"  ERR (error/excepcion)  : {contadores['error']}\n"
        f"  TOTAL procesados       : {contadores['ok'] + contadores['error']}\n"
        f"{'='*60}"
    )
    for item in log_detalle:
        logging.info(f"  [{item['estado']}] {item['detalle']}")


# ---------------------------------------------------------------------------
# Definicion del DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'etl_promociones_bundles_retornables',
    default_args=default_args,
    description=(
        "Detecta promociones de precio fijo vigentes para SKUs de bebidas retornables "
        "y las inserta como precio fijo en la lista de precio de VTEX. "
        "NO crea promociones VTEX. Solo actualiza el precio en la price table."
    ),
    schedule_interval="0 0,12 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "VTEX", "ecommdata", "precios", "bundles", "promociones"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    **ETL Promociones Bundles Retornables**

    A las 00:00 (medianoche) y 12:00 (mediodia) hora Chile, este DAG:
    1. Detecta promociones de **precio fijo** (`tipo_promocion = 4`) vigentes HOY
       para los SKUs de bebida con bundle retornable activo en `ecommdata.sku_bundles_retornables`.
    2. Aplica los mismos filtros de seleccion que `Carga Promos / promotions_query.sql`.
    3. Calcula el precio del bundle: `precio_promo_bebida + basePrice_envase (VTEX)`.
       Para SKUs KG/KGV aplica el multiplicador de unidad antes de sumar el envase.
    4. Inserta el precio en la lista de precio de VTEX via
       `POST /pricing/prices/{vtex_id_bundle}/fixed/{trade_policy_name}`.

    **IMPORTANTE:** Este DAG NO crea promociones en VTEX.
    Solo inserta el precio en la price table para que la promocion ya existente
    tome el valor correcto al activarse.

    Los logs detallan para cada bundle: promocion cargada, trade policy, precio
    insertado y su descomposicion (bebida_promo + envase).
    """

    cargar_promos_task = PythonOperator(
        task_id="cargar_promociones_precio_fijo_bundles",
        python_callable=cargar_promociones_precio_fijo_bundles,
    )
