from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack, send_text_message, upload_bytes_to_slack
from datetime import datetime
import pendulum
import io
import json
import pandas as pd
import requests
import sqlalchemy
from sqlalchemy import text

# Consulta SQL embebida corregida para obtener el PRECIO MODAL de las promociones en workflow_promociones
QUERY_COMPARACION_PRECIOS = """
WITH TiendasSeleccionadas AS (
    SELECT 
        id, 
        id_janis, 
        COALESCE(nombre_tienda, nombre_tienda_janis, glosa, id::text) AS nombre_tienda
    FROM ecommdata.tiendas
    WHERE status = 1
      AND (
             id::text IN ('0034', '0581', '0917', '0469')
          OR LPAD(id::text, 4, '0') IN ('0034', '0581', '0917', '0469')
      )
),
PromocionesVigentes AS (
    SELECT 
        wp.material,
        CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' ELSE wp.umv END AS umv,
        COUNT(DISTINCT wp.n_promocion) AS cantidad_promociones_vigentes,
        STRING_AGG(DISTINCT (wp.n_promocion::text || ': ' || wp.nombre_promocion || ' (Modal: $' || COALESCE(wp.precio_modal::int::text, 'N/A') || ')'), ' | ') AS detalle_promociones_vigentes,
        MIN(wp.precio_modal) AS min_precio_modal
    FROM ecommdata.workflow_promociones wp
    LEFT JOIN ecommdata.skus s ON s.ref_id::text = (
        (wp.material::text || '-'::text) || CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text
    )
    WHERE CURRENT_DATE BETWEEN wp.fecha_inicio_de_promocion AND wp.fecha_fin_de_promocion
      AND (wp.id_mecanica IS NULL OR wp.id_mecanica <> ALL (ARRAY [124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50]))
      AND wp.tipo_promocion <> 3
      AND wp.n_promocion NOT IN (
          5720882025, 5552152024, 4040162024, 5552792024, 5552852024, 
          4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 
          1120012025, 4000952026, 4000182025, 4000602026, 4000652026, 
          1120232025, 5551272026, 5510102026, 1020032026
      )
      AND wp.nombre_promocion::text NOT ILIKE '%%ZONA%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%MFC%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%UNIPAY%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%917%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%ESTADO%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%LOC%%'
      AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
      AND wp.nombre_promocion::text NOT ILIKE '%%HUACHALALUME%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%LOCAL%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%MEMB%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%REGIONAL%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%CYBER%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%CUMPLEANOS%%'
      AND wp.nombre_promocion::text NOT ILIKE '%%BLACK%%'
      AND (s.vtex_id IS NULL OR s.vtex_id <> ALL (ARRAY [3610, 82183, 82184, 39730]))
    GROUP BY 1, 2
)
SELECT 
    l.material,
    l.umv,
    (l.material || '-' || l.umv) AS ref_id,
    COALESCE(s.nombre_sku, p.nombre, l.material) AS nombre_sku,
    t.id AS id_tienda,
    t.nombre_tienda,
    pr.precio,
    pr.precio_lista,
    pr.costo,
    pr.fecha_carga,
    CASE WHEN promo.cantidad_promociones_vigentes > 0 THEN TRUE ELSE FALSE END AS tiene_promocion_vigente,
    COALESCE(promo.cantidad_promociones_vigentes, 0) AS cantidad_promociones_vigentes,
    promo.detalle_promociones_vigentes,
    promo.min_precio_modal
FROM ecommdata.lista8 l
JOIN TiendasSeleccionadas t 
    ON l.id_tienda = t.id 
LEFT JOIN ecommdata.precios pr 
    ON t.id_janis = pr.id_tienda_janis 
    AND (l.material || '-' || l.umv) = pr.ref_id
LEFT JOIN ecommdata.skus s 
    ON (l.material || '-' || l.umv) = s.ref_id
LEFT JOIN ecommdata.productos p
    ON (l.material || '-' || l.umv) = p.ref_id
LEFT JOIN PromocionesVigentes promo
    ON l.material = promo.material
    AND l.umv = promo.umv
WHERE l.excluido IS NOT TRUE
  AND l.bloq_formato IS NULL
  AND l.bloq_centro IS NULL
ORDER BY l.material, t.id;
"""

def _comparar_precios_lista8(ts, **kwargs):
    print("🔍 Iniciando comparación de precios de Lista 8 y precio modal de workflow_promociones...")

    # 1. Conectar a PostgreSQL (usando la conexión estándar 'postgresql_conn' de Airflow)
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        engine = pg_hook.get_sqlalchemy_engine()
        print("Ejecutando consulta SQL en PostgreSQL mediante PostgresHook...")
        df_raw = pg_hook.get_pandas_df(QUERY_COMPARACION_PRECIOS)
    except Exception as e:
        print(f"⚠️ PostgresHook no disponible o falló ({e}). Usando Airflow Variables como respaldo...")
        host = Variable.get("POSTGRESQL_HOST", default_var="postgresql")
        database = Variable.get("POSTGRESQL_DB", default_var="airflow")
        username = Variable.get("POSTGRESQL_USER", default_var="airflow")
        password = Variable.get("POSTGRESQL_PASSWORD", default_var="airflow")
        conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
        engine = sqlalchemy.create_engine(conn_url)
        df_raw = pd.read_sql(QUERY_COMPARACION_PRECIOS, engine)
    print(f"Total de registros obtenidos: {len(df_raw)}")

    if df_raw.empty:
        print("No se encontraron registros de Lista 8 para las tiendas indicadas.")
        return

    # Normalizar id_tienda como string
    df_raw["id_tienda"] = df_raw["id_tienda"].astype(str)

    # 2. Pivotear datos para comparar precios entre tiendas por SKU
    index_cols = [
        "material", 
        "umv", 
        "ref_id", 
        "nombre_sku", 
        "tiene_promocion_vigente", 
        "cantidad_promociones_vigentes", 
        "detalle_promociones_vigentes", 
        "min_precio_modal"
    ]
    df_pivot = df_raw.pivot(index=index_cols, columns="id_tienda", values="precio").reset_index()

    # Obtener columnas de tiendas presentes en la respuesta
    present_stores = [c for c in df_pivot.columns if c not in index_cols]
    
    if len(present_stores) < 2:
        print(f"No se obtuvieron suficientes tiendas con datos para comparar ({present_stores}).")
        return

    # 3. Métricas de comparación
    df_pivot["min_precio"] = df_pivot[present_stores].min(axis=1)
    df_pivot["max_precio"] = df_pivot[present_stores].max(axis=1)
    df_pivot["diferencia_precio"] = df_pivot["max_precio"] - df_pivot["min_precio"]
    df_pivot["precios_distintos"] = df_pivot[present_stores].nunique(axis=1)
    df_pivot["nulos_count"] = df_pivot[present_stores].isnull().sum(axis=1)

    # Detección de diferencias (precios distintos en alguna tienda)
    is_different = df_pivot["precios_distintos"] > 1

    df_diferencias = df_pivot[is_different].copy()
    
    total_skus = len(df_pivot)
    count_diferencias = len(df_diferencias)
    count_coincidentes = total_skus - count_diferencias
    count_con_promocion = df_diferencias["tiene_promocion_vigente"].sum()

    print("=== RESUMEN DE AUDITORÍA DE PRECIOS Y PRECIO MODAL PROMOCIONAL ===")
    print(f"• Total SKUs evaluados: {total_skus}")
    print(f"• SKUs coincidentes: {count_coincidentes}")
    print(f"• SKUs con diferencias de precio: {count_diferencias}")
    print(f"• SKUs con diferencia Y promociones vigentes: {count_con_promocion}")

    # 4. Guardar resultados con diferencias en PostgreSQL (ecommdata.diferencias_precios_lista8)
    exec_timestamp = datetime.now()

    if not df_diferencias.empty:
        def build_precios_json(row):
            return json.dumps({s: int(row[s]) if pd.notnull(row[s]) else None for s in present_stores})

        df_diferencias["tiendas_evaluadas"] = ",".join(present_stores)
        df_diferencias["precios_por_tienda"] = df_diferencias.apply(build_precios_json, axis=1)
        df_diferencias["fecha_ejecucion"] = exec_timestamp

        cols_output = [
            "fecha_ejecucion",
            "material",
            "umv",
            "ref_id",
            "nombre_sku",
            "tiendas_evaluadas",
            "tiene_promocion_vigente",
            "cantidad_promociones_vigentes",
            "detalle_promociones_vigentes",
            "min_precio_modal",
            "min_precio",
            "max_precio",
            "diferencia_precio",
            "precios_por_tienda"
        ]

        df_save = df_diferencias[cols_output].copy()

        with engine.begin() as conn:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS ecommdata.diferencias_precios_lista8 (
                id SERIAL PRIMARY KEY,
                fecha_ejecucion TIMESTAMP,
                material VARCHAR(50),
                umv VARCHAR(10),
                ref_id VARCHAR(50),
                nombre_sku VARCHAR(255),
                tiendas_evaluadas VARCHAR(100),
                tiene_promocion_vigente BOOLEAN,
                cantidad_promociones_vigentes INT,
                detalle_promociones_vigentes TEXT,
                min_precio_modal NUMERIC,
                min_precio NUMERIC,
                max_precio NUMERIC,
                diferencia_precio NUMERIC,
                precios_por_tienda JSONB
            );
            """
            conn.execute(text(create_table_sql))

            # Asegurar la existencia de las columnas en PostgreSQL
            conn.execute(text("ALTER TABLE ecommdata.diferencias_precios_lista8 ADD COLUMN IF NOT EXISTS tiene_promocion_vigente BOOLEAN;"))
            conn.execute(text("ALTER TABLE ecommdata.diferencias_precios_lista8 ADD COLUMN IF NOT EXISTS cantidad_promociones_vigentes INT;"))
            conn.execute(text("ALTER TABLE ecommdata.diferencias_precios_lista8 ADD COLUMN IF NOT EXISTS detalle_promociones_vigentes TEXT;"))
            conn.execute(text("ALTER TABLE ecommdata.diferencias_precios_lista8 ADD COLUMN IF NOT EXISTS min_precio_modal NUMERIC;"))
            
            df_save.to_sql(
                name="diferencias_precios_lista8",
                con=conn,
                schema="ecommdata",
                if_exists="append",
                index=False,
                chunksize=5000,
                method="multi"
            )
        print(f"✅ Se registraron {count_diferencias} discrepancias en ecommdata.diferencias_precios_lista8")

    # 5. Envío de Alerta a Slack (usando la variable SLACK_PROMOTION_ALERT_CHANNEL y Block Kit)
    _send_slack_alert(
        df_diferencias=df_diferencias,
        total_skus=total_skus,
        count_coincidentes=count_coincidentes,
        count_diferencias=count_diferencias,
        count_con_promocion=count_con_promocion,
        present_stores=present_stores
    )


def _send_slack_alert(df_diferencias, total_skus, count_coincidentes, count_diferencias, count_con_promocion, present_stores):
    channel_var_name = "SLACK_PROMOTION_ALERT_CHANNEL"
    try:
        channel_id = Variable.get(channel_var_name, default_var=None)
        if not channel_id:
            print(f"⚠️ Variable de Airflow '{channel_var_name}' no configurada. Omitiendo notificación en Slack.")
            return

        slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

        # Filtrar únicamente los SKUs con diferencias Y promociones vigentes que NO sean UNIPAY
        if not df_diferencias.empty:
            has_promo = df_diferencias["tiene_promocion_vigente"] == True
            not_unipay = ~df_diferencias["detalle_promociones_vigentes"].fillna("").str.contains("UNIPAY", case=False)
            df_alert = df_diferencias[has_promo & not_unipay].copy()
        else:
            df_alert = pd.DataFrame()

        count_alertas = len(df_alert)

        if count_alertas == 0:
            print("✅ No se encontraron SKUs con discrepancias de precio y promociones activas. Omitiendo alerta en Slack.")
            return

        # Restringir a Top 5 para lectura clara en Slack
        top_cases = df_alert.sort_values(by="diferencia_precio", ascending=False).head(5)

        texto_intro = (
            f"<!channel> Se han detectado *{count_alertas} SKUs con discrepancias de precio Y PROMOCIÓN ACTIVA* "
            f"entre las tiendas evaluadas ({', '.join(present_stores)}).\n"
            f"• *Total SKUs evaluados*: {total_skus}\n"
            f"• *SKUs coincidentes*: {count_coincidentes}\n"
            f"• *Total SKUs con diferencia*: {count_diferencias}\n"
            f"• *SKUs con diferencia Y Promoción Vigente*: *{count_alertas}*"
        )

        slack_blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Alerta de Auditoría: Discrepancias de Precios con Promoción Activa",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": texto_intro
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*📌 Top 5 Mayores Diferencias con Promoción Activa:*"
                }
            }
        ]

        for _, row in top_cases.iterrows():
            promo_info = f"🏷️ *Promo (Modal):* {row['detalle_promociones_vigentes']}"
            min_modal_val = row.get("min_precio_modal")
            min_modal_str = f" _(Min Modal: ${int(min_modal_val)})_" if pd.notnull(min_modal_val) else ""
            precios_json = row.get("precios_por_tienda", "")

            item_text = (
                f"🛍️ *{row['nombre_sku']}* (Ref: `{row['ref_id']}`)\n"
                f"• *Diferencia:* `${int(row['diferencia_precio']):,}` _(Min: ${int(row['min_precio']):,} / Max: ${int(row['max_precio']):,})_\n"
                f"• *Precios por Tienda:* `{precios_json}`\n"
                f"• {promo_info}{min_modal_str}"
            )

            slack_blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": item_text
                }
            })

        slack_blocks.append({"type": "divider"})
        slack_blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "💡 _Consulta `ecommdata.diferencias_precios_lista8` en PostgreSQL para ver el detalle de todas las discrepancias._"
                }
            ]
        })

        if slack_token:
            headers = {
                "Authorization": f"Bearer {slack_token}",
                "Content-Type": "application/json; charset=utf-8"
            }
            payload = {
                "channel": channel_id,
                "blocks": slack_blocks,
                "text": f"Alerta Auditoría Precios Lista 8: {count_alertas} promociones activas con discrepancias detectadas."
            }
            res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
            if not res.ok or not res.json().get("ok"):
                print(f"⚠️ Error al enviar mensaje Block Kit a Slack: {res.text}")
            else:
                print(" Notificación de alerta Block Kit enviada a Slack.")
        else:
            print("⚠️ No se encontró token de Slack para enviar mensaje Block Kit. Se procederá con la subida del CSV.")

        # Adjuntar archivo CSV con el detalle de las alertas con promo activa
        csv_buffer = io.BytesIO()
        df_alert.to_csv(csv_buffer, index=False, encoding="utf-8")
        csv_bytes = csv_buffer.getvalue()

        file_name = f"diferencias_precios_promos_lista8_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

        if slack_token:
            _upload_csv_file_to_slack(
                file_name=file_name,
                data_bytes=csv_bytes,
                channel_id=channel_id,
                token=slack_token,
                initial_comment=f"📄 Adjunto reporte con los {count_alertas} SKUs que tienen discrepancias de precios y promociones activas."
            )
            print(" Reporte CSV enviado a Slack.")
        else:
            print("⚠️ No se encontró token de Slack para adjuntar el CSV.")

    except Exception as e:
        print(f"❌ No se pudo enviar la alerta a Slack: {e}")


def _upload_csv_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = ""):
    """
    Sube un archivo a Slack utilizando la API v2 de archivos con el token especificado.
    """
    # 1) Solicitar URL externa de subida
    upload_url_resp = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={
            "filename": file_name,
            "length": str(len(data_bytes)),
            "token": token,
        },
    ).json()
    
    upload_url = upload_url_resp.get("upload_url")
    file_id = upload_url_resp.get("file_id")
    if not upload_url:
        raise RuntimeError(f"Error en files.getUploadURLExternal: {upload_url_resp}")

    # 2) Subir bytes al endpoint devuelto
    up_resp = requests.post(
        upload_url,
        data=data_bytes,
        headers={"Content-Type": "application/octet-stream"},
    )
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo bytes de {file_name}: {up_resp.text}")

    # 3) Completar la subida externamente y publicar en el canal
    complete_payload = {
        "files": [{"id": file_id}],
        "channel_id": channel_id,
        "initial_comment": initial_comment,
    }
    comp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(complete_payload),
    ).json()

    if not comp.get("ok"):
        raise RuntimeError(f"Error en files.completeUploadExternal: {comp}")

    return comp

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "etl_comparacion_precios_lista8_tiendas",
    default_args=default_args,
    description="Compara precios de Lista 8 entre tiendas y obtiene el precio modal de workflow_promociones a las 8:00 AM.",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "Janis", "precios", "lista8", "workflow_promociones", "auditoria", "slack"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Comparación de Precios de Lista 8 y Precio Modal de Promociones entre Tiendas
    Este DAG se ejecuta **todos los días a las 8:00 AM (hora Chile)**.
    1. Compara los precios de productos de Lista 8 entre las tiendas seleccionadas.
    2. Obtiene el **precio modal** (`precio_modal`) de las promociones vigentes hoy en `workflow_promociones`.
    3. Almacena los resultados en `ecommdata.diferencias_precios_lista8` y notifica por Slack.
    """

    task_comparar_precios = PythonOperator(
        task_id="comparar_precios_lista8",
        python_callable=_comparar_precios_lista8,
    )
