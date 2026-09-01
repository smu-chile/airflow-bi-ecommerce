from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime
import pendulum
import io
import json
import pandas as pd
import requests

def _send_slack_alert(**kwargs):
    print("🔍 Consultando resultados de la auditoría de stock Janis Top 500 en PostgreSQL...")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query_summary = """
    SELECT 
        ranking,
        ref_id_sku,
        material,
        umv,
        vtex_id,
        nombre_sku,
        id_tienda,
        nombre_tienda,
        stock_janis,
        es_quiebre_janis,
        tiene_promocion_activa,
        cantidad_promociones_activas,
        detalle_promociones_activas,
        promedio_venta_diaria_unidades_90d,
        promedio_venta_diaria_pesos_90d,
        fecha_modificacion_janis
    FROM ecommdata.alerta_top500_stock_janis
    WHERE fecha_ejecucion::date = CURRENT_DATE
    ORDER BY ranking ASC, tiene_promocion_activa DESC, promedio_venta_diaria_unidades_90d DESC;
    """
    
    try:
        df_raw = pg_hook.get_pandas_df(query_summary)
    except Exception as e:
        print(f"❌ Error al consultar ecommdata.alerta_top500_stock_janis: {e}")
        return

    if df_raw.empty:
        print("⚠️ No se encontraron datos para la ejecución actual.")
        return

    # Filtrar únicamente quiebres de stock (stock_janis <= 0 o NULL)
    df_quiebres = df_raw[df_raw["es_quiebre_janis"] == True].copy()

    total_evaluados = len(df_raw)
    skus_unicos_evaluados = df_raw["ref_id_sku"].nunique()
    total_quiebres = len(df_quiebres)
    quiebres_con_promo = len(df_quiebres[df_quiebres["tiene_promocion_activa"] == True])
    tiendas_evaluadas = df_raw["nombre_tienda"].unique().tolist()

    print("=== RESUMEN DE AUDITORÍA TOP 500 STOCK JANIS ===")
    print(f"• Total combinaciones SKU/Tienda evaluadas: {total_evaluados}")
    print(f"• SKUs únicos Top 500 evaluados: {skus_unicos_evaluados}")
    print(f"• Total de quiebres (Stock 0 en Janis): {total_quiebres}")
    print(f"• Quiebres con Promoción Activa hoy: {quiebres_con_promo}")

    channel_var_name = "SLACK_TOP_500_ALERT"
    channel_id = Variable.get(channel_var_name, default_var=None)
    if not channel_id:
        print(f"⚠️ Variable de Airflow '{channel_var_name}' no configurada. Omitiendo alerta en Slack.")
        return

    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    # Top 10 casos más críticos (Priorizando Los Trapenses 0917, Promoción activa + alta venta diaria e-commerce)
    df_quiebres["es_trapenses"] = df_quiebres["id_tienda"].astype(str).str.zfill(4).eq("0917")

    top_criticos = df_quiebres.sort_values(
        by=["es_trapenses", "tiene_promocion_activa", "promedio_venta_diaria_unidades_90d", "ranking"],
        ascending=[False, False, False, True]
    ).head(10)

    texto_intro = (
        f"<!channel> *Alerta Diaria Top 500 Stock Janis (8:00 AM)*\n"
        f"Se han evaluado las tiendas: *{', '.join(tiendas_evaluadas)}*.\n"
        f"• *Total SKUs Top 500 evaluados*: {skus_unicos_evaluados}\n"
        f"• *Quiebres Detectados (Stock Janis = 0)*: *{total_quiebres}*\n"
        f"• *Quiebres con Promoción Activa HOY*: 🚨 *{quiebres_con_promo}*"
    )

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Alerta Top 500: Quiebres de Stock Janis & Promociones",
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
                "text": "*📌 Top 10 Quiebres Más Críticos (Promoción Activa / Mayor Venta E-Commerce 90d):*"
            }
        }
    ]

    for _, row in top_criticos.iterrows():
        promo_str = f"🏷️ *Promo Activa:* {row['detalle_promociones_activas']}" if row['tiene_promocion_activa'] else "⚪ *Sin Promo Activa*"
        stock_val = int(row['stock_janis']) if pd.notnull(row['stock_janis']) else 0
        vta_unid = row['promedio_venta_diaria_unidades_90d']
        vta_pesos = row['promedio_venta_diaria_pesos_90d']

        item_text = (
            f"🛍️ *{row['nombre_sku']}* (Ref: `{row['ref_id_sku']}` | Rank #{row['ranking']})\n"
            f"• *Tienda:* {row['nombre_tienda']} (ID: `{row['id_tienda']}`)\n"
            f"• *Stock Janis:* `{stock_val}` unidades\n"
            f"• *Venta Diaria E-Commerce (90d):* `{vta_unid}` un/día _(${int(vta_pesos):,} /día)_\n"
            f"• {promo_str}"
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
                "text": "💡 _Consulta `ecommdata.alerta_top500_stock_janis` en PostgreSQL para ver el detalle de todos los quiebres._"
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
            "text": f"Alerta Top 500 Janis: {total_quiebres} quiebres detectados ({quiebres_con_promo} con promoción activa)."
        }
        res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
        if not res.ok or not res.json().get("ok"):
            print(f"⚠️ Error al enviar mensaje Block Kit a Slack: {res.text}")
        else:
            print(" Notificación de alerta Block Kit enviada a Slack.")
    else:
        print("⚠️ No se encontró token de Slack para enviar mensaje Block Kit. Se procederá con la subida del CSV.")

    # Adjuntar archivo CSV con el detalle completo de quiebres de la jornada
    csv_buffer = io.BytesIO()
    df_quiebres.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue()

    file_name = f"quiebres_top500_janis_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    if slack_token:
        try:
            _upload_csv_file_to_slack(
                file_name=file_name,
                data_bytes=csv_bytes,
                channel_id=channel_id,
                token=slack_token,
                initial_comment=f"📄 Adjunto reporte completo con los {total_quiebres} quiebres de stock Janis detectados en el Top 500."
            )
            print(" Reporte CSV de quiebres enviado a Slack.")
        except Exception as e:
            print(f"⚠️ No se pudo adjuntar el CSV en Slack: {e}")
    else:
        print("⚠️ No se encontró token de Slack para adjuntar el CSV.")


def _upload_csv_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = ""):
    """
    Sube un archivo a Slack utilizando la API v2 de archivos.
    """
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

    up_resp = requests.post(
        upload_url,
        data=data_bytes,
        headers={"Content-Type": "application/octet-stream"},
    )
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo bytes de {file_name}: {up_resp.text}")

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
    "etl_alerta_top500_stock_janis",
    default_args=default_args,
    description="Auditoría de quiebres de stock Janis en el Top 500 de productos, promociones activas y ventas 90d para tiendas 0581 y 0917 a las 8:00 AM.",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "Janis", "stock", "quiebres", "ranking", "promociones", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría Diaria Top 500 Stock Janis y Promociones Activas
    Este DAG se ejecuta **todos los días a las 8:00 AM (hora Chile)**.
    1. Evalúa el Top 500 de productos en las tiendas **0581 (Mirador)** y **0917 (Los Trapenses)**.
    2. Consulta el stock Janis desde `ecommdata.stock` e identifica quiebres (`stock_janis = 0`).
    3. Verifica si existe promoción activa en `ecommdata.workflow_promociones`.
    4. Calcula la venta promedio diaria e-commerce de los últimos 90 días por producto y tienda desde `ecommdata.ventas_ecommerce_datawarehouse`.
    5. Guarda el resultado en `ecommdata.alerta_top500_stock_janis` y notifica a Slack (Block Kit + reporte CSV).
    """

    task_auditar_top500 = PostgresOperator(
        task_id="auditar_top500_stock_janis",
        postgres_conn_id="postgresql_conn",
        sql="sql/alerta_top500_stock_janis.sql",
    )

    task_notificar_slack = PythonOperator(
        task_id="send_slack_alert_top500",
        python_callable=_send_slack_alert,
    )

    task_auditar_top500 >> task_notificar_slack
