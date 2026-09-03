from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime
import pendulum
import io
import os
import json
import pandas as pd
import requests

def _send_slack_alert(**kwargs):
    print("🔍 Consultando discrepancias de promociones Canal 10 / Canal 70 en PostgreSQL...")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "alerta_promociones_canal10.sql")
    if os.path.exists(sql_file_path):
        with open(sql_file_path, "r", encoding="utf-8") as f:
            query = f.read()
    else:
        query = """
        WITH cargas AS (
            SELECT DISTINCT fecha_carga
            FROM ecommdata.promotions_viewer_simple
            ORDER BY fecha_carga DESC
            LIMIT 2
        ),
        carga_actual AS (
            SELECT fecha_carga FROM cargas LIMIT 1
        ),
        carga_previa AS (
            SELECT fecha_carga FROM cargas OFFSET 1 LIMIT 1
        ),
        prev_c10 AS (
            SELECT 
                sku_id,
                sku_name,
                workflow_id AS workflow_id_c10,
                promotion_name AS promotion_name_c10,
                begin_date AS begin_date_c10,
                end_date AS end_date_c10,
                fecha_carga
            FROM ecommdata.promotions_viewer_simple
            WHERE fecha_carga = (SELECT fecha_carga FROM carga_previa)
              AND (promotional_tag IN ('C011', '10') OR promotion_name ILIKE '%C10%' OR promotion_name ILIKE '%CANAL10%')
              AND end_date > CURRENT_TIMESTAMP
        ),
        prev_c70 AS (
            SELECT 
                sku_id,
                workflow_id AS workflow_id_c70,
                promotion_name AS promotion_name_c70,
                begin_date AS begin_date_c70,
                end_date AS end_date_c70
            FROM ecommdata.promotions_viewer_simple
            WHERE fecha_carga = (SELECT fecha_carga FROM carga_previa)
              AND (promotional_tag IN ('C065', '70') OR promotion_name ILIKE '%C70%' OR promotion_name ILIKE '%CANAL70%')
        ),
        curr_c10 AS (
            SELECT DISTINCT sku_id
            FROM ecommdata.promotions_viewer_simple
            WHERE fecha_carga = (SELECT fecha_carga FROM carga_actual)
              AND (promotional_tag IN ('C011', '10') OR promotion_name ILIKE '%C10%' OR promotion_name ILIKE '%CANAL10%')
        )
        SELECT 
            p10.sku_id,
            p10.sku_name,
            p10.workflow_id_c10,
            p10.promotion_name_c10,
            p10.begin_date_c10,
            p10.end_date_c10,
            p70.workflow_id_c70,
            p70.promotion_name_c70,
            p10.fecha_carga AS fecha_carga_anterior,
            (SELECT fecha_carga FROM carga_actual) AS fecha_carga_actual
        FROM prev_c10 p10
        JOIN prev_c70 p70 ON p10.sku_id = p70.sku_id
        LEFT JOIN curr_c10 c10 ON p10.sku_id = c10.sku_id
        WHERE c10.sku_id IS NULL
        ORDER BY p10.end_date_c10 ASC;
        """
    
    try:
        df_alertas = pg_hook.get_pandas_df(query)
    except Exception as e:
        print(f"❌ Error al consultar la auditoría de promociones: {e}")
        return

    if df_alertas.empty:
        print("✅ No se detectaron productos con pérdida prematura de promoción Canal 10.")
        return

    total_afectados = len(df_alertas)
    skus_unicos = df_alertas["sku_id"].nunique()

    print(f"=== ALERTA DE PROMOCIONES CANAL 10 / CANAL 70 ===")
    print(f"• Total productos en riesgo/afectados: {total_afectados}")
    print(f"• SKUs únicos: {skus_unicos}")

    channel_var_name = "SLACK_PROMOTIONS_VIEWER_ALERT"
    channel_id = Variable.get(channel_var_name, default_var="C0BVBAHD7L0")
    
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    top_criticos = df_alertas.head(10)

    texto_intro = (
        f"<!channel> *Alerta de Pérdida Prematura Promo Canal 10*\n"
        f"Se han detectado productos con promo simultánea (Canal 10 y Canal 70) que *perdieron la promo Canal 10 antes de su término*.\n"
        f"• *Total SKUs Afectados*: 🚨 *{skus_unicos}*"
    )

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Alerta Promociones: Pérdida Prematura Canal 10",
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
                "text": "*📌 Top 10 Casos Detectados:*"
            }
        }
    ]

    for _, row in top_criticos.iterrows():
        item_text = (
            f"🛍️ *{row['sku_name']}* (SKU: `{row['sku_id']}`)\n"
            f"• *Workflow C10 Perdido:* `{row['workflow_id_c10']}` ({row['promotion_name_c10']})\n"
            f"• *Vencimiento Programado C10:* `{row['end_date_c10']}`\n"
            f"• *Workflow C70 Vigente:* `{row['workflow_id_c70']}` ({row['promotion_name_c70']})"
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
                "text": "💡 _Consulta la vista `ecommdata.promotions_viewer_simple` en PostgreSQL para más detalles._"
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
            "text": f"Alerta Promociones: {skus_unicos} SKUs perdieron promo Canal 10 prematuramente."
        }
        res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
        if not res.ok or not res.json().get("ok"):
            print(f"⚠️ Error al enviar mensaje a Slack: {res.text}")
        else:
            print(" Notificación de alerta enviada a Slack.")
    else:
        print("⚠️ No se encontró token de Slack. Omitiendo mensaje en Slack.")

    # Adjuntar archivo CSV con el detalle completo de SKUs afectados
    csv_buffer = io.BytesIO()
    df_alertas.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue()

    file_name = f"perdida_promociones_c10_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    if slack_token:
        try:
            _upload_csv_file_to_slack(
                file_name=file_name,
                data_bytes=csv_bytes,
                channel_id=channel_id,
                token=slack_token,
                initial_comment=f"📄 Adjunto reporte completo de los {skus_unicos} SKUs que perdieron la promoción Canal 10."
            )
            print(" Reporte CSV de promociones enviado a Slack.")
        except Exception as e:
            print(f"⚠️ No se pudo adjuntar el CSV en Slack: {e}")


def _upload_csv_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = ""):
    """Sube un archivo a Slack utilizando la API v2 de archivos."""
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
    "etl_alerta_promociones_canal10",
    default_args=default_args,
    description="Auditoría de pérdida prematura de promociones Canal 10 en productos con promociones en Canal 70.",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "promociones", "canal10", "canal70", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría de Pérdida Prematura de Promociones Canal 10
    Este DAG se ejecuta todos los días para verificar si algún producto que contaba simultáneamente con promoción en Canal 10 y Canal 70 perdió la promoción de Canal 10 antes de su término.
    """

    task_auditar_y_alertar = PythonOperator(
        task_id="auditar_promociones_c10_c70",
        python_callable=_send_slack_alert,
    )
