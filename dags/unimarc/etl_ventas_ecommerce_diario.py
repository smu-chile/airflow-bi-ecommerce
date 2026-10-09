from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime, timedelta
import pendulum
import io
import json
import numpy as np
import pandas as pd
import requests


def _upload_excel_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = "", thread_ts: str = None):
    """Sube un archivo a Slack usando la API v2 (files.getUploadURLExternal y files.completeUploadExternal)."""
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
    if thread_ts:
        complete_payload["thread_ts"] = thread_ts

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

#----------------------------------------------------------------------------------------------------------------------------

def _procesar_y_enviar_alerta_peso_canales(**kwargs):
    """
    ETL diario de auditoría y comparación de ventas E-commerce totales:
    Calcula la Venta Neta ($ sin IVA) y el peso monetario / transaccional por canal de distribución:
    - 70 (ecommerce)
    - 10 (cadena)
    - sin promocion
    agrupado por Línea SAP, con informe en Excel enviado a Slack.
    """
    ds_str = kwargs.get("ds")
    if ds_str:
        eval_date = pd.to_datetime(ds_str).strftime("%Y-%m-%d")
    else:
        eval_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📅 Evaluando ventas e-commerce para la fecha: {eval_date}")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    # Consulta SQL con cálculo exacto de venta neta y asignación de canal (70 ecommerce vs 10 cadena vs sin promocion)
    query_sales = f"""
    WITH item_promos AS (
      SELECT 
        op.id AS id_orden_producto,
        MAX(CASE WHEN wp.canal_distribucion = '70' THEN 1 ELSE 0 END) AS has_70,
        MAX(CASE WHEN wp.canal_distribucion = '10' THEN 1 ELSE 0 END) AS has_10
      FROM ecommdata.orden_productos op
      JOIN ecommdata.ordenes_janis oj ON oj.id = op.id_orden
      LEFT JOIN ecommdata.productos p ON op.producto_vtex_id = p.vtex_id 
      LEFT JOIN ecommdata.orden_producto_promociones promo ON op.id = promo.orden_producto 
      LEFT JOIN ecommdata.orden_producto_promocion_extrainfo promoextra ON promo.id = promoextra.orden_producto_promocion AND promoextra.campo IN ('ID', 'WORKFLOWID')
      LEFT JOIN ecommdata.workflow_promociones wp ON promoextra.valor::int8 = wp.n_promocion AND p.material = wp.material AND wp.id_evento NOT IN (102)
      WHERE oj.fecha_facturacion::date = '{eval_date}'::date
      GROUP BY op.id
    )
    SELECT 
      op.id AS id_orden_producto,
      oj.id AS id_orden,
      COALESCE(op.producto_vtex_id::text, op.sku_vtex_id::text, op.ref_id::text) AS vtex_id,
      op.ref_id,
      COALESCE(NULLIF(TRIM(op.descripcion), ''), p.nombre, op.ref_id::text) AS producto_nombre,
      COALESCE(NULLIF(TRIM(m.linea_sap), ''), 'Sin Línea') AS linea_sap,
      COALESCE(op.precio_venta, 0) AS precio_unitario,
      COALESCE(NULLIF(op.unidades_pickeadas, 0), op.unidades_solicitadas, 1) AS cantidad_unidades,
      CASE 
        WHEN ip.has_70 = 1 THEN '70 (ecommerce)'
        WHEN ip.has_10 = 1 THEN '10 (cadena)'
        ELSE 'sin promocion'
      END AS canal,
      CASE 
        WHEN split_part(op.ref_id, '-', 2) IN ('KG', 'KGV') AND op.unidades_pickeadas <> 0 
          THEN ROUND(COALESCE(opp.precio, op.precio_venta) / 1.19, 0)
        ELSE ROUND(LEAST(op.precio_venta * op.unidades_pickeadas, COALESCE(op2.precio_venta * op2.unidades_solicitadas, op.precio_venta * op.unidades_pickeadas)) / 1.19, 0)
      END AS venta_neta,
      oj.fecha_facturacion
    FROM ecommdata.ordenes_janis oj
    JOIN ecommdata.orden_productos op ON oj.id = op.id_orden
    JOIN item_promos ip ON op.id = ip.id_orden_producto
    LEFT JOIN ecommdata.orden_productos op2 ON op.id_producto_substituido = op2.id
    LEFT JOIN ecommdata.orden_producto_pesables opp ON op.id = opp.id_orden_producto
    LEFT JOIN (
      SELECT vtex_id, MAX(material) AS material, MAX(nombre) AS nombre 
      FROM ecommdata.productos 
      GROUP BY vtex_id
    ) p ON op.producto_vtex_id = p.vtex_id
    LEFT JOIN (
      SELECT material, MAX(linea_sap) AS linea_sap 
      FROM ecommdata.maestra_sku_proveedor 
      GROUP BY material
    ) m ON m.material = p.material
    WHERE oj.fecha_facturacion::date = '{eval_date}'::date
      AND oj.estado_janis BETWEEN 10 AND 90
      AND oj.id NOT IN (
        SELECT id_orden FROM ecommdata.orden_productos WHERE ref_id IN ('000000000000669484-UN', '000000000000669485-UN')
      );
    """

    print("⚡ Ejecutando consulta de ventas totales en Postgres...")
    df_raw = pg_hook.get_pandas_df(query_sales)
    print(f"📊 Filas crudas recuperadas de Postgres: {len(df_raw)}")

    if df_raw.empty:
        print(f"⚠️ No se registraron ventas e-commerce para la fecha {eval_date}.")
        return

    # Clasificación de montos por canal
    df_raw["monto_c70"] = np.where(df_raw["canal"] == "70 (ecommerce)", df_raw["venta_neta"], 0.0)
    df_raw["monto_c10"] = np.where(df_raw["canal"] == "10 (cadena)", df_raw["venta_neta"], 0.0)
    df_raw["monto_sin_promo"] = np.where(df_raw["canal"] == "sin promocion", df_raw["venta_neta"], 0.0)

    # Indicadores binarios por canal
    df_raw["is_c70"] = (df_raw["canal"] == "70 (ecommerce)").astype(int)
    df_raw["is_c10"] = (df_raw["canal"] == "10 (cadena)").astype(int)
    df_raw["is_sin_promo"] = (df_raw["canal"] == "sin promocion").astype(int)

    # 1. Totales Consolidados Globales
    monto_c70 = float(df_raw["monto_c70"].sum())
    monto_c10 = float(df_raw["monto_c10"].sum())
    monto_sin_promo = float(df_raw["monto_sin_promo"].sum())
    monto_total_general = float(df_raw["venta_neta"].sum())

    pct_c70 = (monto_c70 / monto_total_general * 100.0) if monto_total_general > 0 else 0.0
    pct_c10 = (monto_c10 / monto_total_general * 100.0) if monto_total_general > 0 else 0.0
    pct_sin_promo = (monto_sin_promo / monto_total_general * 100.0) if monto_total_general > 0 else 0.0

    items_c70 = int(df_raw["is_c70"].sum())
    items_c10 = int(df_raw["is_c10"].sum())
    items_sin_promo = int(df_raw["is_sin_promo"].sum())
    items_total = len(df_raw)

    pct_items_c70 = (items_c70 / items_total * 100.0) if items_total > 0 else 0.0
    pct_items_c10 = (items_c10 / items_total * 100.0) if items_total > 0 else 0.0
    pct_items_sin_promo = (items_sin_promo / items_total * 100.0) if items_total > 0 else 0.0

    print(f"📈 Venta Neta Total E-commerce: ${monto_total_general:,.0f} | 70 (ecommerce): ${monto_c70:,.0f} ({pct_c70:.2f}%) | 10 (cadena): ${monto_c10:,.0f} ({pct_c10:.2f}%) | Sin Promo: ${monto_sin_promo:,.0f} ({pct_sin_promo:.2f}%)")

    # 2. Desglose por Línea SAP
    df_lineas = df_raw.groupby("linea_sap", as_index=False).agg(
        monto_c70=("monto_c70", "sum"),
        monto_c10=("monto_c10", "sum"),
        monto_sin_promo=("monto_sin_promo", "sum"),
        monto_total=("venta_neta", "sum"),
        items_c70=("is_c70", "sum"),
        items_c10=("is_c10", "sum"),
        items_sin_promo=("is_sin_promo", "sum"),
        items_total=("id_orden_producto", "count")
    )

    df_lineas["pct_c70"] = (df_lineas["monto_c70"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas["pct_c10"] = (df_lineas["monto_c10"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas["pct_sin_promo"] = (df_lineas["monto_sin_promo"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas = df_lineas.sort_values(by="monto_total", ascending=False)

    # 3. Generar libro de Excel en memoria
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        # Pestaña 1: Resumen Global
        df_resumen_global = pd.DataFrame([
            {
                "Métrica": "Venta Neta ($)",
                "70 (ecommerce)": f"${monto_c70:,.0f}",
                "10 (cadena)": f"${monto_c10:,.0f}",
                "Sin Promoción": f"${monto_sin_promo:,.0f}",
                "Total General": f"${monto_total_general:,.0f}"
            },
            {
                "Métrica": "Peso Monetario (%)",
                "70 (ecommerce)": f"{pct_c70:.2f}%",
                "10 (cadena)": f"{pct_c10:.2f}%",
                "Sin Promoción": f"{pct_sin_promo:.2f}%",
                "Total General": "100.00%"
            },
            {
                "Métrica": "Cantidad Ítems Vendidos",
                "70 (ecommerce)": items_c70,
                "10 (cadena)": items_c10,
                "Sin Promoción": items_sin_promo,
                "Total General": items_total
            },
            {
                "Métrica": "Peso Transaccional (%)",
                "70 (ecommerce)": f"{pct_items_c70:.2f}%",
                "10 (cadena)": f"{pct_items_c10:.2f}%",
                "Sin Promoción": f"{pct_items_sin_promo:.2f}%",
                "Total General": "100.00%"
            }
        ])
        df_resumen_global.to_excel(writer, sheet_name="Resumen Global", index=False)

        # Pestaña 2: Venta por Línea SAP
        df_lineas_excel = df_lineas[[
            "linea_sap", "monto_c70", "monto_c10", "monto_sin_promo", "monto_total",
            "pct_c70", "pct_c10", "pct_sin_promo",
            "items_c70", "items_c10", "items_sin_promo", "items_total"
        ]].rename(columns={
            "linea_sap": "Línea SAP",
            "monto_c70": "Venta Neta ($) 70 (ecommerce)",
            "monto_c10": "Venta Neta ($) 10 (cadena)",
            "monto_sin_promo": "Venta Neta ($) Sin Promoción",
            "monto_total": "Total Venta Neta ($)",
            "pct_c70": "% Peso 70 (ecommerce) ($)",
            "pct_c10": "% Peso 10 (cadena) ($)",
            "pct_sin_promo": "% Peso Sin Promoción ($)",
            "items_c70": "Ítems 70 (ecommerce)",
            "items_c10": "Ítems 10 (cadena)",
            "items_sin_promo": "Ítems Sin Promoción",
            "items_total": "Total Ítems"
        })
        df_lineas_excel.to_excel(writer, sheet_name="Venta por Linea SAP", index=False)

        # Pestaña 3: Detalle Productos
        df_detalle_excel = df_raw[[
            "id_orden_producto", "id_orden", "vtex_id", "ref_id", 
            "producto_nombre", "linea_sap", "precio_unitario", "cantidad_unidades", 
            "venta_neta", "canal", "fecha_facturacion"
        ]].rename(columns={
            "id_orden_producto": "ID Ítem Orden",
            "id_orden": "ID Orden Janis",
            "vtex_id": "VTEX ID",
            "ref_id": "Ref ID",
            "producto_nombre": "Producto",
            "linea_sap": "Línea SAP",
            "precio_unitario": "Precio Unitario ($)",
            "cantidad_unidades": "Cantidad Unidades",
            "venta_neta": "Venta Neta ($)",
            "canal": "Canal",
            "fecha_facturacion": "Fecha Facturación"
        })
        df_detalle_excel.to_excel(writer, sheet_name="Detalle Productos", index=False)

    excel_bytes = excel_buffer.getvalue()
    file_name = f"informe_peso_canales_c70_vs_c10_{eval_date}.xlsx"

    # 4. Construcción de Notificación en Slack
    channel_var_name = "SLACK_PESO_CANALES_REGISTRY"
    channel_id = Variable.get(channel_var_name, default_var=None) or Variable.get("SLACK_PROMOTIONS_VIEWER_ALERT", default_var="C0BVBAHD7L0")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    top_lineas_text = ""
    if not df_lineas.empty:
        top_5 = df_lineas.head(5)
        lineas_list = []
        for _, row in top_5.iterrows():
            lineas_list.append(
                f"• *{row['linea_sap']}*: Total ${row['monto_total']:,.0f} | 70 (ecommerce): ${row['monto_c70']:,.0f} ({row['pct_c70']}%) | 10 (cadena): ${row['monto_c10']:,.0f} ({row['pct_c10']}%) | Sin Promo: ${row['monto_sin_promo']:,.0f} ({row['pct_sin_promo']}%)"
            )
        top_lineas_text = "\n".join(lineas_list)
    else:
        top_lineas_text = "_No se registraron ventas en la fecha evaluada._"

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "📊 Reporte Diario: Venta Neta por Canal de Distribución",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"<!channel> *Informe de Venta Neta E-commerce (Consolidado $ sin IVA)*\n"
                    f"• *Fecha Evaluada*: `{eval_date}`\n"
                    f"• 💵 *Venta Neta Total E-commerce*: `${monto_total_general:,.0f}` ({items_total:,} ítems)\n"
                    f"• 🟢 *70 (ecommerce)*: `${monto_c70:,.0f}` (`{pct_c70:.2f}%`)\n"
                    f"• 🔵 *10 (cadena)*: `${monto_c10:,.0f}` (`{pct_c10:.2f}%`)\n"
                    f"• ⚪ *Sin Promoción*: `${monto_sin_promo:,.0f}` (`{pct_sin_promo:.2f}%`)\n"
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📌 Top 5 Líneas SAP por Venta Neta ($):*\n{top_lineas_text}"
            }
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"📄 _Se adjunta en el hilo el informe completo en Excel con detalle a nivel de producto ({file_name})._"
                }
            ]
        }
    ]

    if slack_token and channel_id:
        headers = {
            "Authorization": f"Bearer {slack_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        payload = {
            "channel": channel_id,
            "blocks": slack_blocks,
            "text": f"Informe Comparación Ventas E-commerce ({eval_date}): Total Neta ${monto_total_general:,.0f} | 70: ${monto_c70:,.0f} ({pct_c70:.2f}%) | 10: ${monto_c10:,.0f} ({pct_c10:.2f}%) | Sin Promo: ${monto_sin_promo:,.0f} ({pct_sin_promo:.2f}%)"
        }
        res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
        res_json = res.json() if res.ok else {}
        thread_ts = None
        if res.ok and res_json.get("ok"):
            print(" Notificación Block Kit enviada a Slack.")
            thread_ts = res_json.get("ts")
        else:
            print(f"⚠️ Error al enviar bloques a Slack: {res.text}")

        try:
            _upload_excel_file_to_slack(
                file_name=file_name,
                data_bytes=excel_bytes,
                channel_id=channel_id,
                token=slack_token,
                initial_comment=f"📊 Adjunto informe consolidado de Venta Neta E-commerce (Canal 70 vs Canal 10 vs Sin Promoción) en Excel para la fecha {eval_date}.",
                thread_ts=thread_ts
            )
            print(" Reporte Excel enviado a Slack en el hilo de la alerta.")
        except Exception as e:
            print(f"⚠️ Error al adjuntar reporte Excel en Slack: {e}")
    else:
        print("⚠️ Token o canal de Slack no configurados. Omitiendo notificación.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_comparacion_ventas_ecommerce",
    default_args=default_args,
    description="ETL diario de auditoría y comparación de Venta Neta E-commerce (Canal 70 ecommerce, Canal 10 cadena y Sin Promoción) por Línea SAP.",
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ventas", "promociones", "canal70", "canal10", "linea_sap", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría y Comparación Diaria: Venta Neta E-commerce por Canal y Línea SAP
    Este DAG analiza la Venta Neta E-commerce ($ sin IVA) del día cerrado anterior (`fecha_facturacion`) registrada en `ecommdata.ordenes_janis`, clasificando los productos en:
    - **70 (ecommerce)**
    - **10 (cadena)**
    - **Sin Promoción**

    Calcula la venta neta acumulada ($ sin IVA), el peso monetario (%) y transaccional (%) por cada Línea SAP.
    Genera una alerta en Slack con adjunto Excel multi-pestaña ("Resumen Global", "Venta por Linea SAP", "Detalle Productos").
    """

    task_auditar_peso_canales = PythonOperator(
        task_id="auditar_peso_canales_promocionales",
        python_callable=_procesar_y_enviar_alerta_peso_canales,
        provide_context=True,
    )

