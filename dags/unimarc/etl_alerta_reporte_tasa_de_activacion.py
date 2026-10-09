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

local_tz = pendulum.timezone("America/Santiago")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=local_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": dag_failure_slack,
}

# Exclusiones estándar de mecánicas
MECANICAS_EXCLUIDAS = [124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50]

# Promociones explícitamente excluidas
PROMOS_EXCLUIDAS = [
    5720882025,
    5552152024,
    4040162024,
    5552792024,
    5552852024,
    4060322024,
    5553242024,
    1120042025,
    1120032025,
    1120022025,
    1120012025,
    4000952026,  # agregada 11/08/2026 Promo excl quesos
    4000182025,
    4000602026,  # agregada 26/05/2026
    4000652026,  # agregada 09/06/2026 Promo dia del completo
    1120232025,
    5551272026,  # agregada 17/06/2026 Promo regional
    5510102026,  # agregada 22/07/2026 Prueba B2B
    1020032026,  # agregada 15/07/2026 Promo ripley
    1020052026,  # agregada 09/09/2026 Promo Banco Estado
]

# vtex_ids excluidos
VTEX_IDS_EXCLUIDOS = [3610, 82183, 82184, 39730]


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


def _consultar_metricas_ventas(pg_hook: PostgresHook, eval_date: str):
    """
    Ejecuta la consulta de ventas totales e-commerce para la fecha eval_date.
    Calcula Venta Neta ($ sin IVA) por canal (70, 10, sin promocion) y desglose por Línea SAP.
    """
    query_sales = f"""
    WITH item_promos AS (
      SELECT 
        op.id AS id_orden_producto,
        MAX(CASE WHEN wp.canal_distribucion = '70' THEN 1 ELSE 0 END) AS has_70,
        MAX(CASE WHEN wp.canal_distribucion = '10' THEN 1 ELSE 0 END) AS has_10,
        MAX(wp.material::text || '-' || CASE
            WHEN wp.umv::text = 'ST' THEN 'UN'
            WHEN wp.umv::text = 'CS' THEN 'CJ'
            ELSE wp.umv::text
        END) AS wp_ref_id,
        MAX(wp.descripcion_material) AS wp_descripcion_material
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
      COALESCE(ip.wp_ref_id, op.ref_id) AS wp_ref_id,
      COALESCE(NULLIF(TRIM(ip.wp_descripcion_material), ''), NULLIF(TRIM(op.descripcion), ''), p.nombre, op.ref_id::text) AS wp_descripcion_material,
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
    df_raw = pg_hook.get_pandas_df(query_sales)
    if df_raw.empty:
        return None, None, {}

    df_raw["monto_c70"] = np.where(df_raw["canal"] == "70 (ecommerce)", df_raw["venta_neta"], 0.0)
    df_raw["monto_c10"] = np.where(df_raw["canal"] == "10 (cadena)", df_raw["venta_neta"], 0.0)
    df_raw["monto_sin_promo"] = np.where(df_raw["canal"] == "sin promocion", df_raw["venta_neta"], 0.0)

    df_raw["is_c70"] = (df_raw["canal"] == "70 (ecommerce)").astype(int)
    df_raw["is_c10"] = (df_raw["canal"] == "10 (cadena)").astype(int)
    df_raw["is_sin_promo"] = (df_raw["canal"] == "sin promocion").astype(int)

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

    df_lineas = df_raw.groupby("linea_sap", as_index=False).agg(
        monto_c70=("monto_c70", "sum"),
        monto_c10=("monto_c10", "sum"),
        monto_sin_promo=("monto_sin_promo", "sum"),
        monto_total=("venta_neta", "sum"),
        items_c70=("is_c70", "sum"),
        items_c10=("is_c10", "sum"),
        items_sin_promo=("is_sin_promo", "sum"),
        items_total=("id_orden_producto", "count"),
    )

    df_lineas["pct_c70"] = (df_lineas["monto_c70"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas["pct_c10"] = (df_lineas["monto_c10"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas["pct_sin_promo"] = (df_lineas["monto_sin_promo"] / df_lineas["monto_total"] * 100.0).round(2)
    df_lineas = df_lineas.sort_values(by="monto_total", ascending=False)

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

    metricas_ventas = {
        "monto_c70": monto_c70,
        "monto_c10": monto_c10,
        "monto_sin_promo": monto_sin_promo,
        "monto_total_general": monto_total_general,
        "pct_c70": pct_c70,
        "pct_c10": pct_c10,
        "pct_sin_promo": pct_sin_promo,
        "items_c70": items_c70,
        "items_c10": items_c10,
        "items_sin_promo": items_sin_promo,
        "items_total": items_total,
        "pct_items_c70": pct_items_c70,
        "pct_items_c10": pct_items_c10,
        "pct_items_sin_promo": pct_items_sin_promo,
        "top_lineas_text": top_lineas_text,
    }

    return df_raw, df_lineas, metricas_ventas


def _consultar_metricas_canal(pg_hook: PostgresHook, canal: str, eval_date: str) -> pd.DataFrame:
    """
    Fotografía diaria: para el canal indicado ('70' o '10'), obtiene los SKUs con promo
    activa en eval_date y verifica cuáles registraron AL MENOS UNA venta ese mismo día.
    """
    mecanicas_str = ", ".join(str(m) for m in MECANICAS_EXCLUIDAS)
    promos_excl_str = ", ".join(str(p) for p in PROMOS_EXCLUIDAS)
    vtex_excl_str = ", ".join(str(v) for v in VTEX_IDS_EXCLUIDOS)

    query = f"""
    WITH promos_activas AS (
        SELECT DISTINCT
            wp.canal_distribucion,
            wp.n_promocion,
            wp.nombre_promocion,
            wp.material,
            wp.descripcion_material,
            (wp.material::text || '-' || CASE
                WHEN wp.umv::text = 'ST' THEN 'UN'
                WHEN wp.umv::text = 'CS' THEN 'CJ'
                ELSE wp.umv::text
            END) AS ref_id,
            s.vtex_id,
            wp.fecha_inicio_de_promocion::date AS fecha_inicio,
            wp.fecha_fin_de_promocion::date AS fecha_fin
        FROM ecommdata.workflow_promociones wp
        LEFT JOIN ecommdata.skus s ON s.ref_id::text = (
            wp.material::text || '-' || CASE
                WHEN wp.umv::text = 'ST' THEN 'UN'
                WHEN wp.umv::text = 'CS' THEN 'CJ'
                ELSE wp.umv::text
            END
        )
        LEFT JOIN ecommdata.resumen_promociones_activas rpa
            ON rpa.sku = (
                wp.material::text || '-' || CASE
                    WHEN wp.umv::text = 'ST' THEN 'UN'
                    WHEN wp.umv::text = 'CS' THEN 'CJ'
                    ELSE wp.umv::text
                END
            )
            AND rpa.n_promocion = wp.n_promocion
        WHERE (
            wp.id_mecanica <> ALL (ARRAY [{mecanicas_str}])
        )
          AND wp.fecha_inicio_de_promocion::date <= '{eval_date}'::date
          AND wp.fecha_fin_de_promocion::date >= '{eval_date}'::date
          AND wp.tipo_promocion <> 3
          AND wp.n_promocion NOT IN ({promos_excl_str})
          AND wp.canal_distribucion = '{canal}'
          AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
          AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
          AND wp.nombre_promocion::text NOT ILIKE '%UNIPAY%'
          AND wp.nombre_promocion::text NOT ILIKE '%917%'
          AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
          AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
          AND wp.nombre_promocion::text NOT ILIKE 'L(0[0-9]{{2}}|[1-9][0-9]{{0,2}})'
          AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
          AND wp.nombre_promocion::text NOT ILIKE '%LOCAL%'
          AND wp.nombre_promocion::text NOT ILIKE '%MEMB%'
          AND wp.nombre_promocion::text NOT ILIKE '%REGIO%'
          AND wp.nombre_promocion::text NOT ILIKE '%BCO%'
          AND wp.nombre_promocion::text NOT ILIKE '%CUMPLEANOS%'
          AND wp.nombre_promocion::text NOT ILIKE '%BLACK%'
          AND s.vtex_id <> ALL (ARRAY [{vtex_excl_str}])
          AND COALESCE(rpa.porcentaje_descuento_final, 0) <= 75
    ),
    ventas_del_dia AS (
        SELECT DISTINCT p.ref_id
        FROM promos_activas p
        JOIN ecommdata.orden_productos op ON (
            op.ref_id = p.ref_id
            OR (p.vtex_id IS NOT NULL AND op.producto_vtex_id = p.vtex_id)
        )
        JOIN ecommdata.ordenes_janis oj ON oj.id = op.id_orden
        WHERE oj.estado_janis BETWEEN 10 AND 90
          AND oj.fecha_facturacion::date = '{eval_date}'::date
          AND COALESCE(NULLIF(op.unidades_pickeadas, 0), op.unidades_solicitadas, 0) > 0
    )
    SELECT
        p.canal_distribucion,
        p.ref_id,
        p.material,
        p.descripcion_material,
        COALESCE(p.vtex_id::text, 'Sin VTEX ID') AS vtex_id,
        p.n_promocion,
        p.nombre_promocion,
        p.fecha_inicio,
        p.fecha_fin,
        CASE WHEN v.ref_id IS NOT NULL THEN 1 ELSE 0 END AS con_venta
    FROM promos_activas p
    LEFT JOIN ventas_del_dia v ON p.ref_id = v.ref_id
    ORDER BY con_venta ASC, p.ref_id ASC;
    """
    df = pg_hook.get_pandas_df(query)
    return df


def _calcular_metricas(df: pd.DataFrame, canal: str, eval_date: str) -> dict:
    """Calcula métricas de conversión a partir del DataFrame de un canal dado."""
    total_skus = df["ref_id"].nunique()
    skus_con_venta = df[df["con_venta"] == 1]["ref_id"].nunique()
    skus_sin_venta = total_skus - skus_con_venta
    pct_conversion = round((skus_con_venta / total_skus * 100), 2) if total_skus > 0 else 0.0

    print(f"=== FOTOGRAFÍA DÍA ANTERIOR ({eval_date}) — CANAL {canal} ===")
    print(f"• Total SKUs en promo Canal {canal} (vigentes el {eval_date}): {total_skus}")
    print(f"• SKUs con venta el {eval_date}: {skus_con_venta}")
    print(f"• SKUs sin venta el {eval_date}: {skus_sin_venta}")
    print(f"• Tasa de Conversión: {pct_conversion}%")

    return {
        "canal": canal,
        "total_skus": total_skus,
        "skus_con_venta": skus_con_venta,
        "skus_sin_venta": skus_sin_venta,
        "pct_conversion": pct_conversion,
    }


def _calcular_y_notificar_alerta_consolidada(**kwargs):
    ds_str = kwargs.get("ds")
    if ds_str:
        eval_date = (pd.to_datetime(ds_str) - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        eval_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"🔍 Ejecutando Alerta Consolidada de Ventas y Activación para la fecha: {eval_date}")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    # ---------------------------------------------------------
    # 1. Procesar Ventas E-Commerce Totales
    # ---------------------------------------------------------
    df_raw_sales, df_lineas, mv = _consultar_metricas_ventas(pg_hook, eval_date)

    # ---------------------------------------------------------
    # 2. Procesar Activación de Promociones por Canal
    # ---------------------------------------------------------
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ecommdata.alerta_conversion_promos_canal (
        id SERIAL PRIMARY KEY,
        fecha_evaluacion DATE NOT NULL,
        canal VARCHAR(10) NOT NULL,
        total_skus_promo INT NOT NULL,
        skus_con_venta INT NOT NULL,
        skus_sin_venta INT NOT NULL,
        porcentaje_conversion NUMERIC(5,2) NOT NULL,
        fecha_registro TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        pg_hook.run(create_table_query)
        print("✅ Tabla ecommdata.alerta_conversion_promos_canal verificada/creada.")
    except Exception as e:
        print(f"⚠️ Error al verificar/crear tabla histórica: {e}")

    resultados_activacion = {}
    dfs_activacion = {}
    omitir_slack = False

    for canal in ["70", "10"]:
        try:
            df_act = _consultar_metricas_canal(pg_hook, canal, eval_date)
            if df_act.empty:
                print(f"⚠️ No se encontraron SKUs en promo activa para Canal {canal} en {eval_date}. Se omite.")
                omitir_slack = True
                continue
            metricas_act = _calcular_metricas(df_act, canal, eval_date)
            resultados_activacion[canal] = metricas_act
            dfs_activacion[canal] = df_act

            # Insertar registro histórico
            insert_query = f"""
            INSERT INTO ecommdata.alerta_conversion_promos_canal (
                fecha_evaluacion, canal, total_skus_promo, skus_con_venta, skus_sin_venta,
                porcentaje_conversion, fecha_registro
            ) VALUES (
                '{eval_date}'::date, '{canal}', {metricas_act['total_skus']}, {metricas_act['skus_con_venta']},
                {metricas_act['skus_sin_venta']}, {metricas_act['pct_conversion']}, CURRENT_TIMESTAMP
            );
            """
            pg_hook.run(insert_query)
            print(f"✅ Histórico Canal {canal} registrado en Postgres.")
        except Exception as e:
            print(f"❌ Error al procesar Activación Canal {canal}: {e}")

    # ---------------------------------------------------------
    # 3. Generar Único Libro Excel Consolidado en Memoria
    # ---------------------------------------------------------
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        # Pestaña 1: Resumen Global Ventas
        if mv:
            df_resumen_global = pd.DataFrame([
                {
                    "Métrica": "Venta Neta ($)",
                    "70 (ecommerce)": f"${mv['monto_c70']:,.0f}",
                    "10 (cadena)": f"${mv['monto_c10']:,.0f}",
                    "Sin Promoción": f"${mv['monto_sin_promo']:,.0f}",
                    "Total General": f"${mv['monto_total_general']:,.0f}"
                },
                {
                    "Métrica": "Peso Monetario (%)",
                    "70 (ecommerce)": f"{mv['pct_c70']:.2f}%",
                    "10 (cadena)": f"{mv['pct_c10']:.2f}%",
                    "Sin Promoción": f"{mv['pct_sin_promo']:.2f}%",
                    "Total General": "100.00%"
                },
                {
                    "Métrica": "Cantidad Ítems Vendidos",
                    "70 (ecommerce)": mv['items_c70'],
                    "10 (cadena)": mv['items_c10'],
                    "Sin Promoción": mv['items_sin_promo'],
                    "Total General": mv['items_total']
                },
                {
                    "Métrica": "Peso Transaccional (%)",
                    "70 (ecommerce)": f"{mv['pct_items_c70']:.2f}%",
                    "10 (cadena)": f"{mv['pct_items_c10']:.2f}%",
                    "Sin Promoción": f"{mv['pct_items_sin_promo']:.2f}%",
                    "Total General": "100.00%"
                }
            ])
            df_resumen_global.to_excel(writer, sheet_name="Resumen Global Ventas", index=False)

        # Pestaña 2: Venta por Línea SAP
        if df_lineas is not None and not df_lineas.empty:
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

        # Pestaña 3: Detalle Ventas Productos
        if df_raw_sales is not None and not df_raw_sales.empty:
            df_detalle_excel = df_raw_sales[[
                "id_orden_producto", "id_orden", "vtex_id", "ref_id", "wp_ref_id",
                "wp_descripcion_material", "producto_nombre", "linea_sap", "precio_unitario", 
                "cantidad_unidades", "venta_neta", "canal", "fecha_facturacion"
            ]].rename(columns={
                "id_orden_producto": "ID Ítem Orden",
                "id_orden": "ID Orden Janis",
                "vtex_id": "VTEX ID",
                "ref_id": "Ref ID Orden",
                "wp_ref_id": "Ref ID WP",
                "wp_descripcion_material": "Descripción Material WP",
                "producto_nombre": "Producto",
                "linea_sap": "Línea SAP",
                "precio_unitario": "Precio Unitario ($)",
                "cantidad_unidades": "Cantidad Unidades",
                "venta_neta": "Venta Neta ($)",
                "canal": "Canal",
                "fecha_facturacion": "Fecha Facturación"
            })
            df_detalle_excel.to_excel(writer, sheet_name="Detalle Ventas Productos", index=False)

        # Pestaña 4: Activación Canal 70
        if "70" in dfs_activacion and not dfs_activacion["70"].empty:
            df_act70_excel = dfs_activacion["70"][[
                "canal_distribucion", "ref_id", "material", "descripcion_material",
                "vtex_id", "n_promocion", "nombre_promocion", "fecha_inicio",
                "fecha_fin", "con_venta"
            ]].rename(columns={
                "canal_distribucion": "Canal Distribución",
                "ref_id": "Ref ID",
                "material": "Material",
                "descripcion_material": "Descripción Material",
                "vtex_id": "VTEX ID",
                "n_promocion": "N° Promoción",
                "nombre_promocion": "Nombre Promoción",
                "fecha_inicio": "Fecha Inicio",
                "fecha_fin": "Fecha Fin",
                "con_venta": "Con Venta (1/0)",
            })
            df_act70_excel.to_excel(writer, sheet_name="Activacion Canal 70", index=False)

        # Pestaña 5: Activación Canal 10
        if "10" in dfs_activacion and not dfs_activacion["10"].empty:
            df_act10_excel = dfs_activacion["10"][[
                "canal_distribucion", "ref_id", "material", "descripcion_material",
                "vtex_id", "n_promocion", "nombre_promocion", "fecha_inicio",
                "fecha_fin", "con_venta"
            ]].rename(columns={
                "canal_distribucion": "Canal Distribución",
                "ref_id": "Ref ID",
                "material": "Material",
                "descripcion_material": "Descripción Material",
                "vtex_id": "VTEX ID",
                "n_promocion": "N° Promoción",
                "nombre_promocion": "Nombre Promoción",
                "fecha_inicio": "Fecha Inicio",
                "fecha_fin": "Fecha Fin",
                "con_venta": "Con Venta (1/0)",
            })
            df_act10_excel.to_excel(writer, sheet_name="Activacion Canal 10", index=False)

    excel_bytes = excel_buffer.getvalue()
    file_name = f"informe_consolidado_ventas_y_activacion_{eval_date}.xlsx"

    # ---------------------------------------------------------
    # 4. Construir Mensaje en Slack (Estructura requerida)
    # ---------------------------------------------------------
    if omitir_slack:
        print("⚠️ No se cargará el reporte en Slack debido a que no se encontraron SKUs en promo activa en al menos un canal.")
        return

    channel_var_name = "SLACK_PESO_CANALES_REGISTRY"
    channel_id = Variable.get(channel_var_name, default_var="C0BVBAHD7L0")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    slack_blocks = []

    # Bloque 1: Reporte Diario: Venta Neta por Canal de Distribución
    slack_blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "📊 Reporte Diario: Venta Neta por Canal de Distribución",
            "emoji": True,
        }
    })

    if mv:
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"<!channel> *Informe de Venta Neta E-commerce (Consolidado $ sin IVA)*\n"
                    f"• *Fecha Evaluada*: `{eval_date}`\n"
                    f"• 💵 *Venta Neta Total E-commerce*: `${mv['monto_total_general']:,.0f}` ({mv['items_total']:,} ítems)\n"
                    f"• 🟢 *70 (ecommerce)*: `${mv['monto_c70']:,.0f}` (`{mv['pct_c70']:.2f}%`)\n"
                    f"• 🔵 *10 (cadena)*: `${mv['monto_c10']:,.0f}` (`{mv['pct_c10']:.2f}%`)\n"
                    f"• ⚪ *Sin Promoción*: `${mv['monto_sin_promo']:,.0f}` (`{mv['pct_sin_promo']:.2f}%`)\n"
                ),
            }
        })
    else:
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<!channel> *Informe de Venta Neta E-commerce*: _Sin ventas registradas en {eval_date}_",
            }
        })

    slack_blocks.append({"type": "divider"})

    # Bloque 2: Reporte de Activación de Venta por Canal (Hasta la Fecha)
    emoji_canal = {"70": "🛒", "10": "🏪"}
    nombre_canal = {"70": "Canal 70 (E-Commerce)", "10": "Canal 10 (Cadena)"}

    resumen_activacion_lines = []
    for canal in ["70", "10"]:
        if canal not in resultados_activacion:
            resumen_activacion_lines.append(f"{emoji_canal.get(canal, '📌')} *{nombre_canal.get(canal, f'Canal {canal}')}*: Sin datos")
            continue
        m = resultados_activacion[canal]
        resumen_activacion_lines.append(
            f"{emoji_canal.get(canal, '📌')} *{nombre_canal.get(canal, f'Canal {canal}')}*\n"
            f"   • Total SKUs en Promo: `{m['total_skus']}`\n"
            f"   • Con Venta ✅: `{m['skus_con_venta']}`   |   Sin Venta 🚨: `{m['skus_sin_venta']}`\n"
            f"   • *Tasa de Activación: {m['pct_conversion']}%*"
        )

    text_activacion = (
        f"*📊 Reporte de Activación de Venta por Canal (Hasta la Fecha)*\n"
        f"Auditoría de productos vigentes en promo por canal y su venta acumulada el día `{eval_date}`:\n\n"
        + "\n\n".join(resumen_activacion_lines)
    )

    slack_blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text_activacion,
        }
    })

    slack_blocks.append({"type": "divider"})

    # Bloque 3: Top 5 Líneas SAP por Venta Neta ($)
    top_text = mv.get("top_lineas_text", "_Sin datos_") if mv else "_Sin datos_"
    slack_blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*📌 Top 5 Líneas SAP por Venta Neta ($):*\n{top_text}"
        }
    })

    slack_blocks.append({"type": "divider"})

    # Bloque 4: Context / Adjunto
    slack_blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": f"📄 _Se adjunta en el hilo el informe consolidado en Excel (`{file_name}`) con el detalle de ventas, peso por línea SAP y tasa de activación por canal._"
            }
        ]
    })

    if not slack_token:
        print("⚠️ No se encontró token de Slack. Omitiendo envío a Slack.")
        return

    # ---------------------------------------------------------
    # 5. Enviar mensaje Slack y adjuntar Excel en el hilo
    # ---------------------------------------------------------
    headers = {
        "Authorization": f"Bearer {slack_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    fall_back_text = f"Reporte Diario Consolidado Ventas y Activación ({eval_date})"
    if mv:
        fall_back_text += f" | Venta Neta Total: ${mv['monto_total_general']:,.0f}"

    payload = {
        "channel": channel_id,
        "blocks": slack_blocks,
        "text": fall_back_text,
    }

    res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
    thread_ts = None
    if res.ok and res.json().get("ok"):
        print("✅ Notificación enviada exitosamente a Slack.")
        thread_ts = res.json().get("ts")
    else:
        print(f"⚠️ Error al enviar mensaje a Slack: {res.text}")

    # Subir Excel único en el hilo de Slack
    try:
        _upload_excel_file_to_slack(
            file_name=file_name,
            data_bytes=excel_bytes,
            channel_id=channel_id,
            token=slack_token,
            initial_comment=f"📊 Adjunto informe consolidado en Excel (Ventas E-Commerce y Tasa de Activación) para la fecha {eval_date}.",
            thread_ts=thread_ts
        )
        print("✅ Reporte Excel consolidado subido a Slack exitosamente.")
    except Exception as e_file:
        print(f"⚠️ Error al subir Excel a Slack: {e_file}")


with DAG(
    dag_id="etl_alerta_reporte_tasa_de_activacion",
    default_args=default_args,
    schedule_interval="0 10 * * *",  # Diariamente a las 10:00 AM (Chile)
    catchup=False,
    max_active_runs=1,
    tags=["Unimarc", "Alerta", "Promociones", "Canal70", "Canal10", "Ventas", "Slack", "Consolidado"],
    description=(
        "ETL consolidado: Auditoría de Venta Neta E-commerce (Canal 70, Canal 10, Sin Promo), "
        "Tasa de Activación de Promociones por Canal y Top 5 Líneas SAP con informe en Excel."
    ),
    on_success_callback=dag_success_slack,
) as dag:

    task_alerta_consolidada = PythonOperator(
        task_id="calcular_y_notificar_alerta_consolidada",
        python_callable=_calcular_y_notificar_alerta_consolidada,
        provide_context=True,
    )

    task_alerta_consolidada
