from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack
from datetime import datetime
import pendulum
import io
import json
import re
import pandas as pd
import requests

# Orden geográfico de Norte a Sur
ORDEN_GEOGRAFICO = [
    "Arica y Parinacota", "Tarapacá", "Antofagasta", "Atacama", "Coquimbo",
    "Valparaíso", "Metropolitana", "O'Higgins", "Maule", "Ñuble",
    "Biobío", "La Araucanía", "Los Ríos", "Los Lagos", "Aysén", "Magallanes"
]


def standardize_region(region_val) -> str:
    """Estandariza los nombres de las regiones geográficas de Chile."""
    if not region_val or pd.isna(region_val):
        return "Sin Región"
    
    r = str(region_val).strip().replace(".0", "").upper()
    
    if r in ["15", "XV", "ARICA Y PARINACOTA", "ARICA"]: return "Arica y Parinacota"
    if r in ["1", "I", "TARAPACA", "TARAPACÁ"]: return "Tarapacá"
    if r in ["2", "II", "ANTOFAGASTA"]: return "Antofagasta"
    if r in ["3", "III", "ATACAMA"]: return "Atacama"
    if r in ["4", "IV", "COQUIMBO"]: return "Coquimbo"
    if r in ["5", "V", "VALPARAISO", "VALPARAÍSO"]: return "Valparaíso"
    if r in ["6", "VI", "O'HIGGINS", "OHIGGINS"]: return "O'Higgins"
    if r in ["7", "VII", "MAULE"]: return "Maule"
    if r in ["8", "VIII", "BIOBIO", "BIO BIO", "BIO-BIO", "BIOBÍO"]: return "Biobío"
    if r in ["9", "IX", "ARAUCANIA", "ARAUCANÍA"]: return "La Araucanía"
    if r in ["10", "X", "LOS LAGOS"]: return "Los Lagos"
    if r in ["11", "XI", "AYSEN", "AYSÉN"]: return "Aysén"
    if r in ["12", "XII", "MAGALLANES"]: return "Magallanes"
    if r in ["13", "RM", "SANTIAGO", "METROPOLITANA"]: return "Metropolitana"
    if r in ["14", "XIV", "LOS RIOS", "LOS RÍOS"]: return "Los Ríos"
    if r in ["16", "XVI", "NUBLE", "ÑUBLE"]: return "Ñuble"
    
    match = re.search(r"\d+", r)
    if match:
        val = int(match.group(0))
        mapping = {
            15: "Arica y Parinacota", 1: "Tarapacá", 2: "Antofagasta", 3: "Atacama", 
            4: "Coquimbo", 5: "Valparaíso", 6: "O'Higgins", 7: "Maule", 8: "Biobío", 
            9: "La Araucanía", 10: "Los Lagos", 11: "Aysén", 12: "Magallanes", 
            13: "Metropolitana", 14: "Los Ríos", 16: "Ñuble"
        }
        if val in mapping:
            return mapping[val]
            
    return r.title()


def _upload_excel_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = "", thread_ts: str = None):
    """Sube un archivo a Slack usando la API v2 (getUploadURLExternal y completeUploadExternal)."""
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


def _procesar_y_enviar_alerta_frutas_verduras(**kwargs):
    print("🔍 Iniciando proceso de auditoría de disponibilidad de stock para SKUs de frutas y verduras...")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")

    # 1. Obtener tiendas activas y estandarizar su región
    query_tiendas = """
    SELECT 
        LPAD(t.id::text, 4, '0') AS id_tienda,
        COALESCE(t.nombre_tienda, t.glosa, t.id::text) AS nombre_tienda,
        t.region
    FROM ecommdata.tiendas t
    WHERE t.status = 1;
    """
    df_tiendas = pg_hook.get_pandas_df(query_tiendas)
    if df_tiendas.empty:
        print("❌ No se encontraron tiendas activas en ecommdata.tiendas.")
        return

    df_tiendas["region_std"] = df_tiendas["region"].apply(standardize_region)
    print(f"🏬 Total tiendas activas encontradas: {len(df_tiendas)}")
    print(df_tiendas["region_std"].value_counts())

    # 2. Obtener la lista de SKUs desde ecommdata.alerta_frutas_verduras
    query_skus = """
    SELECT producto, sku_id
    FROM ecommdata.alerta_frutas_verduras
    ORDER BY producto, sku_id;
    """
    df_skus_base = pg_hook.get_pandas_df(query_skus)
    if df_skus_base.empty:
        print("❌ No se encontraron SKUs en ecommdata.alerta_frutas_verduras.")
        return

    print(f"🥦 Total SKUs de frutas y verduras cargados: {len(df_skus_base)}")

    skus_ids = df_skus_base["sku_id"].unique().tolist()
    skus_in_clause = "'" + "', '".join(skus_ids) + "'"
    productos_unicos = list(dict.fromkeys(df_skus_base["producto"].tolist()))

    # 3. Obtener surtido validado por tienda desde Lista 8
    # Solo se evalúan las combinaciones (tienda, SKU) que están en el catálogo vigente de cada tienda
    query_lista8 = """
    SELECT DISTINCT
        LPAD(l8.id_tienda::text, 4, '0') AS id_tienda,
        s.ref_id AS sku_id
    FROM ecommdata.lista8 l8
    JOIN ecommdata.productos p ON l8.material = p.material
    JOIN ecommdata.skus s ON p.ref_id = s.ref_id
    JOIN ecommdata.tiendas t ON LPAD(l8.id_tienda::text, 4, '0') = LPAD(t.id::text, 4, '0') AND t.status = 1
    WHERE s.ref_id IN ({skus_in_clause});
    """.format(skus_in_clause=skus_in_clause)
    df_lista8_tienda = pg_hook.get_pandas_df(query_lista8)
    print(f"📋 Registros de surtido tienda-SKU en Lista 8: {len(df_lista8_tienda)}")

    # 4. Consultar stock actual de la última carga en ecommdata.stock
    # Se filtra por Lista 8 en SQL para eficiencia: solo (tienda, SKU) del catálogo vigente
    query_stock = f"""
    WITH max_fecha AS (
        SELECT MAX(fecha) AS max_f FROM ecommdata.stock
    )
    SELECT 
        LPAD(st.id_tienda::text, 4, '0') AS id_tienda,
        st.ref_id AS sku_id,
        COALESCE(st.stock_janis, 0) AS stock_janis
    FROM ecommdata.stock st
    JOIN max_fecha m ON st.fecha = m.max_f
    JOIN ecommdata.tiendas t ON LPAD(st.id_tienda::text, 4, '0') = LPAD(t.id::text, 4, '0') AND t.status = 1
    JOIN ecommdata.lista8 l8 ON l8.id_tienda = st.id_tienda
    JOIN ecommdata.productos p ON l8.material = p.material AND p.ref_id = st.ref_id
    WHERE st.ref_id IN ({skus_in_clause});
    """
    df_stock = pg_hook.get_pandas_df(query_stock)
    print(f"📊 Registros de stock leídos (filtrados por Lista 8): {len(df_stock)}")

    # Filtrar solo registros con stock suficiente (mínimo 10 unidades)
    # Una tienda se considera "disponible" para un producto si al menos 1 SKU tiene >= 10 unidades
    df_stock_pos = df_stock[df_stock["stock_janis"] >= 10]

    # 4. Generar Matriz de Disponibilidad (Tiendas vs Productos)
    df_stock_producto = pd.merge(df_stock_pos, df_skus_base[["sku_id", "producto"]], on="sku_id", how="inner")
    df_stock_producto["tiene_stock"] = 1
    
    if not df_stock_producto.empty:
        df_pivot = df_stock_producto.pivot_table(index="id_tienda", columns="producto", values="tiene_stock", aggfunc="max", fill_value=0).reset_index()
        df_pivot_stock = df_stock_producto.pivot_table(index="id_tienda", columns="producto", values="stock_janis", aggfunc="sum", fill_value=0).reset_index()
    else:
        df_pivot = pd.DataFrame(columns=["id_tienda"] + productos_unicos)
        df_pivot_stock = pd.DataFrame(columns=["id_tienda"] + productos_unicos)
    
    df_matrix = pd.merge(df_tiendas[["id_tienda", "nombre_tienda", "region_std"]], df_pivot, on="id_tienda", how="left")
    df_matrix_stock = pd.merge(df_tiendas[["id_tienda", "nombre_tienda", "region_std"]], df_pivot_stock, on="id_tienda", how="left")
    
    for c in productos_unicos:
        if c not in df_matrix.columns:
            df_matrix[c] = 0
        if c not in df_matrix_stock.columns:
            df_matrix_stock[c] = 0
            
    df_matrix[productos_unicos] = df_matrix[productos_unicos].fillna(0).astype(int)
    df_matrix_stock[productos_unicos] = df_matrix_stock[productos_unicos].fillna(0).astype(int)

    # 5. Calcular promedios y totales por Región (sobre el universo COMPLETO de tiendas)
    df_promedios = df_matrix.groupby("region_std")[productos_unicos].mean().reset_index()
    df_totales_stock = df_matrix_stock.groupby("region_std")[productos_unicos].sum().reset_index()

    # Filtrar tiendas con quiebre: al menos 1 producto con 0 en la matriz (< 10 unidades)
    mask_quiebre = (df_matrix[productos_unicos] == 0).any(axis=1)
    df_matrix_quiebre = df_matrix[mask_quiebre].copy()
    df_matrix_stock_quiebre = df_matrix_stock[df_matrix["id_tienda"].isin(df_matrix_quiebre["id_tienda"])].copy()
    print(f"⚠️ Tiendas con quiebre en al menos 1 producto: {len(df_matrix_quiebre)} de {len(df_matrix)}")

    # 6. Formatear las matrices para Excel (orden geográfico de Norte a Sur)
    # Solo se incluyen tiendas con quiebre; promedios reflejan el universo total
    final_rows = []
    final_rows_stock = []

    regiones_presentes = set(df_matrix["region_std"].unique())
    regiones_unicas = [r for r in ORDEN_GEOGRAFICO if r in regiones_presentes]
    # Si existen regiones no mapeadas en la lista, agregarlas al final ---
    regiones_no_mapeadas = sorted(list(regiones_presentes - set(regiones_unicas)))
    regiones_unicas.extend(regiones_no_mapeadas)

    for r in regiones_unicas:
        # ---- Matriz de Disponibilidad (%) — solo tiendas con quiebre ----
        df_r = df_matrix_quiebre[df_matrix_quiebre["region_std"] == r].sort_values("id_tienda").copy()

        # Si no hay tiendas con quiebre en esta región, se omite del reporte
        if df_r.empty:
            continue

        for c in productos_unicos:
            # Celdas con stock OK (100%) quedan vacías para resaltar los quiebres
            df_r[c] = df_r[c].apply(lambda x: "" if x == 1 else "0%")
        final_rows.append(df_r)

        # Fila de promedio calculada sobre el universo completo de tiendas de la región
        row_promedio = df_promedios[df_promedios["region_std"] == r].iloc[0]
        dict_promedio = {
            "id_tienda": "PROMEDIO",
            "nombre_tienda": f"Promedio {r} (todas las tiendas)",
            "region_std": r
        }
        for c in productos_unicos:
            val = round(row_promedio[c] * 100, 1)
            # Promedios 100% quedan vacíos para resaltar los quiebres regionales
            dict_promedio[c] = "" if val == 100.0 else f"{val}%"

        df_prom_row = pd.DataFrame([dict_promedio])
        final_rows.append(df_prom_row)

        # ---- Matriz de Unidades (Stock) — solo tiendas con quiebre ----
        df_r_stock = df_matrix_stock_quiebre[df_matrix_stock_quiebre["region_std"] == r].sort_values("id_tienda").copy()
        final_rows_stock.append(df_r_stock)

        # Fila de total calculada sobre el universo completo de tiendas de la región
        row_total = df_totales_stock[df_totales_stock["region_std"] == r].iloc[0]
        dict_total = {
            "id_tienda": "TOTAL",
            "nombre_tienda": f"Total {r} (todas las tiendas)",
            "region_std": r
        }
        for c in productos_unicos:
            dict_total[c] = int(row_total[c])

        df_tot_row = pd.DataFrame([dict_total])
        final_rows_stock.append(df_tot_row)

    if not final_rows:
        print("✅ No se encontraron tiendas con quiebre de stock. No se genera reporte.")
        return

    df_final_excel = pd.concat(final_rows, ignore_index=True)
    df_final_excel = df_final_excel.rename(columns={
        "id_tienda": "ID Tienda",
        "nombre_tienda": "Nombre Tienda",
        "region_std": "Región"
    })
    
    df_final_stock_excel = pd.concat(final_rows_stock, ignore_index=True)
    df_final_stock_excel = df_final_stock_excel.rename(columns={
        "id_tienda": "ID Tienda",
        "nombre_tienda": "Nombre Tienda",
        "region_std": "Región"
    })

    # 7. Crear el archivo Excel en memoria (2 pestañas)
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        df_final_excel.to_excel(writer, sheet_name="Disponibilidad por Tienda", index=False)
        df_final_stock_excel.to_excel(writer, sheet_name="Stock Unidades", index=False)
    
    excel_bytes = excel_buffer.getvalue()
    file_name = f"alerta_disponibilidad_frutas_verduras_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    # 8. Notificación en Slack
    channel_var_name = "SLACK_DISPONIBILIDAD_FRUTAS_VERDURAS_ALERT"
    channel_id = Variable.get(channel_var_name, default_var=None) or Variable.get("SLACK_PROMOTIONS_VIEWER_ALERT", default_var="C0CBD0BB80Z")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    # -------------------------------------------------------------
    # CONSTRUCCIÓN DEL MENSAJE SLACK: tabla plana por producto
    # Muestra las tiendas con quiebre (<10 unidades) para cada producto
    # -------------------------------------------------------------

    # Calcular el stock máximo por (tienda, producto) usando todos los SKUs
    # Esto representa "lo mejor que tiene esa tienda" para ese producto
    df_stock_max = (
        df_stock
        .merge(df_skus_base[["sku_id", "producto"]], on="sku_id", how="inner")
        .groupby(["id_tienda", "producto"], as_index=False)["stock_janis"]
        .max()
        .rename(columns={"stock_janis": "stock_max"})
    )

    # Obtener las tiendas con quiebre (0 en df_matrix) en formato largo
    df_quiebre_long = (
        df_matrix_quiebre[["id_tienda", "nombre_tienda", "region_std"] + productos_unicos]
        .melt(id_vars=["id_tienda", "nombre_tienda", "region_std"],
              value_vars=productos_unicos,
              var_name="producto",
              value_name="tiene_stock")
        .query("tiene_stock == 0")  # Solo los quiebres
        .merge(df_stock_max, on=["id_tienda", "producto"], how="left")
        .fillna({"stock_max": 0})
        .sort_values(["producto", "id_tienda"])
    )
    df_quiebre_long["stock_max"] = df_quiebre_long["stock_max"].astype(int)

    n_quiebres_total = len(df_quiebre_long)
    productos_con_quiebre = df_quiebre_long["producto"].unique().tolist()
    n_productos_quiebre = len(productos_con_quiebre)
    print(f"⚠️ Combinaciones tienda-producto con quiebre: {n_quiebres_total}")
    print(f"⚠️ Productos con quiebre en al menos 1 tienda: {n_productos_quiebre}")

    intro = (
        f"<!channel> *🥦 Alerta de Quiebre de Stock — Frutas y Verduras*\n"
        f"• *Tiendas Evaluadas*: {len(df_tiendas)} | *Con quiebre (<10 un.)*: {len(df_matrix_quiebre)}\n"
        f"• *Productos afectados*: {n_productos_quiebre} | *Quiebres totales (tienda × producto)*: {n_quiebres_total}\n"
        f"• Umbral: una tienda tiene quiebre si ningún SKU del producto alcanza las 10 unidades\n"
    )

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🥦 Quiebre de Stock — Frutas y Verduras",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": intro
            }
        }
    ]

    if df_quiebre_long.empty:
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "✅ *Sin quiebres detectados.* Todas las tiendas tienen ≥10 unidades en todos los productos."
            }
        })
    else:
        # Anchos de columna para la tabla monoespaciada
        ancho_id     = 6
        ancho_tienda = max(df_quiebre_long["nombre_tienda"].str.len().max(), len("Nombre Tienda")) + 1
        ancho_stock  = 7  # "Stock  "

        header_tabla = (
            "ID".ljust(ancho_id) + " | " +
            "Nombre Tienda".ljust(ancho_tienda) + " | " +
            "Stock"
        )
        separador_tabla = "-" * len(header_tabla)

        # Un bloque de Slack por producto (máx ~2900 chars por bloque)
        for producto in productos_con_quiebre:
            df_prod = df_quiebre_long[df_quiebre_long["producto"] == producto].copy()

            lineas = [header_tabla, separador_tabla]
            for _, row in df_prod.iterrows():
                lineas.append(
                    str(row["id_tienda"]).ljust(ancho_id) + " | " +
                    str(row["nombre_tienda"])[:ancho_tienda].ljust(ancho_tienda) + " | " +
                    str(row["stock_max"])
                )

            # Paginar si el bloque supera el límite de Slack (3000 chars)
            SLACK_CHAR_LIMIT = 2800
            chunk_lines = []
            chunk_actual = [header_tabla, separador_tabla]

            for linea in lineas[2:]:  # saltar header y separador ya incluidos
                test = "\n".join(chunk_actual + [linea])
                if len(f"```{test}```") > SLACK_CHAR_LIMIT:
                    texto_chunk = "\n".join(chunk_actual)
                    slack_blocks.append({
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*{producto}*\n```{texto_chunk}```"
                        }
                    })
                    chunk_actual = [header_tabla, separador_tabla, linea]
                else:
                    chunk_actual.append(linea)

            # Último chunk
            if len(chunk_actual) > 2:
                texto_chunk = "\n".join(chunk_actual)
                slack_blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*{producto}*\n```{texto_chunk}```"
                    }
                })

    footer_block = {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "📄 _Se adjunta en el hilo el reporte detallado en Excel con 2 pestañas (Disponibilidad y Stock en Unidades)._\n⚠️ *Disclaimer*: _Esta tabla es referencial, ya que los datos de stock se actualizan a las 08:00 AM._"
            }
        ]
    }

    slack_blocks.append({"type": "divider"})
    slack_blocks.append(footer_block)

    thread_ts = None
    if slack_token and channel_id:
        headers = {
            "Authorization": f"Bearer {slack_token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        payload = {
            "channel": channel_id,
            "blocks": slack_blocks,
            "text": f"Alerta Disponibilidad Frutas y Verduras: Matriz de {len(productos_unicos)} productos en {len(df_tiendas)} tiendas."
        }
        res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
        res_json = res.json() if res.ok else {}
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
                initial_comment=f"📊 Adjunto reporte de disponibilidad de frutas y verduras (Matriz por Tienda) en Excel ({file_name}).",
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
    "retries": 0,
}

with DAG(
    "etl_alerta_disponibilidad_frutas_verduras",
    default_args=default_args,
    description="ETL de auditoría diaria de disponibilidad de stock de frutas y verduras por Región y subcategorías.",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "stock", "frutas", "verduras", "disponibilidad", "regiones", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría de Disponibilidad de Frutas y Verduras por Regiones y Productos
    Este DAG consulta diariamente la disponibilidad de stock en tiendas activas para los SKUs de frutas y verduras
    definidos en `ecommdata.alerta_frutas_verduras`, genera una matriz binaria (100% o 0%) por Tienda y Producto
    agrupada por Región, y la envía a Slack junto a los promedios regionales.
    Los SKUs se administran directamente en la tabla `ecommdata.alerta_frutas_verduras` sin necesidad de modificar el DAG.
    """

    task_auditar_disponibilidad = PythonOperator(
        task_id="auditar_disponibilidad_frutas_verduras_por_zona",
        python_callable=_procesar_y_enviar_alerta_frutas_verduras,
    )