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
import numpy as np
import requests

# Orden geográfico de Norte a Sur para regiones de Chile
ORDEN_GEOGRAFICO = [
    "Arica y Parinacota", "Tarapacá", "Antofagasta", "Atacama", "Coquimbo",
    "Valparaíso", "Metropolitana", "O'Higgins", "Maule", "Ñuble",
    "Biobío", "La Araucanía", "Los Ríos", "Los Lagos", "Aysén", "Magallanes"
]

# Lista de Marcas Propias a EXCLUIR explícitamente de la alerta y Excel
MARCAS_EXCLUIDAS = [
    "ELABORACION PROPIA",
    "UNIMARC",
    "UNIMARC B",
    "PARTNER & CO.",
    "SANTO GUSTO",
    "GREEN LEGEND",
    "TENTO/UNIMARC"
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


def _clean_sheet_name(name: str) -> str:
    """Limpia y trunca a máximo 31 caracteres el nombre de pestaña para compatibilidad con Excel."""
    cleaned = re.sub(r'[\\/*?:\[\]]', '', str(name)).strip()
    return cleaned[:31] if cleaned else "Marca"


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


def _generar_matriz_grupo(df_tiendas, df_skus_base, df_stock_pos, df_lista8_tienda, col_grupo: str, regiones_unicas: list):
    """
    Genera la matriz de % disponibilidad por tienda y marca/línea evaluando solo los SKUs correspondientes en Lista 8 por tienda.
    Si una tienda no tiene SKUs de ese grupo en su Lista 8, el valor es 'N/A'.
    """
    elementos_unicos = sorted(df_skus_base[col_grupo].unique().tolist())

    # Cruce de Lista 8 (específica por tienda) con catálogo base de marcas propias
    df_surtido_tienda = pd.merge(
        df_lista8_tienda,
        df_skus_base[["sku_id", col_grupo]],
        on="sku_id",
        how="inner"
    )

    # Denominador: Total SKUs que la tienda DEBERÍA tener según Lista 8 por cada grupo (Marca / Línea)
    df_cat_tienda = df_surtido_tienda.groupby(["id_tienda", col_grupo])["sku_id"].nunique().reset_index()
    df_cat_tienda.rename(columns={"sku_id": "total_skus_esperados"}, inplace=True)

    # Numerador: Total SKUs con stock > 0 en esa tienda para ese grupo
    if not df_stock_pos.empty:
        df_stock_grupo = pd.merge(df_stock_pos, df_skus_base[["sku_id", col_grupo]], on="sku_id", how="inner")
        df_skus_con_stock = df_stock_grupo.groupby(["id_tienda", col_grupo])["sku_id"].nunique().reset_index()
        df_skus_con_stock.rename(columns={"sku_id": "skus_con_stock"}, inplace=True)
    else:
        df_skus_con_stock = pd.DataFrame(columns=["id_tienda", col_grupo, "skus_con_stock"])

    # Merge numerador y denominador
    df_disp_calc = pd.merge(df_cat_tienda, df_skus_con_stock, on=["id_tienda", col_grupo], how="left")
    df_disp_calc["skus_con_stock"] = df_disp_calc["skus_con_stock"].fillna(0)
    df_disp_calc["pct_disp"] = (df_disp_calc["skus_con_stock"] / df_disp_calc["total_skus_esperados"]) * 100.0

    # Crear grilla completa de (Tiendas x Grupos)
    tiendas_df = df_tiendas[["id_tienda", "nombre_tienda", "region_std"]].copy()

    # Pivotear porcentaje numérico (dejando NaN si no está en Lista 8 para esa tienda)
    df_pivot_num = df_disp_calc.pivot_table(index="id_tienda", columns=col_grupo, values="pct_disp", aggfunc="max")
    df_matrix_num = pd.merge(tiendas_df, df_pivot_num, on="id_tienda", how="left")

    for elem in elementos_unicos:
        if elem not in df_matrix_num.columns:
            df_matrix_num[elem] = np.nan

    # Promedio regional (omite automáticamente los NaN / N/A)
    df_promedios_reg = df_matrix_num.groupby("region_std")[elementos_unicos].mean().reset_index()

    # Construir filas para Excel
    final_rows_disp = []

    for r in regiones_unicas:
        df_r = df_matrix_num[df_matrix_num["region_std"] == r].sort_values("id_tienda").copy()
        for elem in elementos_unicos:
            df_r[elem] = df_r[elem].apply(lambda x: "N/A" if pd.isna(x) else f"{round(x, 1)}%")
        final_rows_disp.append(df_r)

        row_prom = df_promedios_reg[df_promedios_reg["region_std"] == r].iloc[0]
        dict_prom = {
            "id_tienda": "PROMEDIO",
            "nombre_tienda": f"Promedio {r}",
            "region_std": r
        }
        for elem in elementos_unicos:
            val_p = row_prom[elem]
            dict_prom[elem] = "N/A" if pd.isna(val_p) else f"{round(val_p, 1)}%"
        final_rows_disp.append(pd.DataFrame([dict_prom]))

    df_excel_disp = pd.concat(final_rows_disp, ignore_index=True).rename(columns={
        "id_tienda": "ID Tienda", "nombre_tienda": "Nombre Tienda", "region_std": "Región"
    })

    # --- Construir DataFrame Transpuesto para Slack ---
    df_prom_tabla = df_excel_disp[df_excel_disp["ID Tienda"] == "PROMEDIO"].copy()
    df_prom_tabla["Región"] = df_prom_tabla["Nombre Tienda"].str.replace("Promedio ", "", regex=False)
    df_prom_tabla["Región"] = pd.Categorical(df_prom_tabla["Región"], categories=regiones_unicas, ordered=True)
    df_prom_tabla = df_prom_tabla.sort_values("Región")

    df_transpuesta = df_prom_tabla.set_index("Región")[elementos_unicos].T.reset_index()
    df_transpuesta = df_transpuesta.rename(columns={"index": col_grupo.title()})

    return df_excel_disp, df_transpuesta, elementos_unicos


def _construir_pestaña_marca(df_skus_marca, df_tiendas, df_stock_pos, df_lista8_tienda):
    """
    Construye la matriz en Excel para una marca específica pertenecientes a Marcas Propias:
    - Eje Y: SKUs (SKU ID, Material, Descripción)
    - Eje X: Tiendas (ID Tienda / Nombre Tienda)
    - Valores: Stock Unitario si pertenece a Lista 8 de esa tienda, o 'N/A' si no corresponde por surtido.
    """
    tiendas_ids = df_tiendas["id_tienda"].tolist()
    skus_marca_ids = df_skus_marca["sku_id"].tolist()

    # Filtrar Lista 8 para la marca en cuestión
    df_l8_marca = df_lista8_tienda[df_lista8_tienda["sku_id"].isin(skus_marca_ids)].copy()
    df_l8_marca["en_surtido"] = True

    # Filtrar Stock positivo para la marca
    if not df_stock_pos.empty:
        df_stock_brand = pd.merge(df_stock_pos, df_skus_marca[["sku_id"]], on="sku_id", how="inner")
    else:
        df_stock_brand = pd.DataFrame(columns=["id_tienda", "sku_id", "stock_janis"])

    # Crear DataFrame base SKU x Tienda
    df_base = df_skus_marca[["sku_id", "material", "sku_nombre"]].copy()

    for t_id in tiendas_ids:
        # SKUs en lista 8 de esta tienda
        skus_l8_t = set(df_l8_marca[df_l8_marca["id_tienda"] == t_id]["sku_id"])
        
        # Stock de esta tienda
        stock_t_dict = df_stock_brand[df_stock_brand["id_tienda"] == t_id].set_index("sku_id")["stock_janis"].to_dict()

        col_vals = []
        for s_id in df_base["sku_id"]:
            if s_id not in skus_l8_t:
                col_vals.append("N/A")
            else:
                col_vals.append(int(stock_t_dict.get(s_id, 0)))
        
        df_base[t_id] = col_vals

    df_base.rename(columns={
        "sku_id": "SKU ID",
        "material": "Material",
        "sku_nombre": "Descripción SKU"
    }, inplace=True)

    return df_base


def _construir_bloques_slack_tabla(df_transpuesta, col_name: str, elementos_unicos: list, regiones_list: list) -> list:
    """
    Construye bloques de texto en formato tabla ASCII divididos por regiones (4 por bloque) 
    y paginando filas (máx 15 filas por bloque) para garantizar que NO se superen los 3000 caracteres de Slack.
    """
    TAMANO_GRUPO_REGIONES = 4
    TAMANO_GRUPO_FILAS = 15
    ancho_elem = min(max(max(len(str(e)) for e in elementos_unicos), len(col_name)) + 1, 22)
    ancho_col_region = 13
    blocks = []

    rows_list = list(df_transpuesta.iterrows())

    for i in range(0, len(regiones_list), TAMANO_GRUPO_REGIONES):
        grupo_regiones = regiones_list[i:i + TAMANO_GRUPO_REGIONES]
        
        header_cols = [r[:ancho_col_region].ljust(ancho_col_region) for r in grupo_regiones]
        header = col_name[:ancho_elem].ljust(ancho_elem) + " | " + " | ".join(header_cols)
        separador = "-" * len(header)

        for r_start in range(0, len(rows_list), TAMANO_GRUPO_FILAS):
            chunk_rows = rows_list[r_start:r_start + TAMANO_GRUPO_FILAS]
            lineas_bloque = [header, separador]
            
            for _, row in chunk_rows:
                elem_nombre = str(row[col_name])[:ancho_elem].ljust(ancho_elem)
                valores_regiones = [str(row[reg])[:ancho_col_region].ljust(ancho_col_region) for reg in grupo_regiones]
                fila = elem_nombre + " | " + " | ".join(valores_regiones)
                lineas_bloque.append(fila)
                
            tabla_texto_chunk = "\n".join(lineas_bloque)
            
            if len(tabla_texto_chunk) > 2900:
                tabla_texto_chunk = tabla_texto_chunk[:2850] + "\n... (Tabla recortada por límite de Slack)"

            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{tabla_texto_chunk}```"
                }
            })

    return blocks


def _procesar_y_enviar_alerta_marcas_propias(**kwargs):
    print("🔍 Iniciando auditoría diaria de disponibilidad para SKUs de Marcas Propias activos en Lista 8...")
    
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

    # 2. Extraer SKUs de Marcas Propias filtrando EXPLICITAMENTE por SKUs activos en Lista 8
    query_skus = """
    SELECT DISTINCT 
        s.ref_id AS sku_id,
        m.material,
        COALESCE(NULLIF(TRIM(m.sku_name), ''), p.nombre, s.ref_id) AS sku_nombre,
        COALESCE(NULLIF(TRIM(m.marca), ''), 'Sin Marca') AS marca,
        COALESCE(NULLIF(TRIM(m.linea_sap), ''), 'Sin Línea') AS linea_sap
    FROM ecommdata.maestra_sku_proveedor m
    JOIN ecommdata.productos p ON m.material = p.material
    JOIN ecommdata.skus s ON p.ref_id = s.ref_id
    JOIN (SELECT DISTINCT material FROM ecommdata.lista8) l ON m.material = l.material
    WHERE m.marca_propia = 1;
    """
    df_skus_base = pg_hook.get_pandas_df(query_skus)
    if df_skus_base.empty:
        print("⚠️ No se encontraron SKUs de Marcas Propias activos en Lista 8 en la base de datos.")
        return

    # Excluir marcas específicas solicitadas
    df_skus_base = df_skus_base[~df_skus_base["marca"].str.upper().isin(MARCAS_EXCLUIDAS)]
    if df_skus_base.empty:
        print("⚠️ Tras aplicar exclusión de marcas, no quedan SKUs a evaluar.")
        return

    skus_ids = df_skus_base["sku_id"].unique().tolist()
    print(f"🏷️ Total SKUs de Marcas Propias (en Lista 8) a monitorear: {len(skus_ids)}")

    # 3. Consulta de Surtido Validadas por Tienda desde Lista 8 (cruza material/umv con ref_id)
    query_lista8_tienda = """
    SELECT DISTINCT
        LPAD(l8.id_tienda::text, 4, '0') AS id_tienda,
        s.ref_id AS sku_id
    FROM ecommdata.lista8 l8
    JOIN ecommdata.productos p ON l8.material = p.material
    JOIN ecommdata.skus s ON p.ref_id = s.ref_id
    JOIN ecommdata.maestra_sku_proveedor m ON p.material = m.material
    WHERE m.marca_propia = 1;
    """
    print("⚡ Obteniendo surtido por tienda (Lista 8) para Marcas Propias...")
    df_lista8_tienda = pg_hook.get_pandas_df(query_lista8_tienda)
    print(f"📋 Registros de surtido tienda-SKU recuperados de Lista 8: {len(df_lista8_tienda)}")

    # 4. Consulta de Stock de alto rendimiento (Join 100% interno en Postgres filtrando por Lista 8 y stock positivo)
    query_stock_optimizada = """
    WITH max_fecha AS (
        SELECT MAX(fecha) AS max_f FROM ecommdata.stock
    ),
    skus_mp AS (
        SELECT DISTINCT s.ref_id AS sku_id
        FROM ecommdata.maestra_sku_proveedor m
        JOIN ecommdata.productos p ON m.material = p.material
        JOIN ecommdata.skus s ON p.ref_id = s.ref_id
        JOIN (SELECT DISTINCT material FROM ecommdata.lista8) l ON m.material = l.material
        WHERE m.marca_propia = 1
    )
    SELECT 
        LPAD(st.id_tienda::text, 4, '0') AS id_tienda,
        st.ref_id AS sku_id,
        st.stock_janis
    FROM ecommdata.stock st
    JOIN max_fecha mf ON st.fecha = mf.max_f
    JOIN skus_mp mp ON st.ref_id = mp.sku_id
    WHERE st.stock_janis > 0;
    """
    print("⚡ Ejecutando consulta de stock optimizada (con filtro Lista 8) en Postgres...")
    df_stock_pos = pg_hook.get_pandas_df(query_stock_optimizada)
    print(f"📊 Registros con stock positivo recuperados: {len(df_stock_pos)}")

    # Orden geográfico presente
    regiones_presentes = set(df_tiendas["region_std"].unique())
    regiones_unicas = [r for r in ORDEN_GEOGRAFICO if r in regiones_presentes]
    regiones_no_mapeadas = sorted(list(regiones_presentes - set(regiones_unicas)))
    regiones_unicas.extend(regiones_no_mapeadas)

    # 5. Generar Matrices para Vista 1 (Marca) y Vista 2 (Línea SAP)
    df_excel_disp_marca, df_transp_marca, marcas_unicas = _generar_matriz_grupo(
        df_tiendas, df_skus_base, df_stock_pos, df_lista8_tienda, "marca", regiones_unicas
    )

    df_excel_disp_linea, df_transp_linea, lineas_unicas = _generar_matriz_grupo(
        df_tiendas, df_skus_base, df_stock_pos, df_lista8_tienda, "linea_sap", regiones_unicas
    )

    # 6. Generar archivo Excel en memoria:
    # - Pestaña 1: Disponibilidad Marca
    # - Pestaña 2: Disponibilidad Linea SAP
    # - Pestañas individuales por cada Marca Propia: SKUs (eje Y) vs Tiendas (eje X) con stock unitario (o N/A si no aplica surtido)
    excel_buffer = io.BytesIO()
    used_sheet_names = set(["Disponibilidad Marca", "Disponibilidad Linea SAP"])

    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        df_excel_disp_marca.to_excel(writer, sheet_name="Disponibilidad Marca", index=False)
        df_excel_disp_linea.to_excel(writer, sheet_name="Disponibilidad Linea SAP", index=False)

        for m_nombre in marcas_unicas:
            df_skus_m = df_skus_base[df_skus_base["marca"] == m_nombre].copy()
            df_m_sheet = _construir_pestaña_marca(df_skus_m, df_tiendas, df_stock_pos, df_lista8_tienda)
            
            s_name = _clean_sheet_name(m_nombre)
            counter = 1
            original_s_name = s_name
            while s_name in used_sheet_names:
                s_name = f"{original_s_name[:28]}_{counter}"
                counter += 1
            used_sheet_names.add(s_name)

            df_m_sheet.to_excel(writer, sheet_name=s_name, index=False)

    excel_bytes = excel_buffer.getvalue()
    file_name = f"alerta_disponibilidad_marcas_propias_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    # 6. Notificación a Slack
    channel_var_name = "SLACK_DISPONIBILIDAD_MARCAS_PROPIAS_ALERT"
    channel_id = Variable.get(channel_var_name, default_var=None) or Variable.get("SLACK_PROMOTIONS_VIEWER_ALERT", default_var="C0BVBAHD7L0")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    regiones_list_marca = [col for col in df_transp_marca.columns if col != "Marca"]
    regiones_list_linea = [col for col in df_transp_linea.columns if col != "Linea_Sap"]

    intro = (
        f"<!channel> *🏷️ Alerta Diaria Disponibilidad de Marcas Propias por Región*\n"
        f"• *Tiendas Activas Evaluadas*: {len(df_tiendas)}\n"
        f"• *SKUs Marcas Propias en Lista 8*: {len(skus_ids)}\n"
        f"• *Marcas Propias Evaluadas*: {len(marcas_unicas)} | *Líneas SAP Evaluadas*: {len(lineas_unicas)}\n"
        f"• *Métrica*: % Disponibilidad Promedio por Región (SKUs con stock / Total SKUs catálogo Lista 8)\n"
    )

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🏷️ Reporte de Disponibilidad Marcas Propias",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": intro
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*📌 Vista 1: Disponibilidad (%) por Marca Propia x Región*"
            }
        }
    ]

    # Agregar tablas de Vista 1 (Marca)
    slack_blocks.extend(_construir_bloques_slack_tabla(df_transp_marca, "Marca", marcas_unicas, regiones_list_marca))

    slack_blocks.append({"type": "divider"})

    footer_block = {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "📄 _Se adjunta en el hilo el reporte detallado en Excel con 2 pestañas resumen y 1 pestaña por cada Marca Propia (SKUs x Tienda)._\n⚠️ *Disclaimer*: _Esta tabla es referencial, ya que los datos de stock se actualizan a las 08:00 AM._"
            }
        ]
    }

    slack_blocks.append({"type": "divider"})
    slack_blocks.append(footer_block)

    # Limitar bloques si superan el máximo permitido por Slack (50 bloques máx)
    if len(slack_blocks) > 48:
        slack_blocks = slack_blocks[:47]
        slack_blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "⚠️ _El reporte en Slack fue resumido para no exceder los límites de bloques de Slack. Revisa el Excel adjunto en el hilo para las matrices completas por marca._"
            }]
        })
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
            "text": f"Alerta Disponibilidad Marcas Propias: {len(skus_ids)} SKUs en {len(df_tiendas)} tiendas."
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
                initial_comment=f"📊 Adjunto reporte detallado de disponibilidad de Marcas Propias en Excel ({file_name}).",
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
    "etl_alerta_disponibilidad_marcas_propias",
    default_args=default_args,
    description="ETL de auditoría diaria de disponibilidad de stock de Marcas Propias (en Lista 8) por Región (Vistas Marca y Línea SAP).",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "stock", "marcas_propias", "linea_sap", "disponibilidad", "lista8", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría de Disponibilidad de Marcas Propias (Lista 8) por Regiones, Marca y Línea SAP
    Este DAG consulta diariamente la disponibilidad de stock en tiendas activas para los SKUs pertenecientes a Marcas Propias (`marca_propia = 1`) que están activos en Lista 8 (`ecommdata.lista8`), calculando el porcentaje de disponibilidad respecto al catálogo de cada marca y línea SAP. Genera dos vistas tabulares (Marca y Línea SAP) ordenadas de Norte a Sur y envía la notificación a Slack con el reporte en Excel (pestañas resumen + 1 pestaña por marca propia con desglose SKU vs Tienda) adjunto al hilo.
    """

    task_auditar_disponibilidad = PythonOperator(
        task_id="auditar_disponibilidad_marcas_propias",
        python_callable=_procesar_y_enviar_alerta_marcas_propias,
    )
