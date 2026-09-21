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

# Lista Maestra de SKUs Objetivo a monitorear
SKUS_OBJETIVO = [
    {"categoria": "Filete", "corte": "Filete", "sku_id": "000000000000752514-KG"},
    {"categoria": "Filete", "corte": "Filete", "sku_id": "000000000000400622-KGV"},
    {"categoria": "Lomo Vetado", "corte": "Lomo Vetado", "sku_id": "000000000000752526-KG"},
    {"categoria": "Lomo Vetado", "corte": "Lomo Vetado", "sku_id": "000000000000566220-KG"},
    {"categoria": "Lomo Liso", "corte": "Lomo Liso", "sku_id": "000000000000752523-KG"},
    {"categoria": "Lomo Liso", "corte": "Lomo Liso", "sku_id": "000000000000566219-KG"},
    {"categoria": "Parrilleros", "corte": "Abastero", "sku_id": "000000000000400594-KGV"},
    {"categoria": "Parrilleros", "corte": "Abastero", "sku_id": "000000000000752502-KG"},
    {"categoria": "Parrilleros", "corte": "Abastero", "sku_id": "000000000000028594-KGV"},
    {"categoria": "Parrilleros", "corte": "Asado Carnicero", "sku_id": "000000000000400613-KGV"},
    {"categoria": "Parrilleros", "corte": "Asado Carnicero", "sku_id": "000000000000503362-KGV"},
    {"categoria": "Parrilleros", "corte": "Ganso Vacuno", "sku_id": "000000000000029397-KG"},
    {"categoria": "Parrilleros", "corte": "Ganso Vacuno", "sku_id": "000000000000752517-KG"},
    {"categoria": "Parrilleros", "corte": "Ganso Vacuno", "sku_id": "000000000000028601-KGV"},
    {"categoria": "Parrilleros", "corte": "Huachalomo", "sku_id": "000000000000566022-KG"},
    {"categoria": "Parrilleros", "corte": "Huachalomo", "sku_id": "000000000000752520-KG"},
    {"categoria": "Parrilleros", "corte": "Huachalomo", "sku_id": "000000000000028602-KGV"},
    {"categoria": "Parrilleros", "corte": "Punta de Ganso", "sku_id": "000000000000752487-KG"},
    {"categoria": "Parrilleros", "corte": "Punta de Ganso", "sku_id": "000000000000400702-KGV"},
    {"categoria": "Parrilleros", "corte": "Punta Paleta", "sku_id": "000000000000400706-KGV"},
    {"categoria": "Parrilleros", "corte": "Punta Paleta", "sku_id": "000000000000752490-KG"},
    {"categoria": "Parrilleros", "corte": "Punta Picana", "sku_id": "000000000000400719-KGV"},
    {"categoria": "Parrilleros", "corte": "Punta Picana", "sku_id": "000000000000752493-KG"},
    {"categoria": "Parrilleros", "corte": "Sobrecostilla", "sku_id": "000000000000752497-KG"},
    {"categoria": "Parrilleros", "corte": "Sobrecostilla", "sku_id": "000000000000029402-KG"},
    {"categoria": "Parrilleros", "corte": "Sobrecostilla", "sku_id": "000000000000028614-KGV"},
    {"categoria": "Parrilleros", "corte": "Tapabarriga", "sku_id": "000000000000400725-KGV"},
    {"categoria": "Cacerola", "corte": "Posta Paleta", "sku_id": "000000000000029400-KG"},
    {"categoria": "Cacerola", "corte": "Posta Paleta", "sku_id": "000000000000028609-KGV"},
    {"categoria": "Cacerola", "corte": "Posta Negra", "sku_id": "000000000000028608-KGV"},
    {"categoria": "Cacerola", "corte": "Posta Negra", "sku_id": "000000000000029398-KG"},
    {"categoria": "Cacerola", "corte": "Posta Rosada", "sku_id": "000000000000028610-KGV"},
    {"categoria": "Cacerola", "corte": "Posta Negra", "sku_id": "000000000000752538-KG"},
    {"categoria": "Cacerola", "corte": "Posta Rosada", "sku_id": "000000000000752484-KG"},
    {"categoria": "Cacerola", "corte": "Posta Rosada", "sku_id": "000000000000029401-KG"},
    {"categoria": "Pollo", "corte": "Pechuga", "sku_id": "000000000000051806-KGV"},
    {"categoria": "Pollo", "corte": "Pechuga", "sku_id": "000000000000674766-KGV"},
    {"categoria": "Pollo", "corte": "Trutro", "sku_id": "000000000000051802-KGV"},
    {"categoria": "Pollo", "corte": "Trutro", "sku_id": "000000000000668742-KGV"},
]

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


def _procesar_y_enviar_alerta_carnes(**kwargs):
    print("🔍 Iniciando proceso de auditoría de disponibilidad de stock para SKUs de carnes...")
    
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

    # 2. Obtener la lista de SKUs a consultar
    df_skus_base = pd.DataFrame(SKUS_OBJETIVO)
    skus_ids = df_skus_base["sku_id"].unique().tolist()
    skus_in_clause = "'" + "', '".join(skus_ids) + "'"
    cortes_unicos = list(dict.fromkeys([item["corte"] for item in SKUS_OBJETIVO]))

    # 3. Consultar stock actual de la última carga en ecommdata.stock
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
    WHERE st.ref_id IN ({skus_in_clause});
    """
    df_stock = pg_hook.get_pandas_df(query_stock)
    print(f"📊 Registros de stock leídos: {len(df_stock)}")
    
    # Filtrar solo registros con stock positivo
    df_stock_pos = df_stock[df_stock["stock_janis"] > 0]

    # 4. Generar Matriz de Disponibilidad (Tiendas vs Cortes)
    df_stock_corte = pd.merge(df_stock_pos, df_skus_base[["sku_id", "corte"]], on="sku_id", how="inner")
    df_stock_corte["tiene_stock"] = 1
    
    if not df_stock_corte.empty:
        df_pivot = df_stock_corte.pivot_table(index="id_tienda", columns="corte", values="tiene_stock", aggfunc="max", fill_value=0).reset_index()
        df_pivot_stock = df_stock_corte.pivot_table(index="id_tienda", columns="corte", values="stock_janis", aggfunc="sum", fill_value=0).reset_index()
    else:
        df_pivot = pd.DataFrame(columns=["id_tienda"] + cortes_unicos)
        df_pivot_stock = pd.DataFrame(columns=["id_tienda"] + cortes_unicos)
    
    df_matrix = pd.merge(df_tiendas[["id_tienda", "nombre_tienda", "region_std"]], df_pivot, on="id_tienda", how="left")
    df_matrix_stock = pd.merge(df_tiendas[["id_tienda", "nombre_tienda", "region_std"]], df_pivot_stock, on="id_tienda", how="left")
    
    for c in cortes_unicos:
        if c not in df_matrix.columns:
            df_matrix[c] = 0
        if c not in df_matrix_stock.columns:
            df_matrix_stock[c] = 0
            
    df_matrix[cortes_unicos] = df_matrix[cortes_unicos].fillna(0).astype(int)
    df_matrix_stock[cortes_unicos] = df_matrix_stock[cortes_unicos].fillna(0).astype(int)

    # 5. Calcular promedios y totales por Región
    df_promedios = df_matrix.groupby("region_std")[cortes_unicos].mean().reset_index()
    df_totales_stock = df_matrix_stock.groupby("region_std")[cortes_unicos].sum().reset_index()

    # 6. Formatear las matrices para Excel (orden geográfico de Norte a Sur)
    final_rows = []
    final_rows_stock = []
    
    regiones_presentes = set(df_matrix["region_std"].unique())
    regiones_unicas = [r for r in ORDEN_GEOGRAFICO if r in regiones_presentes]
    # Si existen regiones no mapeadas en la lista, agregarlas al final
    regiones_no_mapeadas = sorted(list(regiones_presentes - set(regiones_unicas)))
    regiones_unicas.extend(regiones_no_mapeadas)

    for r in regiones_unicas:
        # ---- Matriz de Disponibilidad (%) ----
        df_r = df_matrix[df_matrix["region_std"] == r].sort_values("id_tienda").copy()
        for c in cortes_unicos:
            df_r[c] = df_r[c].apply(lambda x: "100%" if x == 1 else "0%")
        final_rows.append(df_r)
        
        row_promedio = df_promedios[df_promedios["region_std"] == r].iloc[0]
        dict_promedio = {
            "id_tienda": "PROMEDIO",
            "nombre_tienda": f"Promedio {r}",
            "region_std": r
        }
        for c in cortes_unicos:
            dict_promedio[c] = f"{round(row_promedio[c] * 100, 1)}%"
            
        df_prom_row = pd.DataFrame([dict_promedio])
        final_rows.append(df_prom_row)

        # ---- Matriz de Unidades (Stock) ----
        df_r_stock = df_matrix_stock[df_matrix_stock["region_std"] == r].sort_values("id_tienda").copy()
        final_rows_stock.append(df_r_stock)
        
        row_total = df_totales_stock[df_totales_stock["region_std"] == r].iloc[0]
        dict_total = {
            "id_tienda": "TOTAL",
            "nombre_tienda": f"Total {r}",
            "region_std": r
        }
        for c in cortes_unicos:
            dict_total[c] = int(row_total[c])
            
        df_tot_row = pd.DataFrame([dict_total])
        final_rows_stock.append(df_tot_row)

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
    file_name = f"alerta_disponibilidad_carnes_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    # 8. Notificación en Slack
    channel_var_name = "SLACK_DISPONIBILIDAD_CARNES_ALERT"
    channel_id = Variable.get(channel_var_name, default_var=None) or Variable.get("SLACK_PROMOTIONS_VIEWER_ALERT", default_var="C0BVBAHD7L0")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    # -------------------------------------------------------------
    # TRASPOSICIÓN DE LA MATRIZ Y ORDENAMIENTO GEOGRÁFICO
    # -------------------------------------------------------------
    df_prom_tabla = df_final_excel[df_final_excel["ID Tienda"] == "PROMEDIO"].copy()
    df_prom_tabla["Región"] = df_prom_tabla["Nombre Tienda"].str.replace("Promedio ", "", regex=False)

    # Asignar categoría ordenada para garantizar secuencia Norte -> Sur
    df_prom_tabla["Región"] = pd.Categorical(df_prom_tabla["Región"], categories=regiones_unicas, ordered=True)
    df_prom_tabla = df_prom_tabla.sort_values("Región")

    # Reestructurar: Index = Corte, Columnas = Regiones
    df_transpuesta = df_prom_tabla.set_index("Región")[cortes_unicos].T.reset_index()
    df_transpuesta = df_transpuesta.rename(columns={"index": "Corte"})

    regiones_list = [col for col in df_transpuesta.columns if col != "Corte"]
    
    intro = (
        f"<!channel> *🥩 Alerta Diaria Disponibilidad de Carnes por Región*\n"
        f"• *Tiendas Activas Evaluadas*: {len(df_tiendas)}\n"
        f"• *Cortes Monitoreados*: {len(cortes_unicos)}\n"
        f"• *Eje X*: Regiones (Norte a Sur) | *Eje Y*: Cortes (Subcategorías)\n"
    )

    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🥩 Reporte de Disponibilidad de Carnes",
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

    # -------------------------------------------------------------
    # CONSTRUCCIÓN DE TABLAS POR GRUPOS DE REGIONES (4 regiones por bloque)
    # -------------------------------------------------------------
    TAMANO_GRUPO_REGIONES = 4
    ancho_corte = max(len(c) for c in cortes_unicos) + 1
    ancho_col_region = 16

    for i in range(0, len(regiones_list), TAMANO_GRUPO_REGIONES):
        grupo_regiones = regiones_list[i:i + TAMANO_GRUPO_REGIONES]
        
        header_cols = [r[:ancho_col_region].ljust(ancho_col_region) for r in grupo_regiones]
        header = "Corte".ljust(ancho_corte) + " | " + " | ".join(header_cols)
        separador = "-" * len(header)
        
        lineas_bloque = [header, separador]
        
        for _, row in df_transpuesta.iterrows():
            corte_nombre = str(row["Corte"]).ljust(ancho_corte)
            valores_regiones = [str(row[reg]).ljust(ancho_col_region) for reg in grupo_regiones]
            fila = corte_nombre + " | " + " | ".join(valores_regiones)
            lineas_bloque.append(fila)
            
        tabla_texto_chunk = "\n".join(lineas_bloque)
        
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"```{tabla_texto_chunk}```"
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
            "text": f"Alerta Disponibilidad Carnes: Matriz de {len(cortes_unicos)} cortes en {len(df_tiendas)} tiendas."
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
                initial_comment=f"📊 Adjunto reporte de disponibilidad de carnes (Matriz por Tienda) en Excel ({file_name}).",
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
    "etl_alerta_disponibilidad_carnes",
    default_args=default_args,
    description="ETL de auditoría diaria de disponibilidad de stock de carnes por Región y subcategorías.",
    schedule_interval="30 7 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "stock", "carnes", "disponibilidad", "regiones", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría de Disponibilidad de Carnes por Regiones y Cortes
    Este DAG consulta diariamente la disponibilidad de stock en tiendas activas para los SKUs de carnes, genera una matriz binaria (100% o 0%) por Tienda y Corte (Subcategoría) agrupada por Región, y la envía a Slack junto a los promedios regionales.
    """

    task_auditar_disponibilidad = PythonOperator(
        task_id="auditar_disponibilidad_carnes_por_zona",
        python_callable=_procesar_y_enviar_alerta_carnes,
    )