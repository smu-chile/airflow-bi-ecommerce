from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack
from utils.bigquery_utils import bq_query_to_df
from datetime import datetime
import pendulum
import io
import json
import pandas as pd
import requests

def _validar_promociones_en_bigquery(promociones_list):
    """
    Valida la vigencia de un conjunto de n_promocion en BigQuery en la vista
    cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW con los parámetros:
      - FECHA_INICIO_DE_PROMOCION <= CURRENT_DATE() + 3
      - FECHA_FIN_DE_PROMOCION >= CURRENT_DATE()
      - ORGANIZACION_VENTAS = '1000'
      - REGISTRO_VALIDO = 'X'
    Retorna un DataFrame con las promociones que SÍ existen/están vigentes en BigQuery.
    """
    if not promociones_list:
        return pd.DataFrame()

    promos_clean = [int(p) for p in set(promociones_list) if pd.notnull(p)]
    if not promos_clean:
        return pd.DataFrame()

    promos_str = ",".join(str(p) for p in promos_clean)

    query = f"""
        SELECT DISTINCT 
            CAST(MATERIAL AS STRING) AS material,
            CAST(N_PROMOCION AS INT64) AS n_promocion,
            REGISTRO_VALIDO
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW`
        WHERE FECHA_INICIO_DE_PROMOCION <= DATE_ADD(CURRENT_DATE(), INTERVAL 3 DAY)
          AND FECHA_FIN_DE_PROMOCION >= CURRENT_DATE()
          AND ORGANIZACION_VENTAS = '1000'
          AND REGISTRO_VALIDO = 'X'
          AND N_PROMOCION IN ({promos_str})
    """
    print(f"🔍 Consultando BigQuery para validar {len(promos_clean)} promociones...")
    try:
        df_bq = bq_query_to_df(query)
        print(f"📊 BigQuery retornó {len(df_bq)} promociones/SKUs vigentes.")
        return df_bq
    except Exception as e:
        print(f"⚠️ Error al consultar BigQuery para validación de promociones: {e}")
        return pd.DataFrame()


def _auditar_y_registrar_precios_fijos_retirados(**kwargs):
    print("🔍 Iniciando auditoría de precios fijos retirados prematuramente...")
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    
    # 1. Crear tablas si no existen
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ecommdata.wp_precios_fijos_vigentes (
        material VARCHAR(50),
        umv VARCHAR(20),
        ref_id VARCHAR(50),
        n_promocion INT8,
        nombre_promocion VARCHAR(255),
        fecha_inicio_de_promocion TIMESTAMP,
        fecha_fin_de_promocion TIMESTAMP,
        fecha_actualizacion TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (material, umv, n_promocion)
    );
    
    CREATE TABLE IF NOT EXISTS ecommdata.wp_skus_a_borrar (
        id SERIAL PRIMARY KEY,
        fecha_deteccion TIMESTAMP DEFAULT NOW(),
        ref_id VARCHAR(50),
        material VARCHAR(50),
        vtex_id VARCHAR(50),
        nombre_producto VARCHAR(255),
        n_promocion INT8,
        nombre_promocion VARCHAR(255),
        nombre_lista_precio VARCHAR(100),
        precio_vtex NUMERIC,
        fecha_inicio_promocion TIMESTAMP,
        fecha_fin_promocion TIMESTAMP,
        observacion TEXT
    );
    """)
    pg_conn.commit()

    # 2. Consultar si existen registros en la foto anterior
    cursor.execute("SELECT COUNT(*) FROM ecommdata.wp_precios_fijos_vigentes;")
    count_prev = cursor.fetchone()[0]

    if count_prev == 0:
        print("ℹ️ Primera ejecución: Inicializando foto previa de precios fijos desde ecommdata.workflow_promociones...")
        init_sql = """
        INSERT INTO ecommdata.wp_precios_fijos_vigentes (
            material, umv, ref_id, n_promocion, nombre_promocion, 
            fecha_inicio_de_promocion, fecha_fin_de_promocion, fecha_actualizacion
        )
        SELECT DISTINCT 
            wp.material, 
            wp.umv, 
            ((wp.material || '-') || CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' ELSE wp.umv END) AS ref_id,
            wp.n_promocion, 
            wp.nombre_promocion,
            wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion,
            NOW()
        FROM ecommdata.workflow_promociones wp
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND wp.tipo_promocion = 4
        ON CONFLICT (material, umv, n_promocion) 
        DO UPDATE SET 
            nombre_promocion = EXCLUDED.nombre_promocion,
            fecha_inicio_de_promocion = EXCLUDED.fecha_inicio_de_promocion,
            fecha_fin_de_promocion = EXCLUDED.fecha_fin_de_promocion,
            fecha_actualizacion = NOW();
        """
        cursor.execute(init_sql)
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        print("✅ Foto previa inicializada correctamente. Se iniciará la auditoría en la siguiente corrida.")
        return

    # 3. Consulta de auditoría: Compara foto anterior vs workflow_promociones actual
    audit_sql = """
    WITH foto_anterior AS (
        SELECT 
            prev.material, 
            prev.umv, 
            prev.ref_id,
            prev.n_promocion, 
            prev.nombre_promocion,
            prev.fecha_inicio_de_promocion,
            prev.fecha_fin_de_promocion
        FROM ecommdata.wp_precios_fijos_vigentes prev
        WHERE prev.fecha_fin_de_promocion >= CURRENT_DATE
    ),
    foto_actual AS (
        SELECT DISTINCT 
            wp.material, 
            wp.umv, 
            wp.n_promocion
        FROM ecommdata.workflow_promociones wp
        WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
          AND wp.fecha_fin_de_promocion >= CURRENT_DATE
          AND wp.tipo_promocion = 4
    )
    SELECT DISTINCT
        p.material,
        p.umv,
        p.ref_id,
        p.n_promocion,
        p.nombre_promocion,
        p.fecha_inicio_de_promocion,
        p.fecha_fin_de_promocion,
        s.vtex_id,
        s.nombre_sku AS nombre_producto
    FROM foto_anterior p
    LEFT JOIN foto_actual c 
           ON p.material = c.material 
          AND p.umv = c.umv 
          AND p.n_promocion = c.n_promocion
    LEFT JOIN ecommdata.skus s ON s.ref_id = p.ref_id
    WHERE c.material IS NULL;
    """

    df_retirados = pg_hook.get_pandas_df(audit_sql)

    if df_retirados.empty:
        print("✅ No se detectaron precios fijos retirados prematuramente en workflow_promociones.")
    else:
        print(f"🚨 Se detectaron {len(df_retirados)} SKUs con precio fijo retirado prematuramente. Validando en BigQuery y VTEX...")

        # 3.1 Consultar BigQuery para validar si la promoción sigue activa en la vista maestro
        promos_afectadas = df_retirados["n_promocion"].tolist()
        df_bq_vigentes = _validar_promociones_en_bigquery(promos_afectadas)

        bq_set = set()
        if not df_bq_vigentes.empty and "material" in df_bq_vigentes.columns and "n_promocion" in df_bq_vigentes.columns:
            for _, bq_row in df_bq_vigentes.iterrows():
                mat_str = str(bq_row["material"]).strip().lstrip("0")
                p_id = int(bq_row["n_promocion"])
                bq_set.add((mat_str, p_id))

        account_name = Variable.get("VTEX_ACCOUNT_NAME", default_var="unimarc")
        vtex_key = Variable.get("X_VTEX_API_AppKey", default_var=None)
        vtex_token = Variable.get("X_VTEX_API_AppToken", default_var=None)
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-VTEX-API-AppKey": vtex_key,
            "X-VTEX-API-AppToken": vtex_token
        }

        registros_a_insertar = []

        for _, row in df_retirados.iterrows():
            vtex_id = str(row["vtex_id"]) if pd.notnull(row["vtex_id"]) else None
            nombre_prom = str(row["nombre_promocion"]) if pd.notnull(row["nombre_promocion"]) else ""
            nombre_lista_precio = nombre_prom.replace(" ", "").replace(",", "").replace(".", "")
            precio_vtex = None
            vtex_obs = ""

            # Validar si existe en BigQuery
            mat_clean = str(row["material"]).strip().lstrip("0") if pd.notnull(row["material"]) else ""
            p_id = int(row["n_promocion"]) if pd.notnull(row["n_promocion"]) else 0

            if (mat_clean, p_id) in bq_set or (str(row["material"]), p_id) in bq_set:
                bq_obs = "BQ: Promo SIGUE ACTIVA en BigQuery (desfase Postgres)"
            else:
                bq_obs = "BQ: Promo dada de baja confirmada en BigQuery"

            if vtex_id and vtex_id != "None" and vtex_key and vtex_token:
                try:
                    url = f"https://api.vtex.com/{account_name}/pricing/prices/{vtex_id}"
                    resp = requests.get(url, headers=headers, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        fixed_prices = data.get("fixedPrices", [])
                        matched_price = None
                        for fp in fixed_prices:
                            trade_policy = str(fp.get("tradePolicyId", ""))
                            if trade_policy.lower() == nombre_lista_precio.lower() or trade_policy in nombre_lista_precio:
                                matched_price = fp.get("value")
                                break
                        
                        if matched_price is not None:
                            precio_vtex = matched_price
                            vtex_obs = "Precio encontrado en VTEX"
                        else:
                            precio_vtex = data.get("basePrice") or data.get("listPrice")
                            vtex_obs = f"Lista '{nombre_lista_precio}' no hallada directamente; retornado precio base/lista de VTEX"
                    else:
                        vtex_obs = f"Error GET API VTEX ({resp.status_code})"
                except Exception as ex:
                    vtex_obs = f"Excepción API VTEX: {str(ex)}"
            else:
                vtex_obs = "Sin VTEX ID o credenciales de API omitidas"

            observacion = f"{bq_obs} | {vtex_obs}"

            registros_a_insertar.append((
                row["ref_id"],
                row["material"],
                vtex_id,
                row["nombre_producto"],
                row["n_promocion"],
                row["nombre_promocion"],
                nombre_lista_precio,
                precio_vtex,
                row["fecha_inicio_de_promocion"],
                row["fecha_fin_de_promocion"],
                observacion
            ))

        # Insertar hallazgos en ecommdata.wp_skus_a_borrar
        insert_sql = """
        INSERT INTO ecommdata.wp_skus_a_borrar (
            ref_id, material, vtex_id, nombre_producto, n_promocion,
            nombre_promocion, nombre_lista_precio, precio_vtex,
            fecha_inicio_promocion, fecha_fin_promocion, observacion
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor.executemany(insert_sql, registros_a_insertar)
        pg_conn.commit()
        print(f"💾 {len(registros_a_insertar)} registros guardados exitosamente en ecommdata.wp_skus_a_borrar.")

        # Notificar a Slack con reporte adjunto
        _notificar_slack(df_retirados, registros_a_insertar)

    # 4. Actualizar la foto liviana para la corrida de mañana
    print("🔄 Refrescando foto previa ecommdata.wp_precios_fijos_vigentes para mañana...")
    refresco_sql = """
    TRUNCATE TABLE ecommdata.wp_precios_fijos_vigentes;
    
    INSERT INTO ecommdata.wp_precios_fijos_vigentes (
        material, umv, ref_id, n_promocion, nombre_promocion, 
        fecha_inicio_de_promocion, fecha_fin_de_promocion, fecha_actualizacion
    )
    SELECT DISTINCT 
        wp.material, 
        wp.umv, 
        ((wp.material || '-') || CASE WHEN wp.umv = 'ST' THEN 'UN' WHEN wp.umv = 'CS' THEN 'CJ' ELSE wp.umv END) AS ref_id,
        wp.n_promocion, 
        wp.nombre_promocion,
        wp.fecha_inicio_de_promocion,
        wp.fecha_fin_de_promocion,
        NOW()
    FROM ecommdata.workflow_promociones wp
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      AND wp.tipo_promocion = 4;
    """
    cursor.execute(refresco_sql)
    pg_conn.commit()
    cursor.close()
    pg_conn.close()
    print("✅ Foto previa actualizada exitosamente.")


def _notificar_slack(df_retirados, registros_a_insertar):
    total = len(df_retirados)
    channel_var_name = "SLACK_PROMOTIONS_VIEWER_ALERT"
    channel_id = Variable.get(channel_var_name, default_var="C0BVBAHD7L0")
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN", default_var=None) or Variable.get("token_slack_bot", default_var=None)

    if not slack_token:
        print("⚠️ Token de Slack no configurado. Omitiendo notificación.")
        return

    top_casos = df_retirados.head(10)
    
    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Alerta: Precios Fijos Retirados de Workflow",
                "emoji": True
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"<!channel> Se han detectado *{total} SKUs* con precios fijos vigentes que fueron *retirados de workflow_promociones antes de su vencimiento*.\nLos registros han sido guardados en `ecommdata.wp_skus_a_borrar`."
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*📌 Muestra de SKUs Afectados:*"
            }
        }
    ]

    for _, row in top_casos.iterrows():
        prod_name = row['nombre_producto'] or 'Desconocido'
        vtex_str = row['vtex_id'] or 'N/A'
        obs_str = ""
        for reg in registros_a_insertar:
            if reg[0] == row['ref_id'] and reg[4] == row['n_promocion']:
                obs_str = reg[10]
                break

        item_text = (
            f"🛍️ *{prod_name}* (Ref: `{row['ref_id']}` | VTEX ID: `{vtex_str}`)\n"
            f"• *Promo:* `{row['n_promocion']}` - {row['nombre_promocion']}\n"
            f"• *Vencimiento:* `{row['fecha_fin_de_promocion']}`\n"
            f"• *Detalle:* _{obs_str}_"
        )
        slack_blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": item_text}
        })

    slack_blocks.append({"type": "divider"})
    slack_blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "💡 _Consulta la tabla `ecommdata.wp_skus_a_borrar` en PostgreSQL para más detalles._"}]
    })

    headers = {
        "Authorization": f"Bearer {slack_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    payload = {
        "channel": channel_id,
        "blocks": slack_blocks,
        "text": f"Alerta Precios Fijos Retirados: {total} SKUs registrados en ecommdata.wp_skus_a_borrar."
    }
    res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
    if not res.ok or not res.json().get("ok"):
        print(f"⚠️ Error enviando notificación a Slack: {res.text}")
    else:
        print(" Notificación enviada a Slack.")

    # Generar CSV para adjuntar
    df_reporte = pd.DataFrame(registros_a_insertar, columns=[
        "ref_id", "material", "vtex_id", "nombre_producto", "n_promocion",
        "nombre_promocion", "nombre_lista_precio", "precio_vtex",
        "fecha_inicio_promocion", "fecha_fin_promocion", "observacion"
    ])
    csv_buffer = io.BytesIO()
    df_reporte.to_csv(csv_buffer, index=False, encoding="utf-8")
    csv_bytes = csv_buffer.getvalue()
    file_name = f"wp_skus_a_borrar_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

    try:
        _upload_csv_file_to_slack(
            file_name=file_name,
            data_bytes=csv_bytes,
            channel_id=channel_id,
            token=slack_token,
            initial_comment=f"📄 Reporte completo de {total} SKUs registrados en `ecommdata.wp_skus_a_borrar`."
        )
        print(" Reporte CSV enviado a Slack.")
    except Exception as e:
        print(f"⚠️ No se pudo adjuntar el CSV en Slack: {e}")


def _upload_csv_file_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str, initial_comment: str = ""):
    upload_url_resp = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={"filename": file_name, "length": str(len(data_bytes)), "token": token},
    ).json()
    
    upload_url = upload_url_resp.get("upload_url")
    file_id = upload_url_resp.get("file_id")
    if not upload_url:
        raise RuntimeError(f"Error en files.getUploadURLExternal: {upload_url_resp}")

    up_resp = requests.post(upload_url, data=data_bytes, headers={"Content-Type": "application/octet-stream"})
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo bytes de {file_name}: {up_resp.text}")

    comp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        data=json.dumps({"files": [{"id": file_id}], "channel_id": channel_id, "initial_comment": initial_comment}),
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
    "etl_alerta_borrado_precios_fijos",
    default_args=default_args,
    description="Auditoría y registro en ecommdata.wp_skus_a_borrar de SKUs con precios fijos retirados prematuramente de workflow_promociones.",
    schedule_interval="45 8 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "promociones", "precios_fijos", "vtex", "slack", "unimarc"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    ### Auditoría y Registro de Precios Fijos Retirados
    Este DAG se ejecuta diariamente a las 8:45 AM (Chile) para auditar si algún producto con precio fijo vigente fue retirado de `workflow_promociones` antes de vencer.
    Consulta la API GET de VTEX para rescatar su precio actual y guarda el detalle completo en la tabla `ecommdata.wp_skus_a_borrar`, emitiendo además una alerta a Slack.
    """

    task_auditar_y_registrar = PythonOperator(
        task_id="auditar_y_registrar_precios_fijos_retirados",
        python_callable=_auditar_y_registrar_precios_fijos_retirados,
    )
