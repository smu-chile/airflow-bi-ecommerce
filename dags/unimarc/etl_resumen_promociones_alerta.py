from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta
import pendulum
import requests

def truncate_and_load_resumen():
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    query = """
    TRUNCATE TABLE ecommdata.resumen_promociones_activas;
    
    INSERT INTO ecommdata.resumen_promociones_activas (
        n_promocion, nombre_promocion, DESC_PROMOCION, material, nombre_producto, 
        vtex_id, precio_modal, precio_promocional, precio_total_promocional, 
        cantidad_n, cantidad_m, porcentaje_descuento_final, cantidad_tiendas_activas, 
        fecha_inicio_de_promocion, fecha_fin_de_promocion, fecha_actualizacion, 
        nombre_promocion_vtex, nombre_lista_precio, nombre_coleccion, sku
    )
    WITH TiendasActivas AS (
        SELECT id::int AS id_tienda_int FROM ecommdata.tiendas WHERE status = 1
    ), Lista8Tiendas AS (
        SELECT (l8.material::text || '-'::text || l8.umv::text) AS ref_id, COUNT(DISTINCT l8.id_tienda) AS cantidad_tiendas_activas
        FROM ecommdata.lista8 l8 INNER JOIN TiendasActivas t ON l8.id_tienda::int = t.id_tienda_int GROUP BY 1
    ), PromocionesUnicas AS (
        SELECT DISTINCT wp.n_promocion, wp.nombre_promocion, wp.DESC_PROMOCION, wp.material, wp.descripcion_material AS nombre_producto, wp.precio_modal, wp.precio_promocional, wp.precio_total_promocional, wp.cantidad_n, wp.cantidad_m, wp.porcentaje_n, wp.porcentaje_de_descuento, wp.marca, wp.llevas_n, wp.umv, wp.tipo_promocion, wp.fecha_inicio_de_promocion, wp.fecha_fin_de_promocion, s.vtex_id, s.multiplicador_unidad_medida, l8.cantidad_tiendas_activas
        FROM ecommdata.workflow_promociones wp
        LEFT JOIN ecommdata.skus s ON s.ref_id::text = ((wp.material::text || '-'::text) || CASE WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying ELSE wp.umv END::text)
        INNER JOIN Lista8Tiendas l8 ON l8.ref_id = ((wp.material::text || '-'::text) || CASE WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying ELSE wp.umv END::text)
        WHERE wp.precio_modal IS NOT NULL AND wp.precio_promocional IS NOT NULL AND wp.precio_modal > 0
        AND (wp.id_mecanica <> ALL (ARRAY [124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59,50]))
        AND wp.fecha_inicio_de_promocion <= current_date + 1 AND wp.fecha_fin_de_promocion >= current_date
        AND wp.tipo_promocion <> 3
        AND wp.n_promocion NOT IN (5720882025, 5552152024, 4040162024, 5552792024, 5552852024, 4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 1120012025, 4000182025, 1120232025, 1120232025)
        AND wp.nombre_promocion::text !~~ '%ZONA%'::text AND wp.nombre_promocion::text !~~ '%MFC%'::text AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text AND wp.nombre_promocion::text !~~ '%917%'::text AND wp.nombre_promocion::text !~~ '%ESTADO%'::text AND wp.nombre_promocion::text !~~ '%LOC%'::text AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})' AND wp.nombre_promocion::text !~~ '%HUACHALALUME%'::text AND wp.nombre_promocion::text !~~ '%LOCAL%'::text AND wp.nombre_promocion::text !~~ '%MEMB%'::text AND wp.nombre_promocion::text !~~ '%CYBER%'::text AND wp.nombre_promocion::text !~~ '%CUMPLEANOS%'::text AND wp.nombre_promocion::text !~~ '%BLACK%'::text
        AND NOT (wp.n_promocion = 4000162026 AND wp.material IN ('000000000649078001', '000000000649078002', '000000000000668408', '000000000000668409', '000000000000668410', '000000000000655713', '000000000000655712', '000000000000655576', '000000000000655577', '000000000000647306'))
        AND s.vtex_id <> ALL (ARRAY [3610,471,3611,472,473,658,82183,82184,39730])
    ), PromocionesConMetadata AS (
        SELECT *, COUNT(vtex_id) OVER (PARTITION BY n_promocion) AS vtex_id_count,
        CASE WHEN tipo_promocion = ANY(ARRAY[1,4]) THEN 'regular' WHEN tipo_promocion = ANY(ARRAY[8,7,2]) THEN 'forThePriceOf' ELSE 'error' END AS mecanica,
        CASE WHEN tipo_promocion = 4 AND umv::text NOT IN ('KG', 'KGV') THEN 'lista-precio' ELSE 'no' END AS es_lista_precio,
        ((n_promocion || ' ') || regexp_replace(nombre_promocion::text, '[^a-zA-Z0-9]', '', 'g') || '_') || CASE WHEN tipo_promocion = 7 THEN marca::text || '_' || cantidad_n::text || 'x' || round(precio_total_promocional, 0) || '$' WHEN tipo_promocion = 1 THEN ((porcentaje_de_descuento * 100)::int)::text || '%' WHEN tipo_promocion = 4 THEN CASE WHEN umv::text IN ('KG', 'KGV') THEN '_PESABLE_' || round(precio_promocional * multiplicador_unidad_medida, 0) ELSE 'LISTA_PRECIOS' END WHEN tipo_promocion = 2 THEN marca::text || '_' || cantidad_n::text || 'x' || cantidad_m::text WHEN tipo_promocion = 8 AND llevas_n = 2::numeric THEN marca::text || '_' || ((porcentaje_de_descuento * 100)::int)::text || '_2DA_UN' ELSE 'FALTA_COD_MEC' END AS base_name_prom,
        CASE WHEN tipo_promocion = 2 AND cantidad_n > 0 THEN ROUND((1.0 - (cantidad_m::numeric / cantidad_n::numeric)) * 100) WHEN tipo_promocion = 7 AND cantidad_n > 0 AND precio_modal > 0 THEN ROUND((1.0 - ((precio_total_promocional::numeric / cantidad_n::numeric) / precio_modal::numeric)) * 100) WHEN tipo_promocion = 8 THEN ROUND((porcentaje_n::numeric * 100) / 2.0) WHEN tipo_promocion = 1 THEN ROUND(porcentaje_de_descuento::numeric * 100) WHEN precio_modal > 0 THEN ROUND((1.0 - (precio_promocional::numeric / precio_modal::numeric)) * 100) ELSE 0 END::int AS porcentaje_descuento_final
        FROM PromocionesUnicas
    )
    SELECT n_promocion, nombre_promocion, DESC_PROMOCION, material, nombre_producto, CASE WHEN vtex_id IS NULL THEN 'SIN_VTEX_ID' ELSE vtex_id::text END AS vtex_id, precio_modal, precio_promocional, precio_total_promocional, cantidad_n, cantidad_m, porcentaje_descuento_final, cantidad_tiendas_activas, fecha_inicio_de_promocion, fecha_fin_de_promocion, timezone('America/Santiago', now()) AS fecha_actualizacion,
    CASE WHEN mecanica = 'forThePriceOf' AND vtex_id_count > 1 THEN base_name_prom || '_CLP1' WHEN mecanica = 'forThePriceOf' AND vtex_id_count <= 1 THEN base_name_prom || '_CLP0' ELSE base_name_prom END AS nombre_promocion_vtex,
    CASE WHEN es_lista_precio = 'lista-precio' THEN regexp_replace(nombre_promocion::text, '[^a-zA-Z0-9]', '', 'g') ELSE 'NO_APLICA' END AS nombre_lista_precio,
    CASE WHEN tipo_promocion = 1 AND vtex_id_count > 200 THEN 'COLLECTION ' || nombre_promocion ELSE 'NO_APLICA' END AS nombre_coleccion,
    (material || '-' || CASE WHEN umv::text = 'ST' THEN 'UN' WHEN umv::text = 'CS' THEN 'CJ' ELSE umv::text END) AS sku
    FROM PromocionesConMetadata
    WHERE cantidad_tiendas_activas > 0;
    """
    
    print("Executing truncate and load query...")
    cursor.execute(query)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Truncate and load finished successfully.")

def check_and_notify_slack():
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    query_alert = """
    SELECT 
        n_promocion, 
        material, 
        nombre_producto, 
        precio_modal, 
        precio_promocional, 
        precio_total_promocional,
        cantidad_n,
        cantidad_m,
        porcentaje_descuento_final,
        nombre_promocion_vtex,
        nombre_lista_precio,
        nombre_coleccion,
        DESC_PROMOCION,
        TO_CHAR(fecha_inicio_de_promocion, 'DD/MM/YYYY'),
        TO_CHAR(fecha_fin_de_promocion, 'DD/MM/YYYY'),
        cantidad_tiendas_activas,
        sku
    FROM ecommdata.resumen_promociones_activas r
    WHERE porcentaje_descuento_final > 75 
    AND NOT EXISTS (
        SELECT 1 FROM ecommdata.excepciones_alertas_promociones exc 
        WHERE exc.sku = r.sku AND exc.nombre_promocion = r.nombre_promocion
    )
    ORDER BY porcentaje_descuento_final DESC;
    """
    
    cursor.execute(query_alert)
    rows = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    
    total_anomalies = len(rows)
    if total_anomalies == 0:
        print("No anomalies detected. Skipping Slack notification.")
        return
    
    # Restringimos a Top 20 para ser ultra-seguros con los límites de caracteres de Slack 
    # y para que la alerta sea fácil de leer en dispositivos móviles sin hacer scroll infinito.
    top_cases = rows[:20]
    
    texto_intro = f"<!channel> Se han detectado *{total_anomalies} productos* activos en e-commerce con un descuento real superior al 75%."
    if total_anomalies > 20:
        texto_intro += " (Mostrando el Top 20 más crítico)."
    texto_intro += " Estos no serán cargados en Vtex, favor, solicitar corregir."
    
    slack_blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "🚨 Alerta de Promociones: Descuentos > 75% Detectados",
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
        }
    ]
    
    for row in top_cases:
        n_prom = row[0]
        material_row = row[1]
        nom_prod = row[2]
        p_modal = row[3]
        p_promo = row[4]
        p_total = row[5]
        cant_n = row[6]
        cant_m = row[7]
        descuento = row[8]
        nom_vtex = row[9]
        nom_lista = row[10]
        nom_col = row[11]
        desc_promocion = row[12]
        f_inicio = row[13]
        f_fin = row[14]
        cant_tiendas = row[15]
        sku_val = row[16]
        
        # Formateo dinámico del título según mecánica
        if "NX$" in desc_promocion or "NX" in desc_promocion or "LLEVA" in desc_promocion:
            if p_total and p_total > 0:
                titulo = f"*{nom_prod} {int(cant_n)} x ${int(p_total)}*"
                promo_uni = p_total / cant_n if cant_n > 0 else 0
            else:
                titulo = f"*{nom_prod} {int(cant_n)} x {int(cant_m)}*"
                promo_uni = 0
        else:
            titulo = f"*{nom_prod} ${int(p_promo)} ({descuento}%)*"
            promo_uni = p_promo
            
        modal_str = f"${int(p_modal)}" if p_modal else "N/A"
        promo_uni_str = f"${int(promo_uni)}" if promo_uni > 0 else "N/A"
        
        texto_detalle = f"🛍️ {titulo}\n• *SKU:* `{sku_val}`\n• *Descuento Real:* {descuento}% _(Modal: {modal_str} ➡️ Promo Uni: {promo_uni_str})_\n• *Vigencia:* `{f_inicio}` al `{f_fin}`\n• *Tiendas Afectadas:* {cant_tiendas}\n• *Nombre VTEX:* `{nom_vtex}`\n• *Mecánica:* {desc_promocion}"
        
        if nom_col != "NO_APLICA":
            texto_detalle += f"\n• *Colección:* `{nom_col}`"
        elif nom_lista != "NO_APLICA":
            texto_detalle += f"\n• *Lista Precio:* `{nom_lista}`"
            
        slack_blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": texto_detalle
            }
        })
        
    slack_blocks.append({"type": "divider"})
    slack_blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "💡 _Consulta `ecommdata.resumen_promociones_activas` para el detalle de todos los casos._"
            }
        ]
    })
    
    slack_token = Variable.get("SLACK_UNITRACK_TOKEN")
    channel_id = Variable.get("SLACK_PROMOTION_ALERT_CHANNEL")
    
    headers = {
        "Authorization": f"Bearer {slack_token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    
    payload = {
        "channel": channel_id,
        "blocks": slack_blocks,
        "text": f"Alerta de Promociones: {total_anomalies} descuentos excesivos detectados."
    }
    
    print("Sending Slack notification...")
    res = requests.post("https://slack.com/api/chat.postMessage", headers=headers, json=payload)
    if not res.ok or not res.json().get("ok"):
        print(f"Error sending Slack notification: {res.text}")
    else:
        print("Slack notification sent successfully.")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    'etl_resumen_promociones_alerta',
    default_args=default_args,
    description="Carga tabla resumen de promociones y alerta descuentos excesivos",
    schedule_interval="0 8,12,16 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["DATA", "ecommdata", "promociones", "alertas", "Unimarc", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    trigger_workflow_promotions = TriggerDagRunOperator(
        task_id="trigger_workflow_promotions",
        trigger_dag_id="workflow_promotions_table_incremental_load",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    truncate_and_load_task = PythonOperator(
        task_id="truncate_and_load_resumen",
        python_callable=truncate_and_load_resumen
    )

    check_and_notify_task = PythonOperator(
        task_id="check_and_notify_slack",
        python_callable=check_and_notify_slack
    )

    trigger_workflow_promotions >> truncate_and_load_task >> check_and_notify_task
