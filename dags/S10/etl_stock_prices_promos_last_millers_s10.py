from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import timedelta

def _run_integration_cruce():
    """
    Ejecuta el Truncate y el Insert masivo (Mega-Query) que une:
    Tiendas (Activas) + Stock + Precios (Latest) + Promos.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    
    # 1. Limpieza absoluta de la tabla intermedia
    cursor.execute("TRUNCATE TABLE ecommdata_s10.tmp_stock_prices_promos_last_millers_s10;")
    print("Tabla temporal ecommdata_s10.tmp_stock... truncada correctamente.")
    
    # 2. El Corazón del Sistema: La Mega-Query (ESPEJO EXACTO M10)
    integration_query = """
    INSERT INTO ecommdata_s10.tmp_stock_prices_promos_last_millers_s10 (
        id_tienda, ean, material, unidad_de_medida, multiplicador_unidad, 
        nombre, marca, stock_unitario, precio, precio_promocional
    )
    SELECT  
        s.id_tienda,
        s2.ean_primario::varchar as ean,
        s.material, 
        s.umv_normalizada as unidad_de_medida, 
        s2.multiplicador_unidad_medida as multiplicador_unidad,
        s.descripcion_producto as nombre,
        m.nombre as marca,
        s.stock as stock_unitario,
        pm.precio as precio,
        w.promo as precio_promocional
    FROM (
        SELECT id_tienda, material, descripcion_producto, stock, bloqueos, 
               upper(trim(umv)) as umv_raw,
               CASE 
                 WHEN upper(trim(umv)) IN ('ST', 'UN') THEN 'UN'
                 WHEN upper(trim(umv)) IN ('CS', 'CJ', 'CJA') THEN 'CJ'
                 ELSE upper(trim(umv))
               END as umv_normalizada
        FROM ecommdata_s10.stock
        WHERE fecha_carga = (SELECT MAX(fecha_carga) FROM ecommdata_s10.stock)
    ) s
    INNER JOIN ecommdata_s10.tiendas t ON s.id_tienda = t.id_tienda AND t.last_millers_rappi = TRUE
    -- 1. Cruce con Precio Modal (Priority 09 + Norm UOM)
    LEFT JOIN (
        SELECT DISTINCT ON (LPAD(pm.codigo_material::varchar, 18, '0'), 
                           CASE 
                             WHEN upper(trim(pm.umv)) IN ('ST', 'UN') THEN 'UN'
                             WHEN upper(trim(pm.umv)) IN ('CS', 'CJ', 'CJA') THEN 'CJ'
                             ELSE upper(trim(pm.umv))
                           END)
            LPAD(pm.codigo_material::varchar, 18, '0') as material, 
            CASE 
              WHEN upper(trim(pm.umv)) IN ('ST', 'UN') THEN 'UN'
              WHEN upper(trim(pm.umv)) IN ('CS', 'CJ', 'CJA') THEN 'CJ'
              ELSE upper(trim(pm.umv))
            END as umv_norm, 
            pm.precio_modal as precio,
            formato_id
        FROM ecommdata_s10.precio_modal pm 
        ORDER BY 1, 2, (formato_id = '09') DESC, (formato_id = '02') DESC, id_semana DESC
    ) AS pm ON pm.material = s.material AND pm.umv_norm = s.umv_normalizada
    -- 2. Cruce con Promociones (Pixel-Perfect M10 + Robust UOM)
    LEFT JOIN (
        SELECT 
            LPAD(material::varchar(18), 18, '0') as material,
            CASE 
              WHEN upper(trim(un_medida_venta)) IN ('ST', 'UN') THEN 'UN'
              WHEN upper(trim(un_medida_venta)) IN ('CS', 'CJ', 'CJA') THEN 'CJ'
              ELSE upper(trim(un_medida_venta))
            END as umv_norm,
            MIN(precio_promocional) as promo
        FROM ecommdata_s10.workflow w
        WHERE fecha_inicio_de_promocion <= CURRENT_DATE + INTERVAL '1 day'
          AND fecha_fin_de_promocion >= CURRENT_DATE + INTERVAL '1 day'
          AND organizacion_ventas = '3000'
          AND desc_promocion IN ('PRECIO FIJO', '% DE DESCUENTO')
          AND (
               nombre_promocion LIKE '%CICLO%' OR 
               nombre_promocion LIKE '%PUNTA DE PRECIO%' OR 
               nombre_promocion LIKE '%PERECIBLES%' OR 
               nombre_promocion LIKE '%LOS ELE%' OR 
               nombre_promocion LIKE '%LAS 10 AL CHANCHO%'
          )
        GROUP BY 1, 2
    ) AS w ON w.material = s.material AND w.umv_norm = s.umv_normalizada
    -- 3. Cruce con Maestros Globales (Pixel-Perfect M10 hacia ecommdata + Robust UOM)
    LEFT JOIN ecommdata.skus s2 ON s2.ref_id = CONCAT(s.material, '-', s.umv_normalizada)
    LEFT JOIN ecommdata.productos p2 ON p2.ref_id = CONCAT(s.material, '-', s.umv_normalizada)
    LEFT JOIN ecommdata.marcas m ON p2.id_marca = m.id 
    WHERE s2.ean_primario IS NOT NULL
      AND pm.precio IS NOT NULL
      AND s.stock > 0
      AND s.bloqueos IS NOT TRUE
      AND m.nombre IS NOT NULL;
    """
    
    cursor.execute(integration_query)
    inserted_rows = cursor.rowcount
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    
    print(f"Cruce final S10 completado. Se generaron {inserted_rows} registros listos para Rappi.")

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_stock_prices_promos_last_millers_s10',
    default_args=default_args,
    description="Motor de Cruce Final (Cerebro) S10 - Prepara datos para Rappi",
    # Se ejecuta a las 06:30 AM para asegurar que los ETLs de la mañana terminaron
    schedule_interval="30 6 * * *",
    start_date=pendulum.datetime(2024, 6, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["S10", "integracion", "rappi", "ecommerce", "cruce", "last-millers", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Este DAG es el **núcleo de integración S10** (Equivalente al lm_stock_precio_promo_10 de M10).
    
    ### Funciones:
    1. **Independencia Total:** Opera exclusivamente sobre el esquema `ecommdata_s10`.
    2. **Corrección de Precios:** Utiliza la lógica `DISTINCT ON + ORDER BY DESC` para garantizar que enviamos el precio de la última semana y no el máximo histórico.
    3. **Filtro de Tiendas:** Solo procesa locales donde `last_millers_rappi = TRUE`.
    4. **Output:** Tabla `ecommdata_s10.tmp_stock_prices_promos_last_millers_s10`.
    """ 
    
    t_cruce = PythonOperator(
        task_id = "run_integration_cruce",
        python_callable = _run_integration_cruce
    )
    
    t_cruce
