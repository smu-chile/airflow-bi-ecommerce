CREATE TABLE IF NOT EXISTS ecommdata.alerta_top500_stock_janis (
    id SERIAL PRIMARY KEY,
    fecha_ejecucion TIMESTAMP,
    ranking INT,
    ref_id_sku VARCHAR(50),
    material VARCHAR(50),
    umv VARCHAR(10),
    vtex_id VARCHAR(50),
    nombre_sku VARCHAR(255),
    id_tienda VARCHAR(20),
    nombre_tienda VARCHAR(255),
    stock_janis NUMERIC,
    es_quiebre_janis BOOLEAN,
    tiene_promocion_activa BOOLEAN,
    cantidad_promociones_activas INT,
    detalle_promociones_activas TEXT,
    promedio_venta_diaria_unidades_90d NUMERIC,
    promedio_venta_diaria_pesos_90d NUMERIC,
    fecha_modificacion_janis TIMESTAMP
);

DELETE FROM ecommdata.alerta_top500_stock_janis
WHERE fecha_ejecucion::date = CURRENT_DATE;

INSERT INTO ecommdata.alerta_top500_stock_janis (
    fecha_ejecucion,
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
)
WITH TiendasObjetivo AS (
    SELECT DISTINCT ON (LPAD(t.id::text, 4, '0'))
        LPAD(t.id::text, 4, '0') AS id_tienda,
        COALESCE(t.nombre_tienda, t.glosa, t.id::text) AS nombre_tienda
    FROM ecommdata.tiendas t
    WHERE t.status = 1
      AND LPAD(t.id::text, 4, '0') IN ('0581', '0917')
    ORDER BY LPAD(t.id::text, 4, '0') ASC
),
Top500 AS (
    SELECT DISTINCT ON (rp.ref_id_sku)
        rp.ranking,
        rp.ref_id_sku,
        s.vtex_id::text AS vtex_id,
        COALESCE(rp.nombre_sku, s.nombre_sku) AS nombre_sku,
        SPLIT_PART(rp.ref_id_sku, '-', 1) AS material,
        SPLIT_PART(rp.ref_id_sku, '-', 2) AS umv
    FROM ecommdata.ranking_productos rp
    LEFT JOIN ecommdata.skus s ON s.ref_id = rp.ref_id_sku
    WHERE rp.ranking <= 500
    ORDER BY rp.ref_id_sku, rp.ranking ASC
),
PromocionesActivas AS (
    SELECT 
        (wp.material::text || '-' || CASE 
            WHEN TRIM(wp.umv::text) = 'ST' THEN 'UN'
            WHEN TRIM(wp.umv::text) = 'CS' THEN 'CJ'
            ELSE TRIM(wp.umv::text)
        END) AS ref_id_sku,
        COUNT(DISTINCT wp.n_promocion) AS cantidad_promociones_activas,
        STRING_AGG(DISTINCT (wp.n_promocion::text || ': ' || wp.nombre_promocion), ' | ') AS detalle_promociones_activas
    FROM ecommdata.workflow_promociones wp
    JOIN Top500 t ON wp.material::text = t.material OR wp.material::text = LTRIM(t.material, '0')
    WHERE CURRENT_DATE BETWEEN wp.fecha_inicio_de_promocion AND wp.fecha_fin_de_promocion
      AND (wp.id_mecanica IS NULL OR wp.id_mecanica <> ALL (ARRAY [124, 36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59, 50]))
      AND wp.tipo_promocion <> 3
      AND wp.n_promocion NOT IN (
          5720882025, 5552152024, 4040162024, 5552792024, 5552852024, 
          4060322024, 5553242024, 1120042025, 1120032025, 1120022025, 
          1120012025, 4000952026, 4000182025, 4000602026, 4000652026, 
          1120232025, 5551272026, 5510102026, 1020032026
      )
      AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
      AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
      AND wp.nombre_promocion::text NOT ILIKE '%UNIPAY%'
      AND wp.nombre_promocion::text NOT ILIKE '%CYBER%'
      AND wp.nombre_promocion::text NOT ILIKE '%BLACK%'
    GROUP BY 1
),
Ventas90Dias AS (
    SELECT
        LPAD(v.id_tienda::text, 4, '0') AS id_tienda,
        v.ref_id_sku::text AS ref_id_sku,
        ROUND(SUM(COALESCE(v.venta_umv, 0))::numeric / 90.0, 2) AS prom_venta_diaria_unidades_90d,
        ROUND(SUM(COALESCE(v.venta_neta, 0))::numeric / 90.0, 2) AS prom_venta_diaria_pesos_90d
    FROM ecommdata.ventas_ecommerce_datawarehouse v
    WHERE LPAD(v.id_tienda::text, 4, '0') IN ('0581', '0917')
      AND v.fecha_facturacion::text >= (CURRENT_DATE - INTERVAL '90 days')::text
      AND v.ref_id_sku IN (SELECT ref_id_sku FROM Top500)
    GROUP BY 1, 2
),
MaxFechaStock AS (
    SELECT MAX(fecha) AS max_fecha FROM ecommdata.stock
),
StockJanis AS (
    SELECT DISTINCT ON (LPAD(st.id_tienda::text, 4, '0'), st.ref_id)
        LPAD(st.id_tienda::text, 4, '0') AS id_tienda,
        st.ref_id::text AS ref_id_sku,
        st.stock_janis,
        st.fecha_modificacion_janis
    FROM ecommdata.stock st
    CROSS JOIN MaxFechaStock m
    WHERE st.fecha = m.max_fecha
      AND LPAD(st.id_tienda::text, 4, '0') IN ('0581', '0917')
      AND st.ref_id IN (SELECT ref_id_sku FROM Top500)
    ORDER BY LPAD(st.id_tienda::text, 4, '0'), st.ref_id, st.fecha_modificacion_janis DESC NULLS LAST
)
SELECT
    NOW() AS fecha_ejecucion,
    t500.ranking,
    t500.ref_id_sku,
    t500.material,
    t500.umv,
    t500.vtex_id,
    t500.nombre_sku,
    ti.id_tienda,
    ti.nombre_tienda,
    sj.stock_janis,
    CASE 
        WHEN sj.stock_janis IS NULL OR sj.stock_janis <= 0 THEN TRUE 
        ELSE FALSE 
    END AS es_quiebre_janis,
    CASE 
        WHEN pa.cantidad_promociones_activas > 0 THEN TRUE 
        ELSE FALSE 
    END AS tiene_promocion_activa,
    COALESCE(pa.cantidad_promociones_activas, 0) AS cantidad_promociones_activas,
    pa.detalle_promociones_activas,
    COALESCE(v90.prom_venta_diaria_unidades_90d, 0) AS promedio_venta_diaria_unidades_90d,
    COALESCE(v90.prom_venta_diaria_pesos_90d, 0) AS promedio_venta_diaria_pesos_90d,
    sj.fecha_modificacion_janis
FROM Top500 t500
CROSS JOIN TiendasObjetivo ti
LEFT JOIN StockJanis sj 
    ON sj.id_tienda = ti.id_tienda
   AND sj.ref_id_sku = t500.ref_id_sku
LEFT JOIN PromocionesActivas pa 
    ON pa.ref_id_sku = t500.ref_id_sku
LEFT JOIN Ventas90Dias v90 
    ON v90.id_tienda = ti.id_tienda
   AND v90.ref_id_sku = t500.ref_id_sku
ORDER BY 
    CASE WHEN ti.id_tienda = '0917' THEN 0 ELSE 1 END ASC,
    t500.ranking ASC;
