CREATE TABLE IF NOT EXISTS ecommdata.alerta_stock_inteligente_listado (
    ref_id_sku VARCHAR(50) PRIMARY KEY,
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ecommdata.alerta_stock_inteligente_listado (ref_id_sku)
VALUES
    ('000000000000038313-UN'), ('000000000000038314-KG'), ('000000000000650272-KG'),
    ('000000000000060056-UN'), ('000000000000884017-KG'), ('000000000000924183-KG'),
    ('000000000000651935-UN'), ('000000000000651906-UN'), ('000000000000540007-UN'),
    ('000000000000544320-KG'), ('000000000000665881-UN'), ('000000000000038270-UN'),
    ('000000000000544317-KG'), ('000000000000038275-UN'), ('000000000000038277-UN'),
    ('000000000000645332-UN'), ('000000000000881625-UN'), ('000000000000038087-KG'),
    ('000000000000038086-KG'), ('000000000000205004-UN'), ('000000000000215704-UN'),
    ('000000000000284252-UN'), ('000000000000567242-UN'), ('000000000000648302-UN'),
    ('000000000000655589-UN'), ('000000000000339989-UN'), ('000000000000607817-UN'),
    ('000000000000756741-UN'), ('000000000000663396-UN'), ('000000000000663402-UN'),
    ('000000000000038260-UN'), ('000000000000028468-UN'), ('000000000000756739-UN'),
    ('000000000000038544-UN'), ('000000000000648091-UN'), ('000000000000060042-UN'),
    ('000000000000284269-UN'), ('000000000000648946-UN'), ('000000000000878890-UN'),
    ('000000000000038323-UN'), ('000000000000665414-UN'), ('000000000000652554-UN'),
    ('000000000000062770-KG'), ('000000000000578334-KG'), ('000000000000540507-KG'),
    ('000000000000315649-UN'), ('000000000000138392-KG'), ('000000000000037978-KG'),
    ('000000000000037988-KG'), ('000000000000037986-KG'), ('000000000000037984-KG'),
    ('000000000000038384-KG')
ON CONFLICT (ref_id_sku) DO NOTHING;

CREATE TABLE IF NOT EXISTS ecommdata.alerta_stock_inteligente (
    id SERIAL PRIMARY KEY,
    fecha_ejecucion TIMESTAMP,
    ref_id_sku VARCHAR(50),
    material VARCHAR(50),
    umv VARCHAR(10),
    vtex_id VARCHAR(50),
    nombre_sku VARCHAR(255),
    categoria VARCHAR(255),
    grupo_articulo VARCHAR(255),
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

DELETE FROM ecommdata.alerta_stock_inteligente
WHERE fecha_ejecucion::date = CURRENT_DATE;

INSERT INTO ecommdata.alerta_stock_inteligente (
    fecha_ejecucion,
    ref_id_sku,
    material,
    umv,
    vtex_id,
    nombre_sku,
    categoria,
    grupo_articulo,
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
      AND LPAD(t.id::text, 4, '0') IN ('0917', '0581', '0442', '0347')
    ORDER BY LPAD(t.id::text, 4, '0') ASC
),
ListadoForzado AS (
    SELECT DISTINCT ref_id_sku
    FROM ecommdata.alerta_stock_inteligente_listado
),
SKUsFiltroLista8 AS (
    SELECT DISTINCT
        (l.material || '-' || l.umv) AS ref_id_sku,
        l.categoria,
        l.grupo_articulo
    FROM ecommdata.lista8 l
    WHERE l.categoria ILIKE 'asados%'
       OR l.grupo_articulo ILIKE 'asado de tira'
       OR l.grupo_articulo ILIKE 'filete'
       OR l.categoria ILIKE 'lomo%'
       OR l.categoria ILIKE 'pollo%'
),
SKUsCandidatos AS (
    SELECT ref_id_sku FROM ListadoForzado
    UNION
    SELECT ref_id_sku FROM SKUsFiltroLista8
),
SKUsBase AS (
    SELECT DISTINCT ON (c.ref_id_sku)
        c.ref_id_sku,
        SPLIT_PART(c.ref_id_sku, '-', 1) AS material,
        SPLIT_PART(c.ref_id_sku, '-', 2) AS umv,
        s.vtex_id::text AS vtex_id,
        COALESCE(s.nombre_sku, p.nombre, c.ref_id_sku) AS nombre_sku,
        COALESCE(fl8.categoria, l8.categoria) AS categoria,
        COALESCE(fl8.grupo_articulo, l8.grupo_articulo) AS grupo_articulo
    FROM SKUsCandidatos c
    LEFT JOIN ecommdata.skus s ON s.ref_id = c.ref_id_sku
    LEFT JOIN ecommdata.productos p ON p.ref_id = c.ref_id_sku
    LEFT JOIN SKUsFiltroLista8 fl8 ON fl8.ref_id_sku = c.ref_id_sku
    LEFT JOIN ecommdata.lista8 l8 ON (l8.material || '-' || l8.umv) = c.ref_id_sku
    ORDER BY c.ref_id_sku
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
    JOIN SKUsBase sb ON wp.material::text = sb.material OR wp.material::text = LTRIM(sb.material, '0')
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
    WHERE LPAD(v.id_tienda::text, 4, '0') IN ('0917', '0581', '0442', '0347')
      AND v.fecha_facturacion::text >= (CURRENT_DATE - INTERVAL '90 days')::text
      AND v.ref_id_sku IN (SELECT ref_id_sku FROM SKUsBase)
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
      AND LPAD(st.id_tienda::text, 4, '0') IN ('0917', '0581', '0442', '0347')
      AND st.ref_id IN (SELECT ref_id_sku FROM SKUsBase)
    ORDER BY LPAD(st.id_tienda::text, 4, '0'), st.ref_id, st.fecha_modificacion_janis DESC NULLS LAST
)
SELECT
    NOW() AS fecha_ejecucion,
    sb.ref_id_sku,
    sb.material,
    sb.umv,
    sb.vtex_id,
    sb.nombre_sku,
    sb.categoria,
    sb.grupo_articulo,
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
FROM SKUsBase sb
CROSS JOIN TiendasObjetivo ti
LEFT JOIN StockJanis sj 
    ON sj.id_tienda = ti.id_tienda
   AND sj.ref_id_sku = sb.ref_id_sku
LEFT JOIN PromocionesActivas pa 
    ON pa.ref_id_sku = sb.ref_id_sku
LEFT JOIN Ventas90Dias v90 
    ON v90.id_tienda = ti.id_tienda
   AND v90.ref_id_sku = sb.ref_id_sku
ORDER BY 
    CASE WHEN ti.id_tienda = '0917' THEN 0 ELSE 1 END ASC,
    sb.ref_id_sku ASC;
