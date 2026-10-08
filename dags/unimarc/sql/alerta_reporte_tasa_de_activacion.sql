-- ==============================================================================
-- Consulta: Alerta de Venta de Promociones por Canal (Canal 70 y Canal 10)
-- Descripción: Para cada canal, identifica promociones vigentes (con los filtros
--              estándar de exclusión) y verifica si cada producto en promo ha
--              registrado venta acumulada desde el inicio de la promo hasta hoy.
-- Nota: Parámetro :canal debe sustituirse por '70' o '10' en ejecución Python.
-- ==============================================================================

WITH promos_activas AS (
    SELECT DISTINCT
        wp.canal_distribucion,
        wp.n_promocion,
        wp.nombre_promocion,
        wp.material,
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
        wp.id_mecanica <> ALL (
            ARRAY [124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59,50]
        )
    )
    AND wp.fecha_inicio_de_promocion <= current_date
    AND wp.fecha_fin_de_promocion >= current_date
    AND wp.tipo_promocion <> 3
    AND wp.n_promocion NOT IN (
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
        4000952026, -- agregada 11/08/2026 Promo excl quesos
        4000182025,
        4000602026, -- agregada 26/05/2026
        4000652026, -- agregada 09/06/2026 Promo dia del completo
        1120232025,
        5551272026, -- agregada 17/06/2026 Promo regional
        5510102026, -- agregada 22/07/2026 Prueba B2B
        1020032026, -- agregada 15/07/2026 Promo ripley
        1020052026  -- agregada 09/09/2026 Promo Banco Estado
    )
    AND wp.canal_distribucion = :canal  -- Reemplazar con '70' o '10'
    AND wp.nombre_promocion::text NOT ILIKE '%ZONA%'
    AND wp.nombre_promocion::text NOT ILIKE '%MFC%'
    AND wp.nombre_promocion::text NOT ILIKE '%UNIPAY%'
    AND wp.nombre_promocion::text NOT ILIKE '%917%'
    AND wp.nombre_promocion::text NOT ILIKE '%ESTADO%'
    AND wp.nombre_promocion::text NOT ILIKE '%LOC%'
    AND wp.nombre_promocion::text NOT ILIKE 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
    AND wp.nombre_promocion::text NOT ILIKE '%HUACHALALUME%'
    AND wp.nombre_promocion::text NOT ILIKE '%LOCAL%'
    AND wp.nombre_promocion::text NOT ILIKE '%MEMB%'
    AND wp.nombre_promocion::text NOT ILIKE '%REGIO%'
    AND wp.nombre_promocion::text NOT ILIKE '%BCO%'
    AND wp.nombre_promocion::text NOT ILIKE '%CUMPLEANOS%'
    AND wp.nombre_promocion::text NOT ILIKE '%BLACK%'
    AND s.vtex_id <> ALL (
        ARRAY [3610, 82183, 82184, 39730]
    )
    AND COALESCE(rpa.porcentaje_descuento_final, 0) <= 75
),
ventas_acumuladas AS (
    SELECT DISTINCT p.ref_id
    FROM promos_activas p
    JOIN ecommdata.orden_productos op ON (
        op.ref_id = p.ref_id
        OR (p.vtex_id IS NOT NULL AND op.producto_vtex_id = p.vtex_id)
    )
    JOIN ecommdata.ordenes_janis oj ON oj.id = op.id_orden
    WHERE oj.estado_janis BETWEEN 10 AND 90
      AND oj.fecha_facturacion::date >= p.fecha_inicio
      AND oj.fecha_facturacion::date <= CURRENT_DATE
      AND COALESCE(NULLIF(op.unidades_pickeadas, 0), op.unidades_solicitadas, 0) > 0
)
SELECT
    p.canal_distribucion,
    p.ref_id,
    p.material,
    COALESCE(p.vtex_id::text, 'Sin VTEX ID') AS vtex_id,
    p.n_promocion,
    p.nombre_promocion,
    p.fecha_inicio,
    p.fecha_fin,
    CASE WHEN v.ref_id IS NOT NULL THEN 1 ELSE 0 END AS con_venta
FROM promos_activas p
LEFT JOIN ventas_acumuladas v ON p.ref_id = v.ref_id
ORDER BY con_venta ASC, p.fecha_inicio ASC, p.ref_id ASC;
