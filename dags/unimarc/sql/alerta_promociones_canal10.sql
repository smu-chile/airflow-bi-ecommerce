-- Consulta de auditoría para detectar SKUs con promociones en Canal 10 y Canal 70 que pierden la promo de Canal 10 antes de su término.
WITH cargas AS (
    SELECT DISTINCT fecha_carga
    FROM ecommdata.promotions_viewer_simple
    ORDER BY fecha_carga DESC
    LIMIT 2
),
carga_actual AS (
    SELECT fecha_carga FROM cargas LIMIT 1
),
carga_previa AS (
    SELECT fecha_carga FROM cargas OFFSET 1 LIMIT 1
),
prev_c10 AS (
    SELECT 
        sku_id,
        sku_name,
        workflow_id AS workflow_id_c10,
        promotion_name AS promotion_name_c10,
        begin_date AS begin_date_c10,
        end_date AS end_date_c10,
        fecha_carga
    FROM ecommdata.promotions_viewer_simple
    WHERE fecha_carga = (SELECT fecha_carga FROM carga_previa)
      AND (promotional_tag IN ('C011', '10') OR promotion_name ILIKE '%C10%' OR promotion_name ILIKE '%CANAL10%')
      AND end_date > CURRENT_TIMESTAMP
),
prev_c70 AS (
    SELECT 
        sku_id,
        workflow_id AS workflow_id_c70,
        promotion_name AS promotion_name_c70,
        begin_date AS begin_date_c70,
        end_date AS end_date_c70
    FROM ecommdata.promotions_viewer_simple
    WHERE fecha_carga = (SELECT fecha_carga FROM carga_previa)
      AND (promotional_tag IN ('C065', '70') OR promotion_name ILIKE '%C70%' OR promotion_name ILIKE '%CANAL70%')
),
curr_c10 AS (
    SELECT DISTINCT sku_id
    FROM ecommdata.promotions_viewer_simple
    WHERE fecha_carga = (SELECT fecha_carga FROM carga_actual)
      AND (promotional_tag IN ('C011', '10') OR promotion_name ILIKE '%C10%' OR promotion_name ILIKE '%CANAL10%')
)
SELECT 
    p10.sku_id,
    p10.sku_name,
    p10.workflow_id_c10,
    p10.promotion_name_c10,
    p10.begin_date_c10,
    p10.end_date_c10,
    p70.workflow_id_c70,
    p70.promotion_name_c70,
    p10.fecha_carga AS fecha_carga_anterior,
    (SELECT fecha_carga FROM carga_actual) AS fecha_carga_actual
FROM prev_c10 p10
JOIN prev_c70 p70 ON p10.sku_id = p70.sku_id
LEFT JOIN curr_c10 c10 ON p10.sku_id = c10.sku_id
WHERE c10.sku_id IS NULL
ORDER BY p10.end_date_c10 ASC;
