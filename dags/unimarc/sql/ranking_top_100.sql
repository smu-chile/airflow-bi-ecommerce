CREATE TABLE IF NOT EXISTS ecommdata.ranking_top_100 (
    ranking INT,
    nombre_sku TEXT,
    n_promocion TEXT,
    fecha_inicio_de_promocion DATE,
    fecha_fin_de_promocion DATE
);

TRUNCATE TABLE ecommdata.ranking_top_100;

INSERT INTO ecommdata.ranking_top_100 (
    ranking,
    nombre_sku,
    n_promocion,
    fecha_inicio_de_promocion,
    fecha_fin_de_promocion
)
SELECT *
FROM (
    SELECT DISTINCT ON (rp.ref_id_sku)
        rp.ranking,  
        rp.nombre_sku, 
        wp.n_promocion, 
        wp.fecha_inicio_de_promocion, 
        wp.fecha_fin_de_promocion
    FROM ecommdata.workflow_promociones wp 
    LEFT JOIN ecommdata.ranking_productos rp
        ON rp.ref_id_sku = (
            TRIM(wp.material::text) || '-' || CASE 
                WHEN TRIM(wp.umv::text) = 'ST' THEN 'UN'
                WHEN TRIM(wp.umv::text) = 'CS' THEN 'CJ'
                ELSE TRIM(wp.umv::text)
            END
        )
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
    ORDER BY rp.ref_id_sku, wp.fecha_inicio_de_promocion DESC
) sub
ORDER BY sub.ranking ASC NULLS LAST
LIMIT 100;
