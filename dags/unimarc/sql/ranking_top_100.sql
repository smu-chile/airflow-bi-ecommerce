DROP TABLE IF EXISTS ecommdata.ranking_top_100;

CREATE TABLE ecommdata.ranking_top_100 (
    ranking INT,
    ref_id_sku TEXT,
    vtex_id TEXT,
    nombre_sku TEXT,
    n_promocion TEXT,
    fecha_inicio_de_promocion DATE,
    fecha_fin_de_promocion DATE
);

INSERT INTO ecommdata.ranking_top_100 (
    ranking,
    ref_id_sku,
    vtex_id,
    nombre_sku,
    n_promocion,
    fecha_inicio_de_promocion,
    fecha_fin_de_promocion
)
WITH promo_products AS (
    SELECT DISTINCT ON (rp.ref_id_sku)
        rp.ranking,  
        rp.ref_id_sku,
        s.vtex_id::text AS vtex_id,
        rp.nombre_sku, 
        wp.n_promocion::text AS n_promocion, 
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
    LEFT JOIN ecommdata.skus s
        ON s.ref_id = rp.ref_id_sku
    WHERE wp.fecha_inicio_de_promocion <= CURRENT_DATE 
      AND wp.fecha_fin_de_promocion >= CURRENT_DATE
      and wp.nombre_promocion not like '%UNIPAY%'
      and wp.n_promocion not in (
        1120012024,
        1120012025,
        1120022024,
        1120032024,
        1120032025,
        1120042024,
        1120052024,
        1120052025,
        1120062024,
        1120062025,
        1120082024,
        1120092024,
        1120112025,
        1120122025,
        1120132025,
        1120152025,
        1120162025
      )
    ORDER BY rp.ref_id_sku, wp.fecha_inicio_de_promocion DESC
),
forced_skus AS (
    SELECT DISTINCT ON (f.vtex_id)
        f.ord AS forced_order,
        COALESCE(rp.ranking, f.ord) AS ranking,
        s.ref_id AS ref_id_sku,
        s.vtex_id::text AS vtex_id,
        COALESCE(rp.nombre_sku, s.nombre_sku) AS nombre_sku,
        COALESCE(wp.n_promocion::text, 'PROMOCION DESTACADA') AS n_promocion,
        COALESCE(wp.fecha_inicio_de_promocion, CURRENT_DATE) AS fecha_inicio_de_promocion,
        COALESCE(wp.fecha_fin_de_promocion, CURRENT_DATE) AS fecha_fin_de_promocion
    FROM (
        VALUES 
            ('59865', 1),
            ('279', 2),
            ('9720', 3),
            ('57399', 4),
            ('365', 5),
            ('2896', 6),
            ('93426', 7),
            ('479', 8),
            ('324', 9),
            ('3269', 10),
            ('83483', 11),
            ('9973', 12),
            ('76772', 13),
            ('59429', 14),
            ('61690', 15)
    ) AS f(vtex_id, ord)
    JOIN ecommdata.skus s ON s.vtex_id::text = f.vtex_id
    LEFT JOIN ecommdata.ranking_productos rp ON s.ref_id = rp.ref_id_sku
    LEFT JOIN ecommdata.workflow_promociones wp ON rp.ref_id_sku = (
            TRIM(wp.material::text) || '-' || CASE 
                WHEN TRIM(wp.umv::text) = 'ST' THEN 'UN'
                WHEN TRIM(wp.umv::text) = 'CS' THEN 'CJ'
                ELSE TRIM(wp.umv::text)
            END
        )
    ORDER BY f.vtex_id, wp.fecha_inicio_de_promocion DESC NULLS LAST
),
combined AS (
    SELECT ref_id_sku, vtex_id, nombre_sku, n_promocion, fecha_inicio_de_promocion, fecha_fin_de_promocion, forced_order, ranking FROM forced_skus
    UNION ALL
    SELECT ref_id_sku, vtex_id, nombre_sku, n_promocion, fecha_inicio_de_promocion, fecha_fin_de_promocion, 999 AS forced_order, rp_rank.ranking
    FROM promo_products rp_rank
    WHERE vtex_id NOT IN ('59865', '279', '9720', '57399', '365', '2896', '93426', '479', '324', '3269', '83483', '9973', '76772', '59429', '61690') OR vtex_id IS NULL
)
SELECT 
    ROW_NUMBER() OVER (
        ORDER BY forced_order ASC, ranking ASC NULLS LAST
    )::int AS ranking,
    ref_id_sku,
    vtex_id,
    nombre_sku,
    n_promocion,
    fecha_inicio_de_promocion,
    fecha_fin_de_promocion
FROM combined
ORDER BY ranking ASC
LIMIT 100;


