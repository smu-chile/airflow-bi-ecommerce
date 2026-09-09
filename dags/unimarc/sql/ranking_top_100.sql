CREATE TABLE IF NOT EXISTS ecommdata.ranking_top_100_forzado (
    vtex_id VARCHAR(50) PRIMARY KEY,
    posicion INT NOT NULL UNIQUE,
    nombre VARCHAR(255),
    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO ecommdata.ranking_top_100_forzado (vtex_id, posicion, nombre)
VALUES
    ('59865', 1, 'Pan ciabatta granel Amada Masa 500 g'),
    ('279', 2, 'Palta hass granel 500 g'),
    ('9720', 3, 'Plátano granel 500 g'),
    ('57399', 4, 'Bebida Coca Cola zero 1 L'),
    ('365', 5, 'Trutro entero de pollo Super Pollo granel 800 g'),
    ('2896', 6, 'Leche entera natural Colun sin tapa 1 L'),
    ('93426', 7, 'Marraqueta precocida amada masa 4un'),
    ('479', 8, 'Limón malla 1 Kg'),
    ('324', 9, 'Tomate larga vida granel 500 g'),
    ('3269', 10, 'Pack Bebida Coca Cola original lata 6 un de 350 ml'),
    ('83483', 11, 'Naranja malla 2 Kg'),
    ('9973', 12, 'Pechuga de pollo deshuesada Super Pollo 850 g'),
    ('76772', 13, 'Aceite Nuestra Cocina 100% maravilla 900 ml'),
    ('59429', 14, 'Yoghurt Loncoleche protein natural endulzado 140 g'),
    ('61690', 15, 'Café Nescafé fina selección frasco 100 gr')
ON CONFLICT (vtex_id) DO NOTHING;

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
        f.posicion AS forced_order,
        COALESCE(rp.ranking, f.posicion) AS ranking,
        s.ref_id AS ref_id_sku,
        s.vtex_id::text AS vtex_id,
        COALESCE(rp.nombre_sku, s.nombre_sku, f.nombre) AS nombre_sku,
        COALESCE(wp.n_promocion::text, 'PROMOCION DESTACADA') AS n_promocion,
        COALESCE(wp.fecha_inicio_de_promocion, CURRENT_DATE) AS fecha_inicio_de_promocion,
        COALESCE(wp.fecha_fin_de_promocion, CURRENT_DATE) AS fecha_fin_de_promocion
    FROM ecommdata.ranking_top_100_forzado f
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
    WHERE vtex_id NOT IN (SELECT vtex_id FROM ecommdata.ranking_top_100_forzado) OR vtex_id IS NULL
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


