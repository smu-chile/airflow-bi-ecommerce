WITH RankedCatalog AS (
    SELECT 
        l.material AS sku,
        s.ean_primario AS ean,
        p.nombre AS name,
        (s.ppum / NULLIF(s.unidades_pack, 0)) AS quantity,
        s.unidad_de_medida_ppum AS unit_type,
        s.unidades_pack AS selling_units,
        CASE WHEN s.unidad_de_venta = 'KGV' THEN 'VERDADERO' ELSE '' END AS is_weightable,
        'VERDADERO' AS is_prepackaged,
        CASE 
            WHEN t.imagen IS NOT NULL AND t.imagen <> '' THEN CONCAT('https://unimarc.vteximg.com.br', t.imagen)
            ELSE NULL 
        END AS image,
        ROW_NUMBER() OVER (PARTITION BY l.material ORDER BY s.ref_id ASC) as rn
    FROM ecommdata.lista8 l
    INNER JOIN ecommdata.skus s 
        ON l.material || '-' || l.umv = s.ref_id
    INNER JOIN ecommdata.productos p 
        ON s.ref_id = p.ref_id
    LEFT JOIN ecommdata.categorias ec
        ON p.id_categoria = ec.id
    LEFT JOIN ecommdata.imagenes_sku t 
        ON s.ref_id = t.ref_id AND t.orden = 1
    WHERE (ec.n1 NOT IN ('No Trabajar', 'Inactivos') OR ec.n1 IS NULL)
)
SELECT 
    sku,
    ean,
    name,
    quantity,
    unit_type,
    selling_units,
    is_weightable,
    is_prepackaged,
    image
FROM RankedCatalog
WHERE rn = 1;


