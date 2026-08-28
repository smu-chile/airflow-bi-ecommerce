WITH CatalogBase AS (
    SELECT 
        -- 1. Campos Base
        l.material AS sku,
        l.umv AS umv, 
        s.ean_primario AS ean,
        l.precio_regular AS precio,
        p.nombre AS nombre,
        CASE 
            WHEN t.imagen IS NOT NULL AND t.imagen <> '' 
                THEN CONCAT('https://unimarc.vteximg.com.br', t.imagen)
            ELSE NULL 
        END AS imagen,
        ec.n1 AS categoria_n1,
        ec.n2 AS categoria_n2,
        ec.n3 AS categoria_n3,
        m.nombre AS marca,
        -- 2. Campos Extra (Rappi, Peya, Uber)
        (s.ppum / NULLIF(s.unidades_pack, 0)) AS quantity,          -- Requerido por Rappi
        s.unidad_de_medida_ppum AS unit_type,                       -- Requerido por Rappi
        s.unidades_pack AS selling_units,                           -- Requerido por Rappi
        CASE 
            WHEN l.umv = 'KGV' OR l.umv = 'KG' THEN 'VERDADERO' 
            ELSE '' 
        END AS is_weightable,                                    -- Requerido por Rappi
        -- Numeración de fila única por SKU (ref_id) para evitar duplicación por tienda
        ROW_NUMBER() OVER (PARTITION BY s.ref_id ORDER BY s.ref_id ASC) AS rn
    FROM ecommdata.lista8 l
    INNER JOIN ecommdata.skus s 
        ON l.material || '-' || l.umv = s.ref_id
    INNER JOIN ecommdata.productos p 
        ON s.ref_id = p.ref_id
    LEFT JOIN ecommdata.categorias ec
        ON p.id_categoria = ec.id
    LEFT JOIN ecommdata.marcas m
        ON p.id_marca = m.id
    LEFT JOIN ecommdata.imagenes_sku t 
        ON s.ref_id = t.ref_id AND t.orden = 1
    WHERE (ec.n1 NOT IN ('No Trabajar', 'Inactivos', 'Integración') OR ec.n1 IS NULL)
      AND l.excluido IS NOT TRUE
      AND l.material NOT IN ('000000000000163603', '000000000000167429')
      AND l.bloq_centro IS NULL
      AND l.bloq_formato IS NULL
)
SELECT 
    sku,
    umv,
    ean,
    precio,
    nombre,
    imagen,
    categoria_n1,
    categoria_n2,
    categoria_n3,
    marca,
    quantity,
    unit_type,
    selling_units,
    is_weightable
FROM CatalogBase
WHERE rn = 1
  AND imagen IS NOT NULL
  AND categoria_n1 IS NOT NULL
  AND precio IS NOT NULL;
  