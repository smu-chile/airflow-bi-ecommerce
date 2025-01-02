WITH PriceCounts AS (
    SELECT 
        l.material,
        l.umv,
        l.precio_regular,
        COUNT(*) AS price_count
    FROM 
        ecommdata.lista8 l
    GROUP BY 
        l.material, l.umv, l.precio_regular
),
ModalPrices AS (
    SELECT 
        material,
        umv,
        precio_regular AS price,
        ROW_NUMBER() OVER (PARTITION BY material, umv ORDER BY price_count DESC, precio_regular DESC) AS rn
    FROM 
        PriceCounts
)
INSERT INTO ecommdata.precios_san_felipe (
    id,
    store,
    skuRefid,
    skuMinQuantity,
    price,
    nombre_tienda_janis,
    listPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
)
SELECT
    CONCAT(l.material, '-', l.umv) AS skuRefid,
    1 AS skuMinQuantity,
    mp.price,
    t.nombre_tienda_janis,
    mp.price AS listPrice,
    CASE
        WHEN p.valido_desde IS NOT NULL THEN TO_CHAR(p.valido_desde, 'DD-MM-YYYY HH24:MI:SS')
        ELSE TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
    END AS validFrom,
    CASE
        WHEN p.valido_hasta IS NOT NULL THEN TO_CHAR(p.valido_hasta, 'DD-MM-YYYY HH24:MI:SS')
        ELSE TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
    END AS validTo,
    0 AS "locked",
    1 AS updatePending,
    1 AS active
FROM 
    ModalPrices mp
INNER JOIN 
    ecommdata.lista8 l
    ON mp.material = l.material AND mp.umv = l.umv
INNER JOIN 
    ecommdata.tiendas t 
    ON l.id_tienda = t.id
LEFT JOIN 
    ecommdata.precios p 
    ON p.ref_id = CONCAT(l.material, '-', l.umv) AND p.id_tienda_janis = t.id_janis
WHERE 
    mp.rn = 1;
