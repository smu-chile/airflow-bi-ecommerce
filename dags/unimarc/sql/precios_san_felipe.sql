WITH RankedPrices AS (
    SELECT 
        CONCAT(l.material, '-', l.umv) AS skuRefid,
        1 AS skuMinQuantity,
        p.precio AS price,
        l.precio_regular AS precio_l8,
        t.nombre_tienda_janis,
        p.precio_lista AS listPrice,
        TO_CHAR(p.valido_desde, 'DD-MM-YYYY HH24:MI:SS') AS validFrom,
        TO_CHAR(p.valido_hasta, 'DD-MM-YYYY HH24:MI:SS') AS validTo,
        0 AS "locked",
        1 AS updatePending,
        1 AS active,
        ROW_NUMBER() OVER (PARTITION BY l.material, l.umv ORDER BY l.precio_regular DESC) AS rn
    FROM 
        ecommdata.lista8 l
    INNER JOIN 
        ecommdata.tiendas t 
        ON l.id_tienda = t.id
    LEFT JOIN 
        ecommdata.precios p 
        ON p.ref_id = CONCAT(l.material, '-', l.umv) AND p.id_tienda_janis = t.id_janis
)
INSERT INTO ecommdata.precios_san_felipe (
    id,
    store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
)
SELECT
    '' as id,
    '0053' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1
    AND price = precio_l8
UNION ALL
SELECT
    '' as id,
    '0054' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1
    AND price = precio_l8
UNION ALL
SELECT
    '' as id,
    '0398' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1
    AND price = precio_l8;