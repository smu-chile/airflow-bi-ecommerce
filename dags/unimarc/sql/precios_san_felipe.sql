WITH RankedPrices AS (
    SELECT 
        p.id as id,
        CONCAT(l.material, '-', l.umv) AS skuRefid,
        1 AS skuMinQuantity,
        l.precio_regular AS price,
        t.nombre_tienda_janis,
        l.precio_regular AS listPrice,
        10 as costPrice,
        case
        	when p.valido_desde is not null then TO_CHAR(p.valido_desde, 'DD-MM-YYYY HH24:MI:SS')
        	else TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
        end as validFrom,
        case
        	when p.valido_hasta is not null then TO_CHAR(p.valido_hasta, 'DD-MM-YYYY HH24:MI:SS')
        	else TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
        end as validTo,
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
    costPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
)
SELECT
    id,
    '0053' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    10 as costPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1
UNION ALL
SELECT
    id,
    '0054' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    10 as costPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1
UNION ALL
SELECT
    id,
    '0398' AS store,
    skuRefid,
    skuMinQuantity,
    price,
    listPrice,
    10 as costPrice,
    validFrom,
    validTo,
    "locked",
    updatePending,
    active
FROM 
    RankedPrices
WHERE 
    rn = 1