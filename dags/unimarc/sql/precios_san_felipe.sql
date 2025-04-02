WITH RankedPrices AS (
    SELECT 
        p.id AS id,
        CONCAT(l.material, '-', l.umv) AS skuRefid,
        1 AS skuMinQuantity,
        p.precio AS price,
        t.id AS store_id,
        p.precio AS listPrice,
        10 AS costPrice,
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
        1 AS active,
        ROW_NUMBER() OVER (PARTITION BY l.material, l.umv ORDER BY l.precio_regular DESC) AS rn
    FROM 
        ecommdata.lista8 l
    INNER JOIN 
        ecommdata.tiendas t 
        ON l.id_tienda = t.id
    LEFT JOIN 
        ecommdata.precios p 
        ON p.ref_id = CONCAT(l.material, '-', l.umv) 
        AND p.id_tienda_janis = t.id_janis
    WHERE 
        t.id IN ('0469', '0917', '0581', '0347', '0336', '0034', '0053', '0054', '0398')
        AND t.status = 1
        AND (p.valido_desde IS NULL OR p.valido_hasta IS NULL OR p.valido_desde <= p.valido_hasta)
        AND p.precio IS NOT NULL
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
    p.id AS id,
    t.id AS store,
    r.skuRefid,
    r.skuMinQuantity,
    r.price,
    r.listPrice,
    10 AS costPrice,
    r.validFrom,
    r.validTo,
    r."locked",
    r.updatePending,
    r.active
FROM RankedPrices r
LEFT JOIN ecommdata.precios p 
    ON p.ref_id = r.skuRefid 
    AND (p.id_tienda_janis = (SELECT id_janis FROM ecommdata.tiendas WHERE id = '0053')
     OR p.id_tienda_janis = (SELECT id_janis FROM ecommdata.tiendas WHERE id = '0054')
     OR p.id_tienda_janis = (SELECT id_janis FROM ecommdata.tiendas WHERE id = '0398'))  
LEFT JOIN ecommdata.tiendas t 
    ON t.id_janis = p.id_tienda_janis  
WHERE r.rn = 1;
