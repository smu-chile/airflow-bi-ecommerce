insert into ecommdata_alvi.precios_huerfanos WITH huerfanos AS (
        -- 1. Identificar productos huérfanos desde lista8 (no están en Pajaritos SAP ID 3092)
        SELECT DISTINCT l.material
        FROM ecommdata_alvi.lista8 l
        WHERE l.material NOT IN (
                SELECT material
                FROM ecommdata_alvi.lista8
                WHERE id_tienda = "3092"
            )
    ),
    precios_activos AS (
        -- 2. Obtener precios activos excluyendo tienda 9 (Pajaritos) para no autoinyectarnos
        SELECT p.*
        FROM ecommdata_alvi.precios p
            INNER JOIN huerfanos h ON h.material = split_part(p.ref_id, '-', 1)
        WHERE CURRENT_DATE BETWEEN p.valido_desde AND p.valido_hasta
            AND p.id_tienda_janis <> 9
    ),
    ranking_tiendas AS (
        -- 3. Calcular la tienda ganadora por producto
        SELECT id_sku_janis,
            id_tienda_janis,
            COUNT(cantidad_minima_sku) as cant_escalas,
            MAX(
                CASE
                    WHEN cantidad_minima_sku = 1 THEN precio
                    ELSE 0
                END
            ) as precio_base
        FROM precios_activos
        GROUP BY id_sku_janis,
            id_tienda_janis
    ),
    tienda_ganadora AS (
        -- Seleccionamos solo la tienda #1 por SKU
        SELECT DISTINCT ON (id_sku_janis) id_sku_janis,
            id_tienda_janis
        FROM ranking_tiendas
        ORDER BY id_sku_janis,
            cant_escalas DESC,
            precio_base DESC
    ),
    escala_ganadora AS (
        -- 4. Obtener TODAS las escalas completas de la tienda ganadora
        SELECT p.*
        FROM precios_activos p
            INNER JOIN tienda_ganadora tg ON p.id_sku_janis = tg.id_sku_janis
            AND p.id_tienda_janis = tg.id_tienda_janis
    ) -- 5. Generar la salida final
SELECT NULL AS id,
    t.id AS store,
    eg.ref_id AS skuRefId,
    eg.cantidad_minima_sku AS skuMinQuantity,
    eg.precio AS price,
    eg.precio_lista AS listPrice,
    COALESCE(eg.costo, 10) AS costPrice,
    TO_CHAR(
        COALESCE(eg.valido_desde, CURRENT_DATE),
        'DD-MM-YYYY HH24:MI:SS'
    ) AS validFrom,
    TO_CHAR(
        COALESCE(eg.valido_hasta, CURRENT_DATE),
        'DD-MM-YYYY HH24:MI:SS'
    ) AS validTo,
    0 AS locked,
    1 AS updatepending,
    1 AS active
FROM escala_ganadora eg -- Cruzamos con las tiendas donde queremos inyectarlo. 
    -- Lo inyectamos en Pajaritos (id_janis = 9) y forzamos homologación en las demás
    CROSS JOIN ecommdata_alvi.tiendas t
WHERE t.status = 1;