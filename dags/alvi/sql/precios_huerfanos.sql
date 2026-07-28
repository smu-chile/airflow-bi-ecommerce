WITH huerfanos AS (
    -- 1. Identificar productos huérfanos desde lista8 (no están en Pajaritos SAP ID 3092)
    -- Y retenemos en qué tiendas SÍ están activos según SAP.
    SELECT DISTINCT l.material, l.umv, l.id_tienda
    FROM ecommdata_alvi.lista8 l
    WHERE l.material NOT IN (
        SELECT material 
        FROM ecommdata_alvi.lista8 
        WHERE id_tienda = '3092'
    )
    AND l.excluido IS FALSE
    -- Validar que la categoría de Janis no sea una categoría bloqueada
    AND NOT EXISTS (
        SELECT 1 
        FROM ecommdata_alvi.skus s 
        JOIN ecommdata_alvi.productos p ON s.ref_id = p.ref_id
        JOIN ecommdata_alvi.categorias c ON p.id_categoria = c.id
        WHERE split_part(s.ref_id, '-', 1) = l.material 
          AND split_part(s.ref_id, '-', 2) = l.umv
          AND c.n1 IN ('No trabajar', 'No Trabajar', 'Fizzmod Categoria', 'Inactivos', 'Integración')
    )
),
precios_activos AS (
    -- 2. Obtener precios activos desde ecommdata_alvi.precios, excluyendo Pajaritos (id_tienda_janis = 9)
    SELECT p.*
    FROM ecommdata_alvi.precios p
    -- Aquí usamos el ref_id para saber el material y UMV y cruzar con los huérfanos
    INNER JOIN (SELECT DISTINCT material, umv FROM huerfanos) h 
            ON h.material = split_part(p.ref_id, '-', 1) 
           AND h.umv = split_part(p.ref_id, '-', 2)
    WHERE CURRENT_DATE BETWEEN p.valido_desde AND p.valido_hasta
      AND p.id_tienda_janis <> 9
),
ranking_tiendas AS (
    -- 3. Calcular la tienda ganadora por producto
    SELECT 
        id_sku_janis,
        id_tienda_janis,
        COUNT(cantidad_minima_sku) as cant_escalas,
        MAX(CASE WHEN cantidad_minima_sku = 1 THEN precio ELSE 0 END) as precio_base
    FROM precios_activos
    GROUP BY id_sku_janis, id_tienda_janis
),
tienda_ganadora AS (
    -- Seleccionamos solo la tienda #1 por SKU
    SELECT DISTINCT ON (id_sku_janis)
        id_sku_janis,
        id_tienda_janis
    FROM ranking_tiendas
    ORDER BY id_sku_janis, cant_escalas DESC, precio_base DESC
),
escala_ganadora AS (
    -- 4. Obtener TODAS las escalas completas de la tienda ganadora
    SELECT p.*
    FROM precios_activos p
    INNER JOIN tienda_ganadora tg ON p.id_sku_janis = tg.id_sku_janis AND p.id_tienda_janis = tg.id_tienda_janis
),
tiendas_destino AS (
    -- Para cada producto, definimos en qué tiendas inyectaremos el precio 100% basado en lista8:
    SELECT DISTINCT
        eg.id_sku_janis,
        eg.cantidad_minima_sku,
        h.id_tienda AS store,
        p_existente.id::VARCHAR AS id_precio_existente
    FROM escala_ganadora eg
    -- Cruzamos con huérfanos (lista8) para saber exactamente qué tiendas lo deben vender
    JOIN huerfanos h ON h.material = split_part(eg.ref_id, '-', 1) AND h.umv = split_part(eg.ref_id, '-', 2)
    -- Hacemos join con tiendas para poder traducir de id_tienda (SAP) a id_tienda_janis
    LEFT JOIN ecommdata_alvi.tiendas t ON t.id = h.id_tienda
    -- Rescatamos el ID del precio existente (si lo hay) PARA ESA ESCALA ESPECIFICA
    LEFT JOIN ecommdata_alvi.precios p_existente 
           ON p_existente.id_sku_janis = eg.id_sku_janis 
          AND p_existente.id_tienda_janis = t.id_janis
          AND p_existente.cantidad_minima_sku = eg.cantidad_minima_sku
)
-- 5. Generar la salida final
SELECT
    td.id_precio_existente                 AS id,
    td.store                               AS store,
    eg.ref_id                              AS skuRefId,
    eg.cantidad_minima_sku                 AS skuMinQuantity,
    eg.precio                              AS price,
    eg.precio_lista                        AS listPrice,
    COALESCE(eg.costo, 10)                 AS costPrice,
    TO_CHAR(COALESCE(eg.valido_desde, CURRENT_DATE),
            'DD-MM-YYYY HH24:MI:SS')       AS validFrom,
    TO_CHAR(COALESCE(eg.valido_hasta, CURRENT_DATE),
            'DD-MM-YYYY HH24:MI:SS')       AS validTo,
    0 AS locked,
    1 AS updatepending,
    1 AS active,
    pr.vtex_id                             AS vtex_product_id
FROM escala_ganadora eg
JOIN tiendas_destino td ON eg.id_sku_janis = td.id_sku_janis AND eg.cantidad_minima_sku = td.cantidad_minima_sku
JOIN ecommdata_alvi.tiendas t ON t.id = td.store
JOIN ecommdata_alvi.skus sk ON sk.id = eg.id_sku_janis
JOIN ecommdata_alvi.productos pr ON pr.id = sk.id_producto
LEFT JOIN ecommdata_alvi.precios p_existente ON p_existente.id::VARCHAR = td.id_precio_existente
WHERE t.status = 1
  AND (td.id_precio_existente IS NULL OR p_existente.precio <> eg.precio OR p_existente.precio_lista <> eg.precio_lista);