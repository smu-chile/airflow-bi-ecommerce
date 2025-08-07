insert into ecommdata_alvi.precios_pajaritos
WITH p               AS (
    /* precios vigentes para TODAS las tiendas excepto la 9 */
    SELECT DISTINCT ON (id_tienda_janis, id_sku_janis, cantidad_minima_sku)
           *
    FROM   ecommdata_alvi.precios
    WHERE  CURRENT_DATE BETWEEN valido_desde AND valido_hasta
      AND  id_tienda_janis <> 9
    ORDER  BY id_tienda_janis,
             id_sku_janis,
             cantidad_minima_sku,
             valido_desde DESC                -- <-- el más nuevo arriba
),
p_pajaritos      AS (
    /* precios vigentes de la tienda 9 (referencia)          */
    SELECT DISTINCT ON (id_sku_janis, cantidad_minima_sku)
           *
    FROM   ecommdata_alvi.precios
    WHERE  CURRENT_DATE BETWEEN valido_desde AND valido_hasta
      AND  id_tienda_janis = 9
    ORDER  BY id_sku_janis,
             cantidad_minima_sku,
             valido_desde DESC
)
SELECT
    p.id                                   AS id,
    t.id                                   AS store,
    p.ref_id                               AS skuRefId,
    p.cantidad_minima_sku                  AS skuMinQuantity,
    p_pajaritos.precio                     AS price,
    p_pajaritos.precio_lista               AS listPrice,
    COALESCE(p_pajaritos.costo, 10)        AS costPrice,
    TO_CHAR(COALESCE(p.valido_desde, CURRENT_DATE),
            'DD-MM-YYYY HH24:MI:SS')       AS validFrom,
    TO_CHAR(COALESCE(p.valido_hasta, CURRENT_DATE),
            'DD-MM-YYYY HH24:MI:SS')       AS validTo,
    0 AS locked,
    1 AS updatepending,
    1 AS active
FROM p
JOIN ecommdata_alvi.tiendas t
  ON p.id_tienda_janis = t.id_janis
inner JOIN p_pajaritos
  ON p.id_sku_janis        = p_pajaritos.id_sku_janis
 AND p.cantidad_minima_sku = p_pajaritos.cantidad_minima_sku
WHERE t.status = 1                -- tiendas activas
  AND (p.precio <> p_pajaritos.precio)
