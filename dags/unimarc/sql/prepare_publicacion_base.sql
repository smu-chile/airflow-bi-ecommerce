-- Base compartida para los 6 agregados publicacion_dia_tienda_*.
-- Antes cada agregado re-escaneaba la particion del dia y re-hacia el join a lista8
-- (concat no indexable). Aqui se hace UNA vez y los agregados leen esta tabla chica.
-- UNLOGGED: no genera WAL (es reconstruible en cada corrida).

CREATE UNLOGGED TABLE IF NOT EXISTS staging.publicacion_catalogo_base_unimarc AS
SELECT pc.fecha_hora,
    pc.id_tienda,
    pc.material,
    pc.ref_id,
    pc.c1,
    pc.c2,
    pc.c3,
    pc.marca,
    pc.publicacion_valida,
    pc.disponible_web,
    pc.stock_janis,
    pc.stock_valido,
    pc.foto_valida,
    pc.categoria_valida,
    pc.tienda_valida,
    pc.instock,
    pc.mfc,
    pc.surtido_ecommerce,
    pc.infaltable,
    pc.top_300
FROM ecommdata.publicacion_catalogo pc
WITH NO DATA;

TRUNCATE staging.publicacion_catalogo_base_unimarc;

-- Filtro = superconjunto de los 6 agregados:
--   surtido / con_marca / marca_proveedor: surtido_ecommerce OR (mfc AND stock_janis > 0)
--   evento / infaltable / top_300:         surtido_ecommerce OR mfc
-- El LEFT JOIN lista8 + WHERE l.catalogado = true original ya actuaba como INNER JOIN.
INSERT INTO staging.publicacion_catalogo_base_unimarc
SELECT pc.fecha_hora,
    pc.id_tienda,
    pc.material,
    pc.ref_id,
    pc.c1,
    pc.c2,
    pc.c3,
    pc.marca,
    pc.publicacion_valida,
    pc.disponible_web,
    pc.stock_janis,
    pc.stock_valido,
    pc.foto_valida,
    pc.categoria_valida,
    pc.tienda_valida,
    pc.instock,
    pc.mfc,
    pc.surtido_ecommerce,
    pc.infaltable,
    pc.top_300
FROM ecommdata.publicacion_catalogo pc
JOIN ecommdata.lista8 l
  ON concat(l.material, '-', l.umv) = pc.ref_id
 AND pc.id_tienda = l.id_tienda
WHERE pc.fecha_hora = '{{ts}}' at time zone 'America/Santiago' + interval '4 hours'
  AND (pc.surtido_ecommerce IS TRUE OR pc.mfc IS TRUE)
  AND pc.id_tienda NOT IN ('0486', '0053', '0398', '0054')
  AND l.bloq_centro IS NULL
  AND l.bloq_formato IS NULL
  AND l.catalogado = TRUE;

ANALYZE staging.publicacion_catalogo_base_unimarc;
