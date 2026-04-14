DELETE FROM catalogo.publicacion_dia_tienda_surtido_y_con_marca_alvi WHERE fecha_hora = '{{ts}}' at time zone 'America/Santiago' + interval '4 hours';
insert into catalogo.publicacion_dia_tienda_surtido_y_con_marca_alvi
SELECT pc.fecha_hora,
    pc.id_tienda,
    pc.c1,
    pc.c2,
    pc.marca,
    count(1) AS total_surtido,
    sum(
        CASE
            WHEN pc.publicacion_valida IS TRUE THEN 1
            ELSE 0
        END) AS publicacion_valida,
    sum(
        CASE
            WHEN pc.disponible_web IS TRUE THEN 1
            ELSE 0
        END) AS disponible_web,
    sum(
        CASE
            WHEN COALESCE(pc.stock_janis, 0::bigint) > 0 THEN 1
            ELSE 0
        END) AS con_stock,
    sum(
        CASE
            WHEN pc.foto_valida IS TRUE THEN 1
            ELSE 0
        END) AS con_foto,
    sum(
        CASE
            WHEN pc.categoria_valida IS TRUE THEN 1
            ELSE 0
        END) AS con_categoria,
    sum(
        CASE
            WHEN pc.tienda_valida IS TRUE THEN 1
            ELSE 0
        END) AS con_tienda,
     sum(
        CASE
            WHEN pc.stock_valido IS TRUE THEN 1
            ELSE 0
        END) AS con_stock_visible
FROM ecommdata_alvi.publicacion_catalogo pc
left join ecommdata_alvi.lista8 l on pc.material = l.material and pc.id_tienda =l.id_tienda
WHERE pc.surtido_ecommerce IS TRUE
and pc.fecha_hora = '{{ts}}' at time zone 'America/Santiago' + interval '4 hours'
and l.bloq_centro is null and l.bloq_formato is null and l.catalogado =true
GROUP BY pc.fecha_hora, pc.id_tienda, pc.c1, pc.c2, pc.marca;