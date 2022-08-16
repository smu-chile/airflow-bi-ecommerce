insert into ecommdata.stock_top300
select s.*
from staging.stock_top300 s
inner join (
select *
from
    (SELECT t_inner.semana_smu,
        t_inner.id_tienda,
        t_inner.ref_id,
        t_inner.venta_creada_bruta,
        row_number() over (
                    partition by id_tienda order by venta_creada_bruta desc
                    ) as rank0
    FROM (
        SELECT c.semana_ano_texto AS semana_smu,
            t.id as id_tienda,
            op.ref_id,
            sum(op.precio_venta * op.unidades_solicitadas::numeric) AS venta_creada_bruta
        FROM ecommdata.orden_productos op
        LEFT JOIN ecommdata.ordenes_janis oj ON oj.id = op.id_orden
        LEFT JOIN ecommdata.calendario c ON c.fecha = oj.fecha_creacion::date
        left join ecommdata.tiendas t on oj.id_tienda_janis = t.id_janis
        WHERE c.semana_relativa = '-1'::integer
        GROUP BY op.ref_id, c.semana_ano_texto, t.id
        ) t_inner
    ORDER BY t_inner.id_tienda, t_inner.venta_creada_bruta desc
) _t2
where rank0 <= 300
order by id_tienda, venta_creada_bruta desc) tust on s.ref_id = tust.ref_id  and s.id_tienda = tust.tienda
where s.fecha = '{{ds}}';

