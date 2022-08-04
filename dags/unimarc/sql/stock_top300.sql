insert into ecommdata.stock_top300
select s.*
from staging.stock_top300 s
inner join (
SELECT DISTINCT t_top.semana_smu,
    t2.id AS tienda,
    t_top.ref_id,
    t_top.venta_creada_bruta
   FROM ( SELECT t.id_janis
           FROM ecommdata.tiendas t) t_outer
     JOIN LATERAL ( SELECT t_inner.semana_smu,
            t_inner.id_tienda_janis,
            t_inner.ref_id,
            t_inner.venta_creada_bruta
           FROM ( SELECT c.semana_ano_texto AS semana_smu,
                    oj.id_tienda_janis,
                    op.ref_id,
                    sum(op.precio_venta * op.unidades_solicitadas::numeric) AS venta_creada_bruta
                   FROM ecommdata.orden_productos op
                     LEFT JOIN ecommdata.ordenes_janis oj ON oj.id = op.id_orden
                     LEFT JOIN ecommdata.calendario c ON c.fecha = oj.fecha_creacion::date
                     LEFT JOIN ecommdata.categorias cat ON op.ref_id_categoria = cat.ref_id
                  WHERE c.semana_relativa = '-1'::integer
                  GROUP BY op.ref_id, c.semana_ano_texto, oj.id_tienda_janis) t_inner
          WHERE t_inner.id_tienda_janis = t_outer.id_janis
          ORDER BY t_inner.venta_creada_bruta DESC
         LIMIT 300) t_top ON true
     LEFT JOIN ecommdata.tiendas t2 ON t_outer.id_janis = t2.id_janis
  ORDER BY t2.id, t_top.venta_creada_bruta DESC
 LIMIT 12000) tust on s.ref_id = tust.ref_id  and s.id_tienda = tust.tienda
 where s.fecha = '{{ds}}';

