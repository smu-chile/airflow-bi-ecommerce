BEGIN TRANSACTION;
insert into staging.stock_top300
select 
'{{ds}}'::date as fecha
, t.id as id_tienda
, t.glosa as glosa_tienda
, b.id as id_bodega
, b.nombre as nombre_bodega
, s.ref_id
, p.material
, s.nombre_sku as descripcion
, c.n1 as c1
, c.n2 as c2
, c.n3 as c3
, s.multiplicador_unidad_medida
, s.unidades_pack 
, su.stock as stock_janis
, su.min_stock as stock_seguridad_janis
, su.infinite_stock::int::bool as stock_infinito_janis
, su.operation_type as tipo_operacion_janis
, svu.cantidad_total as stock_vtex
, svu.cantidad_reservada as stock_reservado_vtex
, (svu.cantidad_total - svu.cantidad_reservada) as stock_disponible_vtex
, svu.cantidad_ilimitada as stock_infinito_vtex
, su.date_published as fecha_publicacion_janis
, su.date_modified as fecha_modificacion_janis
, '{{ts}}'::timestamp as ultima_actualizacion
, l.material is not null as surtido_ecommerce
from staging.stock_vtex_unimarc_2 svu
left join ecommdata.bodegas b on svu.id_warehouse = b.id 
left join ecommdata.tiendas t on b.id_tienda = t.id 
left join ecommdata.skus s on svu.vtex_id = s.vtex_id
left join staging.stock_unimarc_2 su on s.id = su.item_id and t.id_janis = su.store_id and b.id_janis = su.warehouse_id
left join ecommdata.productos p on s.ref_id = p.ref_id
left join ecommdata.categorias c on p.id_categoria = c.id
left join ecommdata_unimarc.lista8 l on s.ref_id = CONCAT(l.material, '-', l.umv) and t.id = l.id_tienda and '{{ds}}'::date = l.fecha
where t.status = 1;
insert into ecommdata.stock_top300
select s.*
from staging.stock_top300
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
truncate table staging.stock_top300;
COMMIT
