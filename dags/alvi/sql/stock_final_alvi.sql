BEGIN TRANSACTION;
insert into ecommdata_alvi.stock
select 
'{{ds}}'::date as fecha
, t.id as id_tienda
, t.glosa as glosa_tienda
, b.id as id_bodega
, b.nombre as nombre_bodega
, s.ref_id
, l.material
, s.nombre_sku as descripcion
, c.n1 as c1
, c.n2 as c2
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
, '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' as ultima_actualizacion
, l.material is not null and l.excluido is false as surtido_ecommerce
from staging.stock_vtex_alvi svu
left join ecommdata_alvi.bodegas b on svu.id_warehouse = b.id 
left join ecommdata_alvi.tiendas t on b.id_tienda = t.id 
left join ecommdata_alvi.skus s on svu.vtex_id = s.vtex_id
left join staging.stock_janis_alvi su on s.id = su.item_id and t.id_janis = su.store_id and b.id_janis = su.warehouse_id
left join ecommdata_alvi.productos p on s.ref_id = p.ref_id
left join ecommdata_alvi.categorias c on p.id_categoria = c.id
left join ecommdata_alvi.lista8 l on l.material = split_part(s.ref_id, '-', 1) and l.umv = split_part(s.ref_id, '-', 2) and t.id = l.id_tienda
where t.status = 1 and (b.dock_activo is true);
DELETE from ecommdata_alvi.stock
WHERE ultima_actualizacion < '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' AND fecha = '{{ds}}'::date;
COMMIT;

ANALYZE ecommdata_alvi.stock;
