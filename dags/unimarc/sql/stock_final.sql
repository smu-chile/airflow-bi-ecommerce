BEGIN TRANSACTION;
insert into ecommdata.stock
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
, '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' as ultima_actualizacion
	, l.material is not null and l.excluido is false and ( (l.bloq_centro is null and l.bloq_formato is null) OR CONCAT(l.material, '-', l.umv) IN ('000000000000661989-UN', '000000000000661988-UN') ) and l.catalogado is true as surtido_ecommerce
, case
	when li.material is null then false
	else true
end as infaltable
from staging.stock_vtex_unimarc svu
left join ecommdata.bodegas b on svu.id_warehouse = b.id 
left join ecommdata.tiendas t on b.id_tienda = t.id 
left join ecommdata.skus s on svu.vtex_id = s.vtex_id
left join staging.stock_unimarc su on s.id = su.item_id and t.id_janis = su.store_id and b.id_janis = su.warehouse_id
left join ecommdata.productos p on s.ref_id = p.ref_id
left join ecommdata.categorias c on p.id_categoria = c.id
left join ecommdata.lista8 l on s.ref_id = CONCAT(l.material, '-', l.umv) and t.id = l.id_tienda
left join ecommdata.lista_infaltables li on p.material = li.material
where t.status = 1 and (b.dock_activo is true)
and NOT (
	(t.id = '0018' AND b.id = '9051') OR
	(t.id = '0069' AND b.id = '0576') OR
	(t.id = '0088' AND b.id = '0324')
)
;
DELETE from ecommdata.stock
WHERE ultima_actualizacion < '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' AND fecha = '{{ds}}'::date;
COMMIT;
