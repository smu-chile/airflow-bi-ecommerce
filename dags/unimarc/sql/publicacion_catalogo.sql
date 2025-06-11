insert into ecommdata.publicacion_catalogo
(with data_publicacion as (
select distinct on (s.ultima_actualizacion, s.ref_id, s.id_tienda, s.id_bodega)
s.ultima_actualizacion as fecha_hora
, s.material
, s.ref_id
, s.descripcion 
, s.c1
, s.c2
, s.c3
, s.id_tienda 
, s.id_bodega
, m.nombre as marca
, val.foto_valida
, foto.q_foto as cantidad_foto
, foto.foto_en_preparacion
, val.categoria_valida
, val.stock_valido
, val.precio_valido
, val.tienda_valida
, case
	when val.foto_valida and val.categoria_valida and tienda_valida is true then true 
	else false
end as publicacion_valida
, case
	when val.foto_valida and val.categoria_valida and val.stock_valido and tienda_valida is true then true 
	else false
end as disponible_web
, case
	when li.material is not null then true
	else false
end as infaltable
, s.stock_janis
, s.stock_seguridad_janis
, s.stock_infinito_janis
, s.stock_vtex
, s.stock_reservado_vtex
, s.stock_infinito_vtex
, s.surtido_ecommerce
, case
	when tp.material is not null then true
	else false
end as top_300
, case
	when (smt.ref_id is not null and smt.quantity_on_hand > 0 and um.mfc_is_item_side = 'REG')  then true
	else false
end as mfc
from ecommdata.stock s
left join (select t1.ref_id
	, t1.q_foto
	, case
		when t2.ref_id is null then false
		else true
	end as foto_en_preparacion
	from (select isku.ref_id, count(1) as q_foto
	from ecommdata.imagenes_sku isku
	group by isku.ref_id) t1
	left join (select isku.ref_id
	from ecommdata.imagenes_sku isku
	where isku.imagen ilike any(array['%foto-en%','%foto-unimarc%'])) t2 
on t1.ref_id = t2.ref_id) foto on s.ref_id = foto.ref_id
left join ecommdata.productos p on s.ref_id = p.ref_id
left join ecommdata.categorias c on p.id_categoria = c.id
left join ecommdata.tiendas t on s.id_tienda = t.id
left join ecommdata.precios pr on t.id_janis = pr.id_tienda_janis and s.ref_id = pr.ref_id
left join ecommdata.productos_tienda pt on s.ref_id = pt.ref_id and s.id_tienda = pt.id_tienda
left join ecommdata.marcas m on p.id_marca = m.id
left join ecommdata.lista_infaltables li on s.material = li.material
left join ecommdata.top300 tp on s.material = tp.material
left join (select tom_id as ref_id,quantity_on_hand, '1917' as id_tienda
			from ecommdata.stock_mfc_takeoff
			where fecha = (select max(fecha) from ecommdata.stock_mfc_takeoff smt)) as smt
			on smt.ref_id = s.ref_id and s.id_tienda = smt.id_tienda
left join ecommdata.ubicacion_mfc um on concat(um.sap_code,'-',um.measurement_unit) = s.ref_id and um.store = s.id_tienda
inner join lateral (select
	case 
	when foto.ref_id is null then false
	else true
end as foto_valida
, case 
	when c.status = 'activo' then true
	else false
end as categoria_valida
, case
	when (s.stock_disponible_vtex > 0) or (s.stock_infinito_vtex is true) then true 
	else false
end as stock_valido
, case
	when pr.id is null then false
	else true
end as precio_valido
, case 
	when pt.ref_id is null then false
	else true
end as tienda_valida) val on true
where s.fecha = '{{ds}}'::date
order by s.ultima_actualizacion, s.id_tienda desc
)
select *
from data_publicacion d
where not exists (
  select 1
  from ecommdata.publicacion_catalogo pc
  where pc.fecha_hora = d.fecha_hora
    and pc.ref_id = d.ref_id
    and pc.id_tienda = d.id_tienda
    and pc.id_bodega = d.id_bodega
))