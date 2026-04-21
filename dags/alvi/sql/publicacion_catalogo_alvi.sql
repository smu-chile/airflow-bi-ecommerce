DELETE FROM ecommdata_alvi.publicacion_catalogo WHERE fecha_hora = '{{ts}}' at time zone 'America/Santiago' + interval '4 hours';
insert into ecommdata_alvi.publicacion_catalogo
select s.ultima_actualizacion as fecha_hora
, s.material
, s.ref_id
, s.descripcion 
, s.c1
, s.c2
, s.id_tienda 
, s.id_bodega
, m.nombre as marca
, val.foto_valida
, foto.q_foto as cantidad_foto
, foto.foto_en_preparacion
, val.categoria_valida
, val.stock_valido
, val.tienda_valida
, case
	when val.foto_valida and val.categoria_valida and tienda_valida is true then true 
	else false
end as publicacion_valida
, case
	when val.foto_valida and val.categoria_valida and val.stock_valido and tienda_valida is true then true 
	else false
end as disponible_web
, s.stock_janis
, s.stock_seguridad_janis
, s.stock_infinito_janis
, s.stock_vtex
, s.stock_reservado_vtex
, s.stock_infinito_vtex
, s.surtido_ecommerce
from ecommdata_alvi.stock s
left join (select t1.ref_id
	, t1.q_foto
	, case
		when t2.ref_id is null then false
		else true
	end as foto_en_preparacion
	from (select isku.ref_id, count(1) as q_foto
	from ecommdata_alvi.imagenes_sku isku
	group by isku.ref_id) t1
	left join (select isku.ref_id
	from ecommdata_alvi.imagenes_sku isku
	where isku.imagen ilike any(array['%foto-en%','%foto-unimarc%'])) t2 
on t1.ref_id = t2.ref_id) foto on s.ref_id = foto.ref_id
left join ecommdata_alvi.productos p on s.ref_id = p.ref_id
left join ecommdata_alvi.categorias c on p.id_categoria = c.id
left join ecommdata_alvi.tiendas t on s.id_tienda = t.id
left join ecommdata_alvi.productos_tienda pt on s.ref_id = pt.ref_id and s.id_tienda = pt.id_tienda
left join ecommdata_alvi.marcas m on p.id_marca = m.id
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
	when pt.ref_id is null then false
	else true
end as tienda_valida) val on true
where s.fecha = '{{ds}}'::date
order by s.ultima_actualizacion, s.id_tienda desc;
