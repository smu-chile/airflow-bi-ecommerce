insert into ecommdata.publicacion_catalogo
select s.ultima_actualizacion as fecha_hora
, s.material
, s.ref_id
, s.descripcion 
, s.c1
, s.c2
, s.c3
, s.id_tienda 
, s.id_bodega
, m.nombre as marca
, case 
	when foto.ref_id is null then false
	else true
end as foto_valida
, foto.q_foto as cantidad_foto
, foto.foto_en_preparacion
, case 
	when c.status = 'activo' then true
	else false
end as categoria_valida
, case
	when s.stock_disponible_vtex > 0 then true
	else false
end as stock_valido
, case
	when pr.id is null then false
	else true
end as precio_valido
, case 
	when pt.ref_id is null then false
	else true
end as tienda_valida
, case
	when t300.ref_id is null then false
	else true
end as top300
, t300.evento
, s.stock_janis
, s.stock_seguridad_janis
, s.stock_infinito_janis
, s.stock_vtex
, s.stock_reservado_vtex
, s.stock_infinito_vtex
, s.surtido_ecommerce
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
left join ecommdata.precios pr on t.id_janis = pr.id_tienda_janis and s.ref_id = pr.ref_id and pr.fecha_carga::date = current_date
left join ecommdata.productos_tienda pt on s.ref_id = pt.ref_id and s.id_tienda = pt.id_tienda
left join ecommdata.marcas m on p.id_marca = m.id
left join ecommdata.productos_top300 t300 on s.ref_id = t300.ref_id
where s.fecha = '{{ds}}'::date
order by s.ultima_actualizacion, s.id_tienda desc;
