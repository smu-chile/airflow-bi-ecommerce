 select s.id_tienda as store_id
, case 
	when (split_part(s.ref_id, '-', 2) not in ('KG', 'KGV')) 
		and (s2.unidades_pack  > 1)
		then (s.material::int)::varchar || '_' || s2.unidades_pack::varchar 
	else (s.material::int)::varchar
	end as id
, s.stock_vtex as stock
, p2.nombre as "name"
, s2.ean_primario as ean
, p.precio as price
, case 
	when coalesce(wf.precio_promocional, p.precio) > p.precio then p.precio
	else coalesce(wf.precio_promocional, p.precio)
	end as discount_price
, m.nombre as trademark
, case 
	when split_part(s.ref_id, '-', 2) in ('UN', 'DIS', 'PAQ') then 'U'
	when split_part(s.ref_id, '-', 2) in ('KG', 'KGV') then 'WW'
	else 'ERROR'
end as sale_type 
from  ecommdata.stock s 
left join ecommdata.tiendas t 
on s.id_tienda = t.id 
and t.rappi = TRUE
left join ecommdata.precios p 
on p.ref_id = s.ref_id
and p.id_tienda_janis = t.id_janis 
left join ecommdata.productos p2 
on p2.ref_id = s.ref_id 
left join ecommdata.marcas m 
on m.id = p2.id_marca
left join ecommdata.skus s2 
on s2.ref_id = s.ref_id 
left join (
	select wp.ean
		, wp.material
		, min(precio_promocional) as precio_promocional 
	from ecommdata.workflow_promociones wp 
	where fecha_inicio_de_promocion <= '{ds}' 
	and fecha_fin_de_promocion >= '{ds}' 
	and tipo_promocion in (1, 4)
	and registro_valido = True
	and organizacion_ventas = '1000'
	and canal_distribucion = '10'
	and id_mecanica not in (25, 26, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99)
	group by ean, material
) wf
on s2.ean_primario = wf.ean
where s.fecha = '{ds}'
and s.surtido_ecommerce is true
and s.id_tienda = '{store_id}'
and exists (
		select sn.sku_id || '-' || sn.umv 
		from ecommdata.stock_nrt sn 
		where sn.fecha_hora between '{ts}'::timestamp - interval '4 hours' and '{ts}'::timestamp
		and sn.sku_id || '-' || sn.umv = s.ref_id 
		and sn.id_tienda = s.id_tienda 
);
