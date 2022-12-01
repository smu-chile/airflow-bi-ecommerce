select s.id_tienda as store_id
	, s.material as id
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
from ecommdata.stock s 
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
		and id_evento not in (105, 555)
		group by ean, material
) wf
	on s.material = wf.material 
where s.fecha = '{ds}'
and s.surtido_ecommerce is true
and p.precio is not null;
