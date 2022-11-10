select s.id_tienda as store_id
	, s.material as id
	, s.stock_vtex as stock
	, p2.nombre as "name"
	, s2.ean_primario as ean
	, p.precio as price
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
where s.fecha = {ds}
and s.surtido_ecommerce is true
and p.precio is not null;
