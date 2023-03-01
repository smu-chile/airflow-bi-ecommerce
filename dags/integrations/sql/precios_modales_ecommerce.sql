select t.id as id_tienda
	, p.ref_id
	, split_part(p.ref_id, '-', 1) as material 
	, split_part(p.ref_id, '-', 2) as umv
	, p.precio
from ecommdata.precios p 
join ecommdata.tiendas t 
	on p.id_tienda_janis = t.id_janis 
join ecommdata.lista8 l 
	on l.material || '-' || l.umv = p.ref_id 
	and l.id_tienda = t.id 
where t.id in ('{store_id}');
