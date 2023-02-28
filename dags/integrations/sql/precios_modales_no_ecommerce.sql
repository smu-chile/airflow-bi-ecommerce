with productos_lista8 as (
	select material || '-' || umv as ref_id
	from ecommdata.lista8 l 
	group by material || '-' || umv
)
select p.ref_id
	, split_part(p.ref_id, '-', 1) as material 
	, split_part(p.ref_id, '-', 2) as umv
	, max(p.precio) as precio
from ecommdata.precios p 
join productos_lista8 l 
	on l.ref_id = p.ref_id 
group by p.ref_id;
