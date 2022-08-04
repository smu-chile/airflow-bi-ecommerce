insert into ecommdata.sku_categorias_datawarehouse
select s.ref_id 
	, scdu.material 
	, scdu.umv 
	, scdu.grupo 
	, scdu.categoria 
	, scdu.seccion 
	, scdu.negocio 
	, scdu.linea 
from ecommdata.skus s 
left join staging.sku_categorias_datawarehouse_unimarc scdu 
	on right(split_part(s.ref_id, '-', 1), 18) = scdu.material
on conflict (ref_id) do update 
set ref_id = EXCLUDED.ref_id
	, material = EXCLUDED.material 
	, umv = EXCLUDED.umv
	, grupo = EXCLUDED.grupo
	, categoria = EXCLUDED.categoria
	, seccion = EXCLUDED.seccion
	, negocio = EXCLUDED.negocio
	, linea = EXCLUDED.linea;