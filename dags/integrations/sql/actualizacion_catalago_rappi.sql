select 
case
	when selling_units>1 and clm.umv <> 'UN' then concat(sku,'_',selling_units)
	else sku
end as sku,
ean,
clm.nombre as name,
clm.quantity ,
clm.unit_type ,
clm.selling_units ,
clm.is_weightable ,
'VERDADERO' as is_prepackaged,
clm.imagen ,
clm.categoria_n3
from integraciones.catalogo_last_millers clm 
