begin transaction;
truncate table ecommdata_alvi.producto_tienda_excluidos;
insert into ecommdata.producto_tienda_excluidos
select material,umv, concat(material,'-',umv) as ref_id, 1 as all_stores,0 as is_mfc,null::varchar as id_tienda,fecha_carga  
from catalogo.productos_excluidos pe 
order by fecha_carga desc;
commit;