begin transaction;
truncate table ecommdata_alvi.producto_tienda_excluidos;
insert into ecommdata_alvi.producto_tienda_excluidos
select material,umv, concat(material,'-',umv) as ref_id, 1 as all_stores,null::varchar as id_tienda,fecha_carga  
from catalogo.productos_excluidos_alvi as pe
order by fecha_carga desc;
commit;