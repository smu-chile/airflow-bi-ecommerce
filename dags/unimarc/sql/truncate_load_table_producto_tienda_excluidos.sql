begin transaction;

truncate table ecommdata.producto_tienda_excluidos;

insert into ecommdata.producto_tienda_excluidos
select material,umv, concat(material,'-',umv) as ref_id, 1 as all_stores,0 as is_mfc,null::varchar as id_tienda,fecha_carga  
from catalogo.productos_excluidos pe 
where not exists (
    select 1 
    from catalogo.productos_excluidos_excepciones ex
    where pe.material = ex.material 
      and pe.umv = ex.umv
)
order by fecha_carga desc;

insert into ecommdata.producto_tienda_excluidos
select split_part(smt.tom_id,'-',1) as material,split_part(smt.tom_id,'-',2) as umv,
smt.tom_id as ref_id, 0 as all_stores, 1 as is_mfc,'1917' as id_tienda, smt.fecha as fecha_carga
from ecommdata.stock_mfc_takeoff smt
left join(
	select split_part(tom_id,'-',1) as material, count(*) as contador
	from ecommdata.stock_mfc_takeoff smt
	where fecha::date = '{{ds}}'::date+1
	group by split_part(tom_id,'-',1)) as _t
	on _t.material = split_part(smt.tom_id,'-',1)
where _t.contador >1
and _t.material is not null
and smt.fecha::date >= '{{ds}}'::date+1
and smt.quantity_on_hand = 0;

insert into ecommdata.producto_tienda_excluidos
select material, umv, ref_id, all_stores, is_mfc, id_tienda, fecha_carga
from catalogo.productos_excluidos_x_tienda pet;

commit;