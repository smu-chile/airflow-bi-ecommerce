insert into operaciones_unimarc.found_rate_productos
select 
orden
, fecha_facturacion 
, id_producto_orden
, a.ref_id
, a.descripcion
, c.n1 as categoria_n1
, c.n2 as categoria_n2
, c.n3 as categoria_n3
, producto_substituto
, producto_substituido
, unidades_solicitadas
, unidades_pickeadas
, umv
, multiplicador_umv
, ref_id_producto_substituido
, CASE
     when producto_substituto is true then null
	-- cuando NO tiene sustituto, es categoria carne y es KG -> Completo
     when producto_substituido is false and id_categoria in (11370566,11370569,11370567,11370570,11370568,
														11370572,11370571,11370573,11370576,11370575,
														11370574,11370577,11370561,11370562,11370565,
														11370564,11370563,11370557,19380738,11370560,
														11370559,19380740,19380739,2748684948312585,
														48312586,48312587,48312588,48312589,48312590,
														48312591,48312592,48312593,48312594,48312595,
														48312596,48312597,48312598,48312599)
    								and umv = 'kg' and (unidades_pickeadas / unidades_solicitadas) >= 0.7 then 3
    -- cuando NO tiene sustituto, NO es categoria carne y es KG -> Completo
    when producto_substituido is false and umv = 'kg' and (unidades_pickeadas / unidades_solicitadas) >= 0.8 then 3
    -- cuando NO tiene sustituto y es UN -> Completo
    when producto_substituido is false and umv = 'un' and (unidades_pickeadas >= unidades_solicitadas) then 3
    -- cuando tiene sustituto -> Completo con sustitucion
    when producto_substituido is true then 2
    else 1 end as estado_foundrate
, t.id as id_tienda
, t.glosa 
, fecha_picking
, concat(admins.nombre ,' ',admins.apellido) AS pickeador
, fp.nombre as perfil_picker
, case
	when li.material is null then false
	else true
end as infaltable
, sp.mfc_is_item_side as mfc
from (
select 	distinct oj.id			as orden
	    , op.id					as id_producto_orden
	    , op.ref_id				
	    , op.descripcion			
	    , case
	    	when op.id_producto_substituido is not null
	    	then true
	    	else false
	    end as producto_substituto
	    , case
	    	when op3.id is not null
	    	then true
	    	else false
	    end as producto_substituido
	    , op.id_producto_substituido as id_producto_substituido
	    , op2.ref_id as ref_id_producto_substituido
	    , CASE
	        when op.unidad_de_medida = 'un' then op.unidades_solicitadas 
	        when op.unidad_de_medida = 'kg' then round(op.unidades_solicitadas * op.multiplicador_unidad , 4)
	        else -1
	       END as unidades_solicitadas
	    , CASE
	        when op.unidad_de_medida = 'un' then coalesce(op.unidades_pickeadas, 0) 
	        when op.unidad_de_medida = 'kg' then round(coalesce((pesables.peso_pickeado / 1000.0)::numeric, 0), 4)
	        else -1
	       END as unidades_pickeadas
	    , op.unidad_de_medida	as umv
	    , op.multiplicador_unidad as multiplicador_umv
	    , oj.id_tienda_janis as id_tienda
	    , oj.fecha_facturacion as fecha_facturacion
	    , oj.fecha_picking as fecha_picking
	    , op.id_picker
	    , p.id_categoria  
    from ecommdata.ordenes_janis oj 
    inner join ecommdata.orden_productos op on oj.id = op.id_orden
    left join ecommdata.orden_productos op2 on op2.id = op.id_producto_substituido
    left join ecommdata.orden_productos op3 on op.id = op3.id_producto_substituido
	left join 	(
		        SELECT opp.id_orden
		        , opp.id_orden_producto
		        , round(sum(opp.peso)) AS peso_pickeado
		        FROM ecommdata.orden_producto_pesables opp 
		        GROUP BY opp.id_orden, opp.id_orden_producto
		       	) pesables
	        		ON op.id = pesables.id_orden_producto
    left join ecommdata.productos p
        ON p.ref_id = op.ref_id
    where oj.fecha_facturacion is not null 
	and oj.id in {{ti.xcom_pull(key="return_value", task_ids=['get_query_order_ids_from_s3'])[0]}}
		  ) a
left join ecommdata.administradores admins on a.id_picker = admins.id
left join ecommdata.ff_perfiles fp ON admins.perfil = fp.id
left join ecommdata.categorias c on a.id_categoria = c.id
left join ecommdata.tiendas t on a.id_tienda = t.id_janis
left join ecommdata.lista_infaltables li on SUBSTRING(a.ref_id, 1, 18) = li.material
left join ecommdata.ubicacion_mfc sp on a.ref_id = CONCAT(sp.sap_code,'-', sp.measurement_unit) and t.id = sp.store
on conflict on constraint found_rate_pk
do
update set orden = excluded.orden
, fecha_facturacion = excluded.fecha_facturacion
, ref_id = excluded.ref_id
, descripcion = excluded.descripcion
, categoria_n1 = excluded.categoria_n1
, categoria_n2 = excluded.categoria_n2
, categoria_n3 = excluded.categoria_n3
, producto_substituto = excluded.producto_substituto
, producto_substituido = excluded.producto_substituido
, unidades_solicitadas = excluded.unidades_solicitadas
, unidades_pickeadas = excluded.unidades_pickeadas
, umv = excluded.umv
, multiplicador_umv = excluded.multiplicador_umv
, ref_id_producto_substituido = excluded.ref_id_producto_substituido
, estado_foundrate = excluded.estado_foundrate
, id_tienda = excluded.id_tienda
, glosa = excluded.glosa
, fecha_picking = excluded.fecha_picking
, pickeador = excluded.pickeador
, perfil_picker = excluded.perfil_picker
, infaltable = excluded.infaltable
, mfc = excluded.mfc
