insert into ecommdata_unimarc.planes_de_despacho 
select pddu.id
	, t.id as id_tienda
	, t.glosa as glosa_tienda
	, t2.id  as id_transportadora
	, t2.nombre as glosa_transportadora
	, pddu.cantidad
	, pddu.cuota 
	, pddu.cantidad_nuevo 
	, pddu.cantidad_solicitada
	, pddu.cantidad_pickeada 
	, pddu.cantidad_facturada 
	, pddu.cantidad_despachada 
	, pddu.cantidad_entregada 
	, pddu.fecha_inicio
	, pddu.fecha_fin 
	, pddu.editado 
	, pddu.bloqueado
	, pddu.fecha_bloqueo
	, pddu.estado 
	, pddu.fecha_creacion
	, pddu.fecha_modificacion
	, pddu.fecha_modificacion_unixtime 
from staging.planes_de_despacho_unimarc pddu
left join ecommdata.tiendas t
	on pddu.id_janis_tienda = t.id_janis 
left join ecommdata_unimarc.transportadoras t2 
	on pddu.id_transportadora = t2.id_janis  
on conflict (id) do update
set id_tienda = EXCLUDED.id_tienda
	, glosa_tienda = EXCLUDED.glosa_tienda
	, id_transportadora = EXCLUDED.id_transportadora
	, glosa_transportadora = EXCLUDED.glosa_transportadora
	, cantidad = EXCLUDED.cantidad
	, cuota = EXCLUDED.cuota 
	, cantidad_nuevo = EXCLUDED.cantidad_nuevo 
	, cantidad_solicitada = EXCLUDED.cantidad_solicitada
	, cantidad_pickeada = EXCLUDED.cantidad_pickeada 
	, cantidad_facturada = EXCLUDED.cantidad_facturada 
	, cantidad_despachada = EXCLUDED.cantidad_despachada 
	, cantidad_entregada = EXCLUDED.cantidad_entregada 
	, fecha_inicio = EXCLUDED.fecha_inicio
	, fecha_fin = EXCLUDED.fecha_fin 
	, editado = EXCLUDED.editado 
	, bloqueado = EXCLUDED.bloqueado
	, fecha_bloqueo = EXCLUDED.fecha_bloqueo
	, estado = EXCLUDED.estado 
	, fecha_creacion = EXCLUDED.fecha_creacion
	, fecha_modificacion = EXCLUDED.fecha_modificacion
	, fecha_modificacion_unixtime = EXCLUDED.fecha_modificacion_unixtime ; 
