insert into ecommdata_alvi.monitor_despacho 
select mda.id
	, t.id as id_tienda
	, t.glosa as glosa_tienda
	, t2.id  as id_transportadora
	, t2.nombre as glosa_transportadora
	, mda.cantidad
	, mda.cuota 
	, mda.cantidad_nuevo 
	, mda.cantidad_en_picking
	, mda.cantidad_pickeada 
	, mda.cantidad_facturada 
	, mda.cantidad_despachada 
	, mda.cantidad_entregada 
	, mda.fecha_inicio
	, mda.fecha_fin 
	, mda.editado 
	, mda.bloqueado
	, mda.fecha_bloqueo
	, mda.estado 
	, mda.fecha_creacion
	, mda.fecha_modificacion
	, mda.fecha_modificacion_unixtime 
from staging.monitor_despacho_alvi mda
left join ecommdata_alvi.tiendas t
	on mda.id_janis_tienda = t.id_janis 
left join ecommdata_alvi.transportadoras t2 
	on mda.id_transportadora = t2.id_janis  
on conflict (id) do update
set id_tienda = EXCLUDED.id_tienda
	, glosa_tienda = EXCLUDED.glosa_tienda
	, id_transportadora = EXCLUDED.id_transportadora
	, glosa_transportadora = EXCLUDED.glosa_transportadora
	, cantidad = EXCLUDED.cantidad
	, cuota = EXCLUDED.cuota 
	, cantidad_nuevo = EXCLUDED.cantidad_nuevo 
	, cantidad_en_picking = EXCLUDED.cantidad_en_picking
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
