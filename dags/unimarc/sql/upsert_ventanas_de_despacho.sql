insert into ecommdata_unimarc.ventanas_de_despacho 
select vddu.id
	, t.id as id_tienda
	, t.glosa as glosa_tienda
	, t2.id  as id_transportadora
	, t2.nombre as glosa_transportadora
	, vddu.cantidad
	, vddu.cuota 
	, vddu.cantidad_nuevo 
	, vddu.cantidad_en_picking
	, vddu.cantidad_pickeada 
	, vddu.cantidad_facturada 
	, vddu.cantidad_despachada 
	, vddu.cantidad_entregada 
	, vddu.fecha_inicio
	, vddu.fecha_fin 
	, vddu.editado 
	, vddu.bloqueado
	, vddu.fecha_bloqueo
	, vddu.estado 
	, vddu.fecha_creacion
	, vddu.fecha_modificacion
	, vddu.fecha_modificacion_unixtime 
from staging.ventanas_de_despacho_unimarc vddu
left join ecommdata.tiendas t
	on vddu.id_janis_tienda = t.id_janis 
left join ecommdata_unimarc.transportadoras t2 
	on vddu.id_transportadora = t2.id_janis  
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
