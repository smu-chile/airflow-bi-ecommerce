insert into ecommdata_alvi.transportadoras 
select ta.id_janis 
		, ta.id
		, ta.nombre 
		, ta.tipo 
		, ta.tipo_despacho 
		, ta.agendado 
		, ta.rango_maximo_despacho 
		, ta.cuota 
		, ta.estado 
		, ta.fecha_creacion 
		, ta.creado_por 
		, ta.fecha_modificacion 
		, ta.modificado_por 
		, ta.descripcion 
		, ta.integration_lock 
		, ta.fecha_modificacion_unixtime
		, max(case when b.nombre is null then null else ta.dock end) as dock
		, max(b.nombre) as nombre_dock
		, max(b.id_tienda) as id_tienda
		, ta.id_compañia_logistica
		, ta.nombre_compañia_logistica
from staging.transportadoras_alvi ta
left join ecommdata_alvi.bodegas b 
on ta.dock = b.dock 
group by ta.id_janis 
		, ta.id
		, ta.nombre 
		, ta.tipo 
		, ta.tipo_despacho 
		, ta.agendado 
		, ta.rango_maximo_despacho 
		, ta.cuota 
		, ta.estado 
		, ta.fecha_creacion 
		, ta.creado_por 
		, ta.fecha_modificacion 
		, ta.modificado_por 
		, ta.descripcion 
		, ta.integration_lock 
		, ta.fecha_modificacion_unixtime
		, ta.id_compañia_logistica
		, ta.nombre_compañia_logistica
on conflict (id) do update 
set nombre = EXCLUDED.nombre 
	, tipo = EXCLUDED.tipo 
	, tipo_despacho = EXCLUDED.tipo_despacho 
	, agendado = EXCLUDED.agendado 
	, rango_maximo_despacho = EXCLUDED.rango_maximo_despacho 
	, cuota = EXCLUDED.cuota 
	, estado = EXCLUDED.estado 
	, fecha_creacion = EXCLUDED.fecha_creacion 
	, creado_por = EXCLUDED.creado_por 
	, fecha_modificacion = EXCLUDED.fecha_modificacion 
	, modificado_por = EXCLUDED.modificado_por 
	, descripcion = EXCLUDED.descripcion 
	, integration_lock = EXCLUDED.integration_lock 
	, fecha_modificacion_unixtime= EXCLUDED.fecha_modificacion_unixtime
	, dock= EXCLUDED.dock
	, nombre_bodega = EXCLUDED.nombre_bodega
	, id_tienda = EXCLUDED.id_tienda
	, id_compañia_logistica = EXCLUDED.id_compañia_logistica
	, nombre_compañia_logistica = EXCLUDED.nombre_compañia_logistica;
