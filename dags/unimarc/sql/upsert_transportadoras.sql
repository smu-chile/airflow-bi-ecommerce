insert into ecommdata.transportadoras 
select tu.id_janis 
		, tu.id, tu.nombre 
		, tu.tipo 
		, tu.tipo_despacho 
		, tu.agendado 
		, tu.rango_maximo_despacho 
		, tu.cuota 
		, tu.estado 
		, tu.fecha_creacion 
		, tu.creado_por 
		, tu.fecha_modificacion 
		, tu.modificado_por 
		, tu.descripcion 
		, tu.integration_lock 
		, tu.fecha_modificacion_unixtime
		, max(case when b.nombre is null then null else tu.dock end) as dock
		, max(b.nombre) as nombre_dock
		, max(b.id_tienda) as id_tienda
		, tu.id_compañia_logistica
		, tu.nombre_compañia_logistica
from staging.transportadoras_unimarc tu
left join ecommdata.bodegas b 
on tu.dock = b.dock 
group by tu.id_janis 
		, tu.id, tu.nombre 
		, tu.tipo 
		, tu.tipo_despacho 
		, tu.agendado 
		, tu.rango_maximo_despacho 
		, tu.cuota 
		, tu.estado 
		, tu.fecha_creacion 
		, tu.creado_por 
		, tu.fecha_modificacion 
		, tu.modificado_por 
		, tu.descripcion 
		, tu.integration_lock 
		, tu.fecha_modificacion_unixtime
		, tu.id_compañia_logistica
		, tu.nombre_compañia_logistica
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
