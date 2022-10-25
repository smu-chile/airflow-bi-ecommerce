insert into operaciones_unimarc.cumplimiento_despacho
select _t.id_orden
	, _t.fecha_facturacion
	, _t.tipo_despacho
	, _t.id_tienda
	, _t.glosa as glosa_tienda
	, _t.id_transportadora
	, _t.nombre_transportadora
	, _t.fecha_despacho
	, _t.hora_despacho
	, _t.fecha_entrega::date as fecha_entrega
	, _t.fecha_entrega::time as hora_entrega
	, _t.inicio_ventana
	, _t.termino_ventana
	, _t.comuna	
	, CASE WHEN -- ANULADAS
				_t.estado_janis >= 100 THEN 0
			WHEN -- ON DAY
                _t.fecha_despacho = _t.fecha_entrega::date THEN 10
            WHEN -- EARLY DAY
                _t.fecha_despacho > _t.fecha_entrega::date THEN 20  
            WHEN -- LATE DAY
                _t.fecha_despacho < _t.fecha_entrega::date THEN 30
            WHEN -- DIA NO FINALIZADO
            	_t.fecha_despacho >= current_date THEN 40
                -- ERROR // DIA FINALIZADO SIN ENTREGA
      		ELSE 999
      END AS cumplimiento_ondate
    , CASE WHEN  -- ANULADAS
				_t.estado_janis >= 100 THEN 0
	    	WHEN -- ON TIME
				_t.fecha_despacho = _t.fecha_entrega::date
				AND _t.hora_entrega BETWEEN 
					to_char(_t.inicio_ventana, 'HH24MI')
					AND
					to_char(_t.termino_ventana + interval '20 minutes', 'HH24MI')
				THEN 10
        	WHEN-- EARLY TIME
                _t.fecha_despacho = _t.fecha_entrega::date
                AND _t.hora_entrega < to_char(_t.inicio_ventana, 'HH24MI')
                THEN 20
        	WHEN -- EARLY DATE
                _t.fecha_despacho > _t.fecha_entrega::date THEN 20
        	WHEN -- LATE TIME
                _t.fecha_despacho = _t.fecha_entrega::date
                AND _t.hora_entrega > to_char(_t.termino_ventana + interval '20 minutes', 'HH24MI')
                THEN 30
        	WHEN -- LATE DAY
                _t.fecha_despacho < _t.fecha_entrega::date THEN 30
			WHEN -- VENTANA NO FINALIZADA
            	_t.fecha_despacho = current_date
                AND to_char(_t.termino_ventana, 'HH24MI') > to_char(current_timestamp, 'HH24MI')
                THEN 40
            WHEN
            	_t.fecha_despacho > current_date
            	then 40
        -- ERROR // VENTANA O DIA FINALIZADO SIN ENTREGA
            ELSE 999
        END AS cumplimiento_ontime
FROM
(
	SELECT d.id
		, d.id_orden 
		, oj.janis_id
		, oj.estado_janis 
		, d.tipo_despacho
		, oj.fecha_facturacion
		, t.id as id_tienda
		, t.glosa 
		, t2.id as id_transportadora
		, t2.nombre as nombre_transportadora
		, d.fecha_despacho::date as fecha_despacho
		, d.fecha_despacho::time as hora_despacho
		, ocde2.fecha_creacion at time zone 'UTC' at time zone 'America/Santiago' as fecha_entrega 
		, to_char(ocde2.fecha_creacion at time zone 'UTC' at time zone 'America/Santiago', 'HH24MI') as hora_entrega 
		, d.inicio_ventana
		, d.termino_ventana
	    , d.comuna 
		, rank() over (partition by d.id_orden order by d.id desc) as _rank
	FROM ecommdata.despachos d 
	JOIN ecommdata.ordenes_janis oj 
		ON d.id_orden = oj.id 
	LEFT JOIN (select _a.id, _a.id_orden, _a.fecha_creacion
				from(
					select ocde.id, ocde.id_orden, ocde.fecha_creacion, rank() over (partition by ocde.id_orden order by ocde.id desc) as _rank1
					from ecommdata.orden_cambios_de_estado ocde
					where ocde.estado_nuevo = 90
					) _a
				where _rank1 = 1
		) ocde2
		on ocde2.id_orden = oj.janis_id
	left join ecommdata.tiendas t
		on oj.id_tienda_janis = t.id_janis 
	left join ecommdata.transportadoras t2 
		on t2.id = d.id_transportadora
	where oj.estado_janis not in (80)
	and oj.id in ({id_list})
) _t
where _t._rank = 1
on conflict (id_orden) do update
set fecha_facturacion = EXCLUDED.fecha_facturacion
	, tipo_despacho = EXCLUDED.tipo_despacho
	, id_tienda = EXCLUDED.id_tienda
	, glosa_tienda = EXCLUDED.glosa_tienda
	, id_transportadora = EXCLUDED.id_transportadora
	, nombre_transportadora = EXCLUDED.nombre_transportadora
	, fecha_despacho = EXCLUDED.fecha_despacho
	, hora_despacho = EXCLUDED.hora_despacho
	, fecha_entrega = EXCLUDED.fecha_entrega
	, hora_entrega = EXCLUDED.hora_entrega
	, inicio_ventana = EXCLUDED.inicio_ventana
	, termino_ventana = EXCLUDED.termino_ventana
	, comuna = EXCLUDED.comuna	
	, cumplimiento_ondate = EXCLUDED.cumplimiento_ondate
    , cumplimiento_ontime = EXCLUDED.cumplimiento_ontime
;
