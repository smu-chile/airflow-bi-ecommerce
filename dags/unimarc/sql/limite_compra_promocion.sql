select (wp.material::text || '-'::text) ||
	        CASE
	            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
	            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
	            ELSE wp.umv
	        END::text AS ref_id,
	wp.n_promocion,
	wp.nombre_promocion,
	abs(wp.precio_promocional - wp.precio_modal) as descuento_pesos,
	TRUNC(wp.porcentaje_descuento_final * 100) as porcentaje_descuento,
	wp.fecha_inicio_de_promocion,
	wp.fecha_fin_de_promocion
from ecommdata.workflow_promociones wp 
where (abs(wp.precio_promocional - wp.precio_modal) >= 5000 or wp.porcentaje_descuento_final >= 0.35)
and (wp.id_mecanica <> ALL (ARRAY[36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59]))
AND wp.fecha_inicio_de_promocion <= '{ds}'::date + 1
AND wp.fecha_fin_de_promocion >= '{ds}'::date -1 
and wp.tipo_promocion <> 3
AND wp.nombre_promocion::text !~~ '%MFC%'::text
AND wp.nombre_promocion::text !~~ '%S06%'::text
AND wp.nombre_promocion::text !~~ '%NO ELIMINAR%'::text 
AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text 
AND wp.nombre_promocion::text !~~ '%917%'::text
AND wp.nombre_promocion::text !~~ '%0743%'::text
and wp.nombre_promocion::text !~~ '% LOC%'::text
and wp.nombre_promocion::text !~~ '%L65%'::text
and wp.nombre_promocion::text !~~ '%L0089%'::text;