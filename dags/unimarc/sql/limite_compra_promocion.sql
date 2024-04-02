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
LEFT JOIN ecommdata.skus s ON s.ref_id::text = ((wp.material::text || '-'::text) ||
            CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text)
left join ecommdata.productos p ON p.ref_id::text = ((wp.material::text || '-'::text) ||
            CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text)
left join ecommdata.categorias c on c.id = p.id_categoria
where (wp.id_mecanica <> ALL (ARRAY[36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59]))
AND wp.fecha_inicio_de_promocion <= '{ds}'::date + 1
AND wp.fecha_fin_de_promocion >= '{ds}'::date -1 
and wp.tipo_promocion <> 3
and c.n1 not in ('Carnes')
and s.vtex_id is not null
AND wp.nombre_promocion::text !~~ '%MFC%'::text
AND wp.nombre_promocion::text !~~ '%S06%'::text
AND wp.nombre_promocion::text !~~ '%NO ELIMINAR%'::text 
AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text 
AND wp.nombre_promocion::text !~~ '%917%'::text
AND wp.nombre_promocion::text !~~ '%0743%'::text
and wp.nombre_promocion::text !~~ '% LOC%'::text
and wp.nombre_promocion::text !~~ '%L65%'::text
and wp.nombre_promocion::text !~~ '%L0089%'::text;