WITH ReferenceRow AS (
  SELECT
    descripcion_mecanica AS coleccion,
    fecha_inicio_de_promocion AS min_start_date,
    fecha_fin_de_promocion AS max_end_date
  FROM
    ecommdata.workflow_promociones
  WHERE
    id_evento = 553
    AND fecha_fin_de_promocion >= '{ds}'::date
    AND fecha_inicio_de_promocion <= '{ds}'::date
  ORDER BY
    fecha_inicio_de_promocion
  LIMIT 1
),
RankedPromotions AS (
  SELECT
    wp.*,
    rr.coleccion,
    ROW_NUMBER() OVER (PARTITION BY material, umv ORDER BY precio_promocional ASC) AS row_num
  FROM
    ecommdata.workflow_promociones wp
  LEFT JOIN ReferenceRow rr ON 1=1
  WHERE
    wp.fecha_fin_de_promocion >= rr.min_start_date
    AND wp.fecha_inicio_de_promocion <= rr.max_end_date
    AND wp.fecha_fin_de_promocion >= '{ds}'::date
    AND wp.fecha_inicio_de_promocion <= '{ds}'::date
    AND wp.nombre_promocion::text !~~ '%MFC%'::text 
  	AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text 
  	AND wp.nombre_promocion::text !~~ '%917%'::text
  	AND wp.nombre_promocion::text !~~ '%977%'::text
  	AND wp.nombre_promocion::text !~~ '%0743%'::text
  	and wp.nombre_promocion::text !~~ '% LOC%'::text
  	and wp.nombre_promocion::text !~~ '%BANCO ESTADO%'::text
  	and wp.id_evento <> 551
    AND wp.id_mecanica <> ALL (ARRAY[36, 67, 72, 99, 84, 12, 37, 51, 93, 53, 96, 77, 59])
)
SELECT
  (coleccion::text || '_GENERICA_' || TO_CHAR('{ds}'::date, 'DDMMYYYY')) as nombre_coleccion,
  wp.n_promocion,
  wp.nombre_promocion,
  wp.id_evento,
  wp.descripcion_evento_promocional,
  wp.id_mecanica,
  wp.descripcion_mecanica,
  (wp.material::text || '-'::text) ||
        CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text AS ref_id,
  s.nombre_sku,
  s.vtex_id,
  wp.descripcion_linea,
  wp.fecha_inicio_de_promocion,
  wp.fecha_fin_de_promocion,
  rp.ranking
FROM
  RankedPromotions wp
INNER JOIN ecommdata.ranking_productos rp ON rp.ref_id_sku = ((wp.material::text || '-'::text) ||
  CASE
    WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
    WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
    ELSE wp.umv
  END::text)
LEFT JOIN ecommdata.skus s ON s.ref_id::text = ((wp.material::text || '-'::text) ||
        CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text)
WHERE
  wp.row_num = 1
ORDER BY rp.ranking
LIMIT 1000;