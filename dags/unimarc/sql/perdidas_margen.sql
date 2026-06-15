WITH venta_neta AS (
	SELECT
	    oj.fecha_facturacion AS fecha,
	    TRUNC(SUM(
	        CASE 
	            WHEN opp.id_orden_producto IS NOT NULL THEN opp.precio
	            ELSE (op.precio_lista * op.unidades_pickeadas)
	        END
	    )/1.19) AS venta_total_neta_sin_descuentos,
	    TRUNC(SUM(
	        CASE 
	            WHEN opp.id_orden_producto IS NOT NULL THEN opp.precio
	            ELSE (op.precio_venta * op.unidades_pickeadas)
	        END
	    )/1.19) AS venta_total_neta
	FROM
	    ecommdata.orden_productos op
	LEFT JOIN
	    ecommdata.orden_producto_pesables opp ON op.id = opp.id_orden_producto
	LEFT JOIN
	    ecommdata.ordenes_janis oj ON oj.id = op.id_orden
	WHERE
	    op.unidades_pickeadas IS NOT NULL
	    AND oj.fecha_facturacion >= '{ds}'::date - interval '7 day'
	GROUP BY
	    oj.fecha_facturacion
),
sustituciones AS (
  SELECT
    oj.fecha_facturacion AS fecha,
    (SUM((substitute.precio_lista * substitute.unidades_pickeadas) - (original.precio_lista * original.unidades_solicitadas)) * -1) AS perdida_sustitucion
  FROM
    ecommdata.orden_productos original
  JOIN
    ecommdata.orden_productos substitute ON substitute.id_producto_substituido = original.id
  LEFT JOIN
    ecommdata.productos p ON p.ref_id = original.ref_id
  LEFT JOIN
    ecommdata.marcas m ON p.id_marca = m.id
  LEFT JOIN
    ecommdata.categorias c ON p.id_categoria = c.id
  LEFT JOIN
    ecommdata.ordenes_janis oj ON oj.id = original.id_orden
  WHERE
    original.unidades_solicitadas > 0
    AND (substitute.precio_lista * substitute.unidades_pickeadas) > (original.precio_lista * original.unidades_solicitadas)
    AND oj.fecha_facturacion >= '{ds}'::date - interval '7 day'
    AND split_part(original.ref_id, '-', 2) NOT ILIKE '%KG%'
    AND split_part(substitute.ref_id, '-', 2) NOT ILIKE '%KG%'
  GROUP BY
    oj.fecha_facturacion
)
SELECT 
    oj.fecha_facturacion AS fecha,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%Colaborador%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_colaborador,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%referido%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_referido,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%CLUB AHORRO%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_club_ahorro,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%reclamo%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_cupon_reclamo,
    SUM(CASE 
        WHEN opp.ref_id IN (
                            -- Viejos
                            '0f9e6453-2002-4846-9291-234fbf471ec6', 
                            'e0617e0a-a3af-4b34-8305-449858bd60ab', 
                            '57886947-d010-4472-99d2-2eab8b45a23c', 
                            '83c6bfd2-2424-4a80-9c0e-47b4dab6c8c6',
                            -- Nuevos
                            '44f5580a-2697-4469-aea2-77c61d599a82', 
                            'f8d74e7a-6210-4f28-8ca8-d1517f080024', 
                            '60978326-5690-4bdf-a89a-26b4de8c8ce6', 
                            '6cf20cba-b475-4272-a21b-fdb02e21f66a', 
                            '86f3374d-ccc1-4987-962d-754ec3ae9602', 
                            'f7c7c08d-35cf-4b5d-8e44-a9e30260f121', 
                            '5eed6626-6316-46a1-9fb0-7158bd65a3ce', 
                            '33ca862f-9abb-4e51-9058-12fd0a781fff') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_diamante,
    SUM(CASE 
        WHEN opp.ref_id IN (
                            -- Viejos
                            '4bf15f33-19e9-449c-a319-108b8917216f', 
                            '71aa0c11-3d1a-43dd-bbe1-a0d0233151af', 
                            'd5a0d51b-f168-462e-bc05-70e33382147d', 
                            '256742bc-c21f-47d1-9af2-382c2de143c8',
                            -- Nuevos
                            '04c8939c-68a9-4b74-9b96-ecb518574550', 
                            'a17c9a9c-8540-4c3a-9f0a-25a34a0db782', 
                            '7827f1d6-0052-412e-a8bd-0fe88d03472a', 
                            '67a6da1b-c522-4552-ab6c-e021f367a796', 
                            '353abb11-728d-4889-b088-956ed9055877', 
                            '033d0577-6fd4-4b05-8f1a-e82fca69f8bc', 
                            '8c1d88c9-39f2-4155-980b-0bde87ce05de', 
                            '50f52eda-e5ac-4088-91b0-bc0b522d8a73', 
                            'ca24a2fc-5d0e-4387-b25b-6ae224b58d10', 
                            '3c92e856-d91c-47c5-b267-f67ba0efd3e1', 
                            '9bd9ee22-3c12-471a-b639-5eb470785335') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_platino,
    SUM(CASE 
        WHEN opp.ref_id IN (
                            -- Viejos
                            '6324d0ed-5976-4959-a30d-1c1a509c6cb1', 
                            'ca744b70-07b9-4c21-978f-081857689c85',
                            -- Nuevos
                            '5ba32ada-b618-4c82-9369-696b440b5f6a', 
                            '36392468-05ae-4bed-b66a-9e78789abfa5', 
                            '12068eb5-bf36-4d4b-bc12-ab016400c534', 
                            '30cef0ae-f680-45ca-b7d9-404e1b067a9a', 
                            '15d9cd91-7923-45dc-852e-e0a66f3a3541', 
                            '29ff6db3-a65f-4fad-b48f-44b6e6ec3c40') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_oro,
    SUM(CASE 
        WHEN opp.ref_id IN ('cdc7f107-4132-4fef-a6ed-cd2105e2e37b', 
                            'bef5403a-7054-4cd7-b9a9-01f2a2f05004', 
                            'd3f36520-2721-4dcc-b894-5e17946d6c9a', 
                            '546b51a6-cf9e-4356-8455-eb4a6e9cfef2', 
                            '5fec3887-edc7-436b-a9f8-0b577ea71a30', 
                            '86e25cbd-195b-4af4-9d5e-15d8fb53b9c7', 
                            'aacabfe4-e754-4817-ad5b-47ad67e9cffb', 
                            '3190365c-9a53-49b1-9c66-f301ae690b73', 
                            'cd820bb2-957d-4c5e-b59d-cc87d84710b8', 
                            '3ee35c10-d6e8-407e-b39e-e098f240a9df', 
                            '5fcc2cec-2c5c-4a40-b05d-875f0d560e6c', 
                            '7382c46b-036e-4fcb-a3ad-41b1283f959f', 
                            'ad55249e-a163-459c-a433-20e4a9f038df', 
                            'c4ca8d55-d5d4-4134-b2e5-7fbf41984cc8') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_unipay,
    SUM(CASE 
        WHEN opp.ref_id IN ('2257d2e7-8653-4ec0-98de-b8827b30e7ec',
                            '6d89c126-19e1-4f4e-9f5c-a2a4ef85a8ba',
                            '74806be4-f768-4ee0-9ab6-3722153ae8e5') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_cupones_crm,
    SUM(CASE 
        WHEN ( oj.fecha_facturacion::date > wp.fecha_fin_de_promocion::date
        and wp.tipo_financiamiento not ilike '%SIN FINANCIAMIENTO%'
         and opp.nombre NOT ILIKE '%Cupon%'
		 and opp.nombre NOT ILIKE '%Nivel%'
		 and opp.nombre NOT ILIKE '%Gcia%'
         and opp.nombre NOT ILIKE '%Colaborador%' 
	     AND opp.nombre  not ILIKE '%referido%'
	     AND opp.nombre not ILIKE '%CLUB AHORRO%' 
	     AND opp.ref_id not IN (
                           -- Unipay
                           'cdc7f107-4132-4fef-a6ed-cd2105e2e37b',
                           'bef5403a-7054-4cd7-b9a9-01f2a2f05004',
                           'd3f36520-2721-4dcc-b894-5e17946d6c9a',
                           '546b51a6-cf9e-4356-8455-eb4a6e9cfef2',
                           '5fec3887-edc7-436b-a9f8-0b577ea71a30',
                           '86e25cbd-195b-4af4-9d5e-15d8fb53b9c7',
                           'aacabfe4-e754-4817-ad5b-47ad67e9cffb',
                           '3190365c-9a53-49b1-9c66-f301ae690b73',
                           'cd820bb2-957d-4c5e-b59d-cc87d84710b8',
                           '3ee35c10-d6e8-407e-b39e-e098f240a9df',
                           '5fcc2cec-2c5c-4a40-b05d-875f0d560e6c',
                           '7382c46b-036e-4fcb-a3ad-41b1283f959f',
                           'ad55249e-a163-459c-a433-20e4a9f038df',
                           'c4ca8d55-d5d4-4134-b2e5-7fbf41984cc8',
                           -- Nuevas Promociones Diamante
                           '44f5580a-2697-4469-aea2-77c61d599a82', 
                           'f8d74e7a-6210-4f28-8ca8-d1517f080024', 
                           '60978326-5690-4bdf-a89a-26b4de8c8ce6', 
                           '6cf20cba-b475-4272-a21b-fdb02e21f66a', 
                           '86f3374d-ccc1-4987-962d-754ec3ae9602', 
                           'f7c7c08d-35cf-4b5d-8e44-a9e30260f121', 
                           '5eed6626-6316-46a1-9fb0-7158bd65a3ce', 
                           '33ca862f-9abb-4e51-9058-12fd0a781fff',
                           -- Nuevas Promociones Platino
                           '04c8939c-68a9-4b74-9b96-ecb518574550', 
                           'a17c9a9c-8540-4c3a-9f0a-25a34a0db782', 
                           '7827f1d6-0052-412e-a8bd-0fe88d03472a', 
                           '67a6da1b-c522-4552-ab6c-e021f367a796', 
                           '353abb11-728d-4889-b088-956ed9055877', 
                           '033d0577-6fd4-4b05-8f1a-e82fca69f8bc', 
                           '8c1d88c9-39f2-4155-980b-0bde87ce05de', 
                           '50f52eda-e5ac-4088-91b0-bc0b522d8a73', 
                           'ca24a2fc-5d0e-4387-b25b-6ae224b58d10', 
                           '3c92e856-d91c-47c5-b267-f67ba0efd3e1', 
                           '9bd9ee22-3c12-471a-b639-5eb470785335',
                           -- Nuevas Promociones Oro
                           '5ba32ada-b618-4c82-9369-696b440b5f6a', 
                           '36392468-05ae-4bed-b66a-9e78789abfa5', 
                           '12068eb5-bf36-4d4b-bc12-ab016400c534', 
                           '30cef0ae-f680-45ca-b7d9-404e1b067a9a', 
                           '15d9cd91-7923-45dc-852e-e0a66f3a3541', 
                           '29ff6db3-a65f-4fad-b48f-44b6e6ec3c40',
                           -- Cupones CRM
                           '2257d2e7-8653-4ec0-98de-b8827b30e7ec',
                           '6d89c126-19e1-4f4e-9f5c-a2a4ef85a8ba',
                           '74806be4-f768-4ee0-9ab6-3722153ae8e5',
                           -- Viejas Promociones
                           '57886947-d010-4472-99d2-2eab8b45a23c',
                           '4bf15f33-19e9-449c-a319-108b8917216f',
                           '0f9e6453-2002-4846-9291-234fbf471ec6',
                           'd5a0d51b-f168-462e-bc05-70e33382147d',
                           'e0617e0a-a3af-4b34-8305-449858bd60ab',
                           '71aa0c11-3d1a-43dd-bbe1-a0d0233151af',
                           '6324d0ed-5976-4959-a30d-1c1a509c6cb1',
                           'ca744b70-07b9-4c21-978f-081857689c85',
                           '83c6bfd2-2424-4a80-9c0e-47b4dab6c8c6')
	                    	) THEN ((wp.importe_negociado)*op.unidades_pickeadas)*-1
        ELSE 0 
    END)::int AS desfase_sellout,
    coalesce(sustituciones.perdida_sustitucion, 0)::int as perdida_sustitucion,
    venta_neta.venta_total_neta_sin_descuentos::int,
    venta_neta.venta_total_neta::int
FROM 
    ecommdata.ordenes_janis oj
LEFT JOIN 
    ecommdata.orden_productos op ON oj.id = op.id_orden
LEFT JOIN 
    ecommdata.orden_producto_promociones opp ON op.id = opp.orden_producto
left join ecommdata.orden_producto_promocion_extrainfo oppe on oppe.orden_producto_promocion = opp.id and oppe.campo = 'WORKFLOWID'
LEFT JOIN 
    ecommdata.productos p ON p.ref_id = op.ref_id
LEFT JOIN 
    sustituciones ON sustituciones.fecha = oj.fecha_facturacion
LEFT JOIN
	venta_neta ON venta_neta.fecha = oj.fecha_facturacion
left join ecommdata.workflow_promociones wp on op.ref_id = ((wp.material::text || '-'::text) ||
        CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text) and oppe.valor = wp.n_promocion::text
WHERE opp.nombre not ilike '%despacho%'
    AND oj.fecha_facturacion >= '{ds}'::date - interval '7 day'
GROUP BY 
    oj.fecha_facturacion, sustituciones.perdida_sustitucion, venta_neta.venta_total_neta_sin_descuentos, venta_neta.venta_total_neta
ORDER BY 
    oj.fecha_facturacion DESC;