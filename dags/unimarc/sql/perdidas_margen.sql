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
        WHEN opp.ref_id IN ('0f9e6453-2002-4846-9291-234fbf471ec6', 
                            'e0617e0a-a3af-4b34-8305-449858bd60ab', 
                            '57886947-d010-4472-99d2-2eab8b45a23c', 
                            '83c6bfd2-2424-4a80-9c0e-47b4dab6c8c6') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_diamante,
    SUM(CASE 
        WHEN opp.ref_id IN ('4bf15f33-19e9-449c-a319-108b8917216f', 
                            '71aa0c11-3d1a-43dd-bbe1-a0d0233151af', 
                            'd5a0d51b-f168-462e-bc05-70e33382147d', 
                            '256742bc-c21f-47d1-9af2-382c2de143c8') THEN opp.valor
        ELSE 0 
    END)::int AS descuento_platino,
    SUM(CASE 
        WHEN opp.ref_id IN ('6324d0ed-5976-4959-a30d-1c1a509c6cb1', 
                            'ca744b70-07b9-4c21-978f-081857689c85') THEN opp.valor
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
        WHEN (oj.fecha_facturacion::date > wp.fecha_fin_de_promocion::date
            AND wp.tipo_financiamiento NOT ILIKE '%SIN FINANCIAMIENTO%'
            AND opp.nombre NOT ILIKE '%Cupon%'
            AND opp.nombre NOT ILIKE '%Nivel%'
            AND opp.nombre NOT ILIKE '%Gcia%'
            AND opp.nombre NOT ILIKE '%Colaborador%'
            AND opp.nombre NOT ILIKE '%referido%'
            AND opp.nombre NOT ILIKE '%CLUB AHORRO%'
            AND opp.ref_id NOT IN ('57886947-d010-4472-99d2-2eab8b45a23c',
                                   '4bf15f33-19e9-449c-a319-108b8917216f',
                                   '0f9e6453-2002-4846-9291-234fbf471ec6',
                                   'cdc7f107-4132-4fef-a6ed-cd2105e2e37b',
                                   'd5a0d51b-f168-462e-bc05-70e33382147d',
                                   'bef5403a-7054-4cd7-b9a9-01f2a2f05004',
                                   'e0617e0a-a3af-4b34-8305-449858bd60ab',
                                   '71aa0c11-3d1a-43dd-bbe1-a0d0233151af',
                                   'd3f36520-2721-4dcc-b894-5e17946d6c9a',
                                   '546b51a6-cf9e-4356-8455-eb4a6e9cfef2',
                                   '5fec3887-edc7-436b-a9f8-0b577ea71a30',
                                   '86e25cbd-195b-4af4-9d5e-15d8fb53b9c7',
                                   'aacabfe4-e754-4817-ad5b-47ad67e9cffb',
                                   '6324d0ed-5976-4959-a30d-1c1a509c6cb1',
                                   '3190365c-9a53-49b1-9c66-f301ae690b73',
                                   'ad55249e-a163-459c-a433-20e4a9f038df',
                                   'ca744b70-07b9-4c21-978f-081857689c85',
                                   'cd820bb2-957d-4c5e-b59d-cc87d84710b8',
                                   '7382c46b-036e-4fcb-a3ad-41b1283f959f',
                                   '5fcc2cec-2c5c-4a40-b05d-875f0d560e6c',
                                   '3ee35c10-d6e8-407e-b39e-e098f240a9df',
                                   'c4ca8d55-d5d4-4134-b2e5-7fbf41984cc8',
                                   '83c6bfd2-2424-4a80-9c0e-47b4dab6c8c6'))
        THEN opp.valor ELSE 0
    END)::int AS descuento_general
FROM 
    ecommdata.orden_productos op
JOIN 
    ecommdata.ordenes_janis oj ON oj.id = op.id_orden
JOIN 
    ecommdata.promociones_ordenes_producto opp ON opp.id_orden_producto = op.id
LEFT JOIN 
    ecommdata.promociones wp ON wp.id_promocion = opp.id_promocion
WHERE 
    oj.fecha_facturacion >= '{ds}'::date - interval '7 day'
GROUP BY 
    oj.fecha_facturacion
