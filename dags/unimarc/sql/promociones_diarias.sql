SELECT wp.n_promocion,
    wp.nombre_promocion,
    (((((wp.n_promocion || ' '::text) || regexp_replace(wp.nombre_promocion::text, '[^a-zA-Z0-9]'::text, ''::text, 'g'::text)) || '_'::text) ||
        CASE
            WHEN wp.id_mecanica = 13 THEN 'C003'::text
            WHEN wp.id_mecanica = 10 THEN 'C004'::text
            WHEN wp.id_mecanica = 9  THEN 'C005'::text
            WHEN wp.id_mecanica = 30 THEN 'C008'::text
            WHEN wp.id_mecanica = 12 THEN 'C011'::text
            WHEN wp.id_mecanica in (8,114,117) THEN 'C015'::text
            WHEN wp.id_mecanica = 43 THEN 'C017'::text
            WHEN wp.id_mecanica = 11 THEN 'C018'::text
            WHEN wp.nombre_promocion::text ~~ '%CYBER%' THEN 'C029'::text
            WHEN wp.id_mecanica = 57 THEN 'C031'::text
            WHEN wp.id_mecanica = 7 THEN 'C064'::text
            WHEN wp.id_evento = 400 THEN 'C065'::text
            WHEN wp.descripcion_mecanica::text ~~ '%MARCAS PROPIAS%'::text THEN 'C068'::text
            WHEN wp.descripcion_mecanica::text ~~ '%BLACK FRIDAYS%'::text THEN 'C070'::text
            WHEN wp.id_mecanica = 115 THEN 'C073'::text
            ELSE 'C011'::text
        END) || '_'::text) ||
        CASE
            WHEN wp.tipo_promocion = 7 THEN
            CASE
                WHEN wp.cantidad_n = 2 THEN 'M001__'::text
                WHEN wp.cantidad_n = 3 THEN 'M002__'::text
                WHEN wp.cantidad_n = 4 THEN 'M003__'::text
                WHEN wp.cantidad_n = 5 THEN 'M004__'::text
                WHEN wp.cantidad_n = 6 THEN 'M005__'::text
                WHEN wp.cantidad_n = 7 THEN 'M006__'::text
                WHEN wp.cantidad_n = 8 THEN 'M007__'::text
                WHEN wp.cantidad_n = 9 THEN 'M008__'::text
                WHEN wp.cantidad_n = 10 THEN 'M009__'::text
                WHEN wp.cantidad_n = 12 THEN 'M010__'::text
                WHEN wp.cantidad_n = 18 THEN 'M011__'::text
                WHEN wp.cantidad_n = 24 THEN 'M012__'::text
                WHEN wp.cantidad_n = 36 THEN 'M013__'::text
                ELSE NULL::text
            END || round(wp.precio_promocional, 0)
            WHEN wp.tipo_promocion = 1 THEN
            CASE
                WHEN wp.porcentaje_de_descuento = 0.1 THEN 'M014__'::text
                WHEN wp.porcentaje_de_descuento = 0.15 THEN 'M015__'::text
                WHEN wp.porcentaje_de_descuento = 0.2 THEN 'M016__'::text
                WHEN wp.porcentaje_de_descuento = 0.25 THEN 'M017__'::text
                WHEN wp.porcentaje_de_descuento = 0.3 THEN 'M018__'::text
                WHEN wp.porcentaje_de_descuento = 0.35 THEN 'M019__'::text
                WHEN wp.porcentaje_de_descuento = 0.4 THEN 'M020__'::text
                WHEN wp.porcentaje_de_descuento = 0.45 THEN 'M021__'::text
                WHEN wp.porcentaje_de_descuento = 0.5 THEN 'M034__'::text
                WHEN wp.porcentaje_de_descuento = ANY (ARRAY[0.6, 0.7]) THEN '___'::text
                ELSE NULL::text
            END
            WHEN wp.tipo_promocion = 4 THEN
            CASE
                WHEN wp.umv::text = ANY (ARRAY['KG'::character varying::text, 'KGV'::character varying::text]) THEN '__'::text || round(wp.precio_promocional * s.multiplicador_unidad_medida, 0)
                ELSE '_XXXX_'::text
            END
            WHEN wp.tipo_promocion = 2 THEN
            CASE
                WHEN wp.cantidad_n = 2 THEN wp.marca::text || 'M022-1__'::text
                WHEN wp.cantidad_n = 3 THEN wp.marca::text || 'M023-1__'::text
                WHEN wp.cantidad_n = 4 THEN (wp.marca::text || 'M003__'::text) || round(wp.precio_promocional, 0)
                WHEN wp.cantidad_n = 5 THEN (wp.marca::text || 'M004__'::text) || round(wp.precio_promocional, 0)
                WHEN wp.cantidad_n = 6 THEN (wp.marca::text || 'M005__'::text) || round(wp.precio_promocional, 0)
                ELSE NULL::text
            END
            WHEN wp.tipo_promocion = 8 AND wp.llevas_n = 2::numeric THEN
            CASE
                WHEN wp.porcentaje_n = 0.5 THEN 'M025-1'::text
                WHEN wp.porcentaje_n = 0.6 THEN 'M026-1'::text
                WHEN wp.porcentaje_n = 0.7 THEN 'M027-1'::text
                WHEN wp.porcentaje_n = 0.8 THEN 'M028-1'::text
                WHEN wp.porcentaje_n = 0.9 THEN 'M029-1'::text
                ELSE NULL::text
            END
            ELSE 'FALTA_COD_MEC'::text
        END AS nombre_identificador,
    wp.id_mecanica,
    wp.descripcion_mecanica,
    (wp.material::text || '-'::text) ||
        CASE
            WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
            WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
            ELSE wp.umv
        END::text AS ref_id,
    wp.descripcion_material,
    wp.marca,
    wp.tipo_promocion,
    wp.desc_promocion,
        CASE
            WHEN wp.tipo_promocion = ANY (ARRAY[1, 4]) THEN 'regular'::text
            WHEN wp.tipo_promocion = ANY (ARRAY[8, 7, 2]) THEN 'forThePriceOf'::text
            ELSE 'error'::text
        END AS mecanica,
    wp.precio_modal,
    wp.precio_promocional,
        CASE
            WHEN wp.tipo_promocion = 4 AND (wp.umv::text <> ALL (ARRAY['KG'::character varying::text, 'KGV'::character varying::text])) THEN 'lista-precio'::text
            WHEN wp.tipo_promocion = 4 AND (wp.umv::text = ANY (ARRAY['KG'::character varying::text, 'KGV'::character varying::text])) THEN round(wp.precio_promocional * s.multiplicador_unidad_medida, 0)::text
            WHEN wp.tipo_promocion = ANY (ARRAY[1, 2, 8]) THEN '0'::text
            WHEN wp.tipo_promocion = 7 THEN round(wp.precio_promocional, 0)::text
            ELSE 'err'::text
        END AS precio,
    wp.cantidad_n,
    wp.cantidad_m,
    wp.llevas_n,
    round(wp.porcentaje_n * 100::numeric, 0) AS porcentaje_n,
    wp.fecha_inicio_de_promocion,
    wp.fecha_fin_de_promocion,
    round(wp.porcentaje_de_descuento * 100::numeric, 0) AS porcentaje_de_descuento,
    wp.fecha_modificacion,
    wp.factor,
    s.vtex_id,
    s.nombre_sku,
    s.id_producto,
    pdvd.idcalculatorconfigurator,
    pdvd.nombre_promocion_vtex,
    pdvd.link_promocion
    FROM ecommdata.workflow_promociones wp
        LEFT JOIN ecommdata.skus s ON s.ref_id::text = ((wp.material::text || '-'::text) ||
            CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text)
        LEFT JOIN ecommdata.lista8 l8 ON ((l8.material::text || '-'::text) || l8.umv::text) = ((wp.material::text || '-'::text) ||
            CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text)
        LEFT JOIN ecommdata.stock_mfc sm
			ON (LPAD(sm.material::text, 18, '0') || '-' || sm.unidad_venta ::text) = ((wp.material::text || '-' || CASE
			    WHEN wp.umv::text = 'ST' THEN 'UN'
			    WHEN wp.umv::text = 'CS' THEN 'CJ'
			    ELSE wp.umv
			END) ::text)
        left join (select coalesce(lpv."SKU ID"::numeric,pdv.vtex_id_sku::numeric) as id_vtex,
                    pdv.id as idcalculatorconfigurator,
                    pdv.nombre_promocion as nombre_promocion_vtex,
                    concat('https://unimarc.myvtex.com/admin/promotions/',pdv.id) as link_promocion
                    from ecommdata.promociones_detalle_vtex pdv
                    left join ecommdata.promociones_vtex pv on pv.id = pdv.id
                    LEFT JOIN catalogo.listas_precios_vtex lpv ON pdv.tabla_nombre_precio = lpv."Trade Policy"
                    where pdv.archivado is false
                    and pv.estado = 'active') as pdvd on pdvd.id_vtex = s.vtex_id and split_part(pdvd.nombre_promocion_vtex,' ', 1)::text = wp.n_promocion::text
    WHERE (wp.id_mecanica <> ALL (ARRAY[36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59]))
    AND wp.fecha_inicio_de_promocion <= '{ds}'::date + 1
    AND wp.fecha_fin_de_promocion >= '{ds}'::date
    and wp.tipo_promocion <> 3
    AND wp.nombre_promocion::text !~~ '%MFC%'::text
    AND wp.nombre_promocion::text !~~ '%BANCO ESTADO%'::text
    AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text 
    AND wp.nombre_promocion::text !~~ '%917%'::text
    and wp.nombre_promocion::text !~~ '% LOC%'::text
    and s.vtex_id <> ALL (ARRAY[3610,471,3611,472,473,658,82183,82184,39730])
    and s.vtex_id IS NOT null
    and
    (
        ((l8.material::text || '-' || l8.umv::text) IS NOT NULL)
        OR
        (sm.stock >= 1)
    )
    GROUP BY wp.n_promocion, wp.nombre_promocion, wp.id_evento,wp.id_mecanica, wp.descripcion_mecanica, wp.material, s.ref_id, wp.umv, wp.descripcion_material, wp.marca, wp.tipo_promocion, wp.desc_promocion, wp.precio_modal, wp.precio_promocional, s.multiplicador_unidad_medida, wp.ahorro, wp.cantidad_n, wp.cantidad_m, wp.llevas_n, wp.porcentaje_n, wp.fecha_inicio_de_promocion, wp.fecha_fin_de_promocion, wp.porcentaje_de_descuento, wp.fecha_modificacion, wp.factor, s.vtex_id, s.nombre_sku, s.id_producto, pdvd.idcalculatorconfigurator, pdvd.nombre_promocion_vtex, pdvd.link_promocion
    ORDER BY wp.precio_promocional, wp.fecha_fin_de_promocion DESC;