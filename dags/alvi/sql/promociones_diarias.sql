SELECT DISTINCT
            wp.n_promocion,
            wp.nombre_promocion,
            wp.id_mecanica,
            wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion,
            wp.tipo_promocion,
            wp.porcentaje_de_descuento,
            wp.precio_modal,
            wp.precio_promocional,
            wp.precio_promocional_2,
            wp.factor,
            wp.factor_2,
            wp.desc_promocion,
            s.ref_id,
            s.vtex_id,
            pdvd.idcalculatorconfigurator,
    		pdvd.nombre_promocion_vtex,
    		pdvd.link_promocion
        FROM ecommdata_alvi.workflow_promociones wp
        LEFT JOIN ecommdata_alvi.skus s ON s.ref_id::text = ((wp.material::text || '-'::text) ||
            CASE
                WHEN wp.umv::text = 'ST'::text THEN 'UN'::character varying
                WHEN wp.umv::text = 'CS'::text THEN 'CJ'::character varying
                ELSE wp.umv
            END::text)
       	left join (select coalesce(lpv."SKU ID"::numeric,pdv.vtex_id_sku::numeric) as id_vtex,
                    pdv.id as idcalculatorconfigurator,
                    pdv.nombre_promocion as nombre_promocion_vtex,
                    concat('https://alvicl.myvtex.com/admin/promotions/',pdv.id) as link_promocion
                    from ecommdata_alvi.promociones_detalle_vtex pdv
                    LEFT JOIN ecommdata_alvi.listas_precios_vtex lpv ON pdv.tabla_nombre_precio = lpv."Trade Policy") as pdvd 
                    on pdvd.id_vtex = s.vtex_id and split_part(pdvd.nombre_promocion_vtex,' ', 1)::text = wp.n_promocion::text
        WHERE wp.tipo_promocion IN (10,9,4)
        and wp.id_mecanica in (18,83,40,26)
        and wp.n_promocion not in (9920092021)--Promociones excluidas y la XXXXX
        and wp.nombre_promocion not ilike '%LOCAL%'
        and wp.nombre_promocion not ilike '%LIQUIDACION%' 
        and wp.nombre_promocion not ilike '%TERMINAL%' 
        and wp.nombre_promocion not ilike '%LOC%'
        and wp.nombre_promocion not ilike '%ANDINA%'
        and wp.nombre_promocion not ilike '%EMBONOR%'
        and wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
        and s.vtex_id is not null
        and wp.precio_promocional > 0
        AND wp.fecha_inicio_de_promocion <= '{ds}'::date + interval '1 day'
        AND wp.fecha_fin_de_promocion >= '{ds}'::date + interval '1 day'
        ORDER BY wp.precio_promocional, wp.fecha_fin_de_promocion DESC;