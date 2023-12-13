SELECT
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
            wp.material || '-' || CASE
                WHEN wp.umv = 'ST' THEN 'UN'
                WHEN wp.umv = 'CS' THEN 'CJ'
            END AS ref_id,
            null as idcalculatorconfigurator
        FROM ecommdata_alvi.workflow_promociones wp
        WHERE wp.tipo_promocion IN (10)
        and wp.id_mecanica in (18,83,40)
        AND wp.fecha_inicio_de_promocion <= '{ds}'::date + interval '1 day'
        AND wp.fecha_fin_de_promocion >= '{ds}'::date + interval '1 day'
        ORDER BY wp.precio_promocional, wp.fecha_fin_de_promocion DESC;