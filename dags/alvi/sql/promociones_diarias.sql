WITH promos_ranked AS (
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
        wp.material,
        pdvd.idcalculatorconfigurator,
        pdvd.nombre_promocion_vtex,
        pdvd.link_promocion,
        ROW_NUMBER() OVER (
            PARTITION BY wp.material 
            ORDER BY wp.precio_promocional ASC, wp.fecha_fin_de_promocion DESC
        ) AS rn
    FROM ecommdata_alvi.workflow_promociones wp
    LEFT JOIN ecommdata_alvi.skus s 
        ON s.ref_id::text = (
            wp.material::text || '-' ||
            CASE
                WHEN wp.umv::text = 'ST' THEN 'UN'
                WHEN wp.umv::text = 'CS' THEN 'CJ'
                ELSE wp.umv
            END::text
        )
    LEFT JOIN (
        SELECT 
            COALESCE(lpv."SKU ID"::numeric, pdv.vtex_id_sku::numeric) AS id_vtex,
            pdv.id AS idcalculatorconfigurator,
            pdv.nombre_promocion AS nombre_promocion_vtex,
            CONCAT('https://alvicl.myvtex.com/admin/promotions/', pdv.id) AS link_promocion
        FROM ecommdata_alvi.promociones_detalle_vtex pdv
        LEFT JOIN ecommdata_alvi.listas_precios_vtex lpv 
            ON pdv.tabla_nombre_precio = lpv."Trade Policy"
    ) pdvd 
        ON pdvd.id_vtex = s.vtex_id 
        AND split_part(pdvd.nombre_promocion_vtex, ' ', 1)::text = wp.n_promocion::text
    WHERE wp.tipo_promocion IN (10, 9, 4)
      AND wp.id_mecanica IN (18, 83, 40, 26)
      AND wp.n_promocion NOT IN (9920092021, 9960782024)
      AND wp.nombre_promocion NOT ILIKE '%LOCAL%'
      AND wp.nombre_promocion NOT ILIKE '%LIQUIDACION%' 
      AND wp.nombre_promocion NOT ILIKE '%TERMINAL%' 
      AND wp.nombre_promocion NOT ILIKE '%LOC%'
      AND wp.nombre_promocion NOT ILIKE '%ANDINA%'
      AND wp.nombre_promocion NOT ILIKE '%EMBONOR%'
      AND wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
      AND wp.precio_promocional > 0
      AND wp.fecha_inicio_de_promocion <= current_date + interval '1 day'
      AND wp.fecha_fin_de_promocion >= current_date + interval '1 day'
      AND s.vtex_id IS NOT NULL
)
SELECT *
FROM promos_ranked
WHERE rn = 1
ORDER BY precio_promocional ASC;
