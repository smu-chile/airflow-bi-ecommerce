select
	t.nombre_promocion,
	t.valido_desde as fecha_inicio_promocion,
    t.valido_hasta as fecha_fin_promocion,
    c.n2 as categoria,
    t.ref_id,
    split_part(t.ref_id,'-',1) as material,
    t.nombre_sku as descripcion_material,
    s.ean_primario::text as ean,
    split_part(t.ref_id,'-',2) as umv,
    m.nombre as marca,
    'Escala' as descripcion_promocion,
    case 
    	when pp2.precio_promocional is not null
    		then pp2.precio_promocional
    	else p2.precio
    end as precio_modal,
    case 
    	when pp2.precio_promocional is not null
    		then pp2.precio_promocional * t.cantidad_minima_sku
    	else p2.precio * t.cantidad_minima_sku
    end as precio_modal_total,
    t.precio as precio_promocional,
    t.precio * t.cantidad_minima_sku as precio_promocional_total,
    case 
    	when pp2.precio_promocional is not null
    		then (pp2.precio_promocional * t.cantidad_minima_sku) - (t.precio * t.cantidad_minima_sku)
    	else (p2.precio * t.cantidad_minima_sku) - (t.precio * t.cantidad_minima_sku)
    end as ahorro,
    CAST(ABS(
	    CASE 
	        WHEN pp2.precio_promocional IS NOT NULL THEN
	            ((CAST(t.precio AS DECIMAL(18, 2)) - CAST(pp2.precio_promocional AS DECIMAL(18, 2))) / CAST(t.precio AS DECIMAL(18, 2))) * 100
	        ELSE
	            ((CAST(t.precio AS DECIMAL(18, 2)) - CAST(p2.precio AS DECIMAL(18, 2))) / CAST(t.precio AS DECIMAL(18, 2))) * 100
	    END
	) AS INT) AS porcentaje_descuento,
	t.cantidad_minima_sku as factor,
    t.cantidad_minima_sku as cantidad_minima_sku
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ref_id ORDER BY precio) AS row_num
    FROM (
        SELECT DISTINCT
            p.ref_id,
            '' AS nombre_promocion,
            p.nombre_sku,
            FIRST_VALUE(p.precio) OVER (PARTITION BY p.ref_id, p.nombre_sku, p.valido_desde, p.valido_hasta ORDER BY p.precio) AS precio,
            FIRST_VALUE(p.cantidad_minima_sku) OVER (PARTITION BY p.ref_id, p.nombre_sku, p.valido_desde, p.valido_hasta ORDER BY p.precio) AS cantidad_minima_sku,
            p.valido_desde,
            p.valido_hasta
        FROM
            ecommdata_alvi.precios p
        LEFT JOIN
            ecommdata_alvi.productos pr ON p.ref_id = pr.ref_id
        LEFT JOIN
            ecommdata_alvi.categorias c ON pr.id_categoria = c.id
        INNER JOIN
            ecommdata_alvi.lista8 l8 ON ((l8.material::text || '-'::text) || l8.umv::text) = p.ref_id AND l8.id_tienda IN ('3193','3092')
        WHERE
            p.valido_desde >= '{ds}'::date - 60
            AND p.id_tienda_janis IN (9)
            AND c.n1 NOT IN ('No trabajar','Fizzmod Categoria')
            and p.cantidad_minima_sku > 1
        UNION  
        SELECT DISTINCT
            pp.ref_id,
            pp.nombre_promocion,
            s.nombre_sku,
            FIRST_VALUE(pp.precio_promocional) OVER (PARTITION BY pp.ref_id, s.nombre_sku, pp.fecha_inicio_promocion, pp.fecha_fin_promocion ORDER BY pp.precio_promocional) AS precio,
            FIRST_VALUE(pp.cantidad) OVER (PARTITION BY pp.ref_id, s.nombre_sku, pp.fecha_inicio_promocion, pp.fecha_fin_promocion ORDER BY pp.precio_promocional) AS cantidad_minima_sku,
            pp.fecha_inicio_promocion AS valido_desde,
            pp.fecha_fin_promocion AS valido_hasta
        FROM
            ecommdata_alvi.precios_promocionales pp
        LEFT JOIN
            ecommdata_alvi.skus s ON pp.ref_id = s.ref_id
        WHERE
            (fecha_inicio_promocion <= '{ds}'::date AND fecha_fin_promocion >= '{ds}'::date)
            and pp.cantidad > 1
    ) AS combined_data
) AS t
left join ecommdata_alvi.productos p on p.ref_id = t.ref_id
left join ecommdata_alvi.categorias c on p.id_categoria = c.id
left join ecommdata_alvi.skus s on s.ref_id = t.ref_id
left join ecommdata_alvi.marcas m on m.id = p.id_marca
left join ecommdata_alvi.precios p2 on p2.ref_id = t.ref_id and p2.id_tienda_janis IN (9) and p2.cantidad_minima_sku = 1
left join ecommdata_alvi.precios_promocionales pp2 on pp2.ref_id = t.ref_id and pp2.nombre_promocion = t.nombre_promocion and pp2.cantidad = 1
WHERE
    row_num = 1
and CAST(ABS(
	    CASE 
	        WHEN pp2.precio_promocional IS NOT NULL THEN
	            ((CAST(t.precio AS DECIMAL(18, 2)) - CAST(pp2.precio_promocional AS DECIMAL(18, 2))) / CAST(t.precio AS DECIMAL(18, 2))) * 100
	        ELSE
	            ((CAST(t.precio AS DECIMAL(18, 2)) - CAST(p2.precio AS DECIMAL(18, 2))) / CAST(t.precio AS DECIMAL(18, 2))) * 100
	    END
	) AS INT) <> 0;