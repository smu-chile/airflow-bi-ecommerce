insert into ecommdata_alvi.precios_pajaritos
SELECT 
	p.id as id,
	t.id as store,
    p.ref_id as skuRefId,
    p.cantidad_minima_sku as skuMinQuantity,
    p_pajaritos.precio as price,
    p_pajaritos.precio_lista as listPrice,
    --p.precio AS priceOG,
    --p.precio_lista as listPriceOG,
    case 
    	when p_pajaritos.costo is not null then p_pajaritos.costo 
    	else 10
    end as costPrice,
    case
        when p.valido_desde is not null then TO_CHAR(p.valido_desde, 'DD-MM-YYYY HH24:MI:SS')
        else TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
    end as validFrom,
    case
        when p.valido_hasta is not null then TO_CHAR(p.valido_hasta, 'DD-MM-YYYY HH24:MI:SS')
        else TO_CHAR(current_date, 'DD-MM-YYYY HH24:MI:SS')
    end as validTo,
    0 as locked,
    1 as updatepending,
    1 as active
--locked
--updatepending
--active
	--p.precio - p_pajaritos.precio AS diferencia_precio
FROM 
    ecommdata_alvi.precios p
LEFT JOIN 
    ecommdata_alvi.tiendas t 
    ON p.id_tienda_janis = t.id_janis
LEFT JOIN 
    ecommdata_alvi.precios p_pajaritos 
    ON p.id_sku_janis = p_pajaritos.id_sku_janis 
    AND p_pajaritos.id_tienda_janis = 9
    AND p.cantidad_minima_sku = p_pajaritos.cantidad_minima_sku
WHERE 
    t.status = 1
    AND t.id_janis != 9
    AND CURRENT_DATE BETWEEN p.valido_desde AND p.valido_hasta
    AND CURRENT_DATE BETWEEN p_pajaritos.valido_desde AND p_pajaritos.valido_hasta
    AND (p.precio != p_pajaritos.precio OR p_pajaritos.precio IS NULL);