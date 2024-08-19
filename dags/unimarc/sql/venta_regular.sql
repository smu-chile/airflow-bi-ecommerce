BEGIN TRANSACTION;
truncate table ecommdata.venta_regular_mfc;
insert into ecommdata.venta_regular_mfc
select ved.ref_id_sku AS ref_id,
    ved.id_tienda,
    ved.fecha_facturacion::date AS fecha_facturacion,
    ved.venta_umv,
    op.precio_lista,
    op.precio,
    op.precio_venta,
    op.precio_venta_original,
    CASE
        WHEN split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['UN'::text, 'DIS'::text]) THEN 1::numeric - op.precio_venta / op.precio
        WHEN split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['KG'::text, 'KGV'::text]) THEN 1::numeric - op.precio_venta / ved.venta_umv / op.precio
        ELSE NULL::integer::numeric
    END AS porcenta_descuento,
    case 
    	when split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['UN'::text, 'DIS'::text]) and 1::numeric - op.precio_venta / op.precio >= 0.34 then true 
    	when split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['UN'::text, 'DIS'::text]) and 1::numeric - op.precio_venta / op.precio < 0.34 then false 
    	when split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['KG'::text, 'KGV'::text]) and 1::numeric - op.precio_venta / ved.venta_umv / op.precio >= 0.34 then true
    	when split_part(ved.ref_id_sku::text, '-'::text, 2) = ANY (ARRAY['KG'::text, 'KGV'::text]) and 1::numeric - op.precio_venta / ved.venta_umv / op.precio < 0.34 then true
    	else false 
    end AS apo
FROM ecommdata.ventas_ecommerce_datawarehouse ved
LEFT JOIN ecommdata.ordenes_janis oj ON oj.id = ved.id_orden
LEFT JOIN ecommdata.orden_productos op ON op.id_orden = oj.id AND op.ref_id::text = ved.ref_id_sku::text
WHERE ved.id_tienda::text = '1917'::text AND ved.fecha_facturacion::date >= '{{ds}}'::date - 90 AND oj.id IS NOT NULL AND op.ref_id IS NOT NULL;
COMMIT