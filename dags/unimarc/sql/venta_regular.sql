BEGIN TRANSACTION;
truncate table ecommdata.venta_regular_mfc;
insert into ecommdata.venta_regular_mfc
 WITH filtered_promotions AS (
         SELECT op_1.id AS op_id,
            opp.id AS opp_id,
            opp.nombre,
            oppe.campo,
            row_number() OVER (PARTITION BY op_1.id ORDER BY opp.id DESC) AS rn
           FROM ecommdata.orden_productos op_1
             LEFT JOIN ecommdata.orden_producto_promociones opp ON opp.orden_producto = op_1.id
             LEFT JOIN ecommdata.orden_producto_promocion_extrainfo oppe ON oppe.orden_producto_promocion = opp.id
          WHERE opp.nombre::text !~~ '%cupon%'::text AND opp.nombre::text !~~ '%Cupon%'::text AND opp.nombre::text !~~ '%Despacho%'::text AND opp.nombre::text !~~ '%Liquidacion%'::text AND opp.nombre::text !~~ '%Referido%'::text AND opp.nombre::text !~~ '%Descuento%'::text AND opp.nombre::text !~~ '%Colaborador%'::text
        ), unique_promotions AS (
         SELECT filtered_promotions.op_id,
            filtered_promotions.opp_id,
            filtered_promotions.nombre,
            filtered_promotions.campo,
            filtered_promotions.rn
           FROM filtered_promotions
          WHERE filtered_promotions.rn = 1
        )
 SELECT ved.ref_id_sku AS ref_id,
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
        CASE
            WHEN up.campo::text ~~ '%WORK%'::text THEN true
            ELSE false
        END AS apo
   FROM ecommdata.ventas_ecommerce_datawarehouse ved
     LEFT JOIN ecommdata.ordenes_janis oj ON oj.id = ved.id_orden
     LEFT JOIN ecommdata.orden_productos op ON op.id_orden = oj.id AND op.ref_id::text = ved.ref_id_sku::text
     LEFT JOIN unique_promotions up ON up.op_id = op.id
  WHERE ved.id_tienda::text = '1917'::text AND ved.fecha_facturacion::date >= ('{{ds}}'::date - 90) AND oj.id IS NOT NULL AND op.ref_id IS NOT NULL;
COMMIT