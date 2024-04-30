insert into ventas_unimarc.detalle_promociones
SELECT oj.fecha_creacion,
    oj.fecha_facturacion,
    oj.canal_venta,
    oj.id AS seq_id,
    oj.id_cliente_janis,
    t.id AS id_tienda,
    t.glosa,
    c.n1,
    c.n2,
    c.n3,
    op.ref_id,
    op.descripcion,
    op.precio_lista,
    op.precio_venta,
    op.unidades_solicitadas,
    opp.cantidad AS unidades_aplica_promo,
    op.precio_venta * op.unidades_solicitadas::numeric AS venta_bruta,
    opp.nombre AS nombre_promo,
    opp.valor AS ahorro_promo,
        CASE
            WHEN (opp.nombre::text <> ALL (ARRAY['Descuento 10% boleta app'::character varying::text, 'Descuento Colaborador Unimarc 10%'::character varying::text])) AND opp.nombre::text !~~* '%despac%'::text AND opp.nombre::text !~~* '%flet%'::text THEN opp.valor
            ELSE 0
        END AS ahorro_producto,
        CASE
            WHEN opp.nombre::text ~~* '%despac%'::text OR opp.nombre::text ~~* '%flet%'::text THEN opp.valor
            ELSE 0
        END AS ahorro_delivery,
        CASE
            WHEN opp.nombre::text = ANY (ARRAY['Descuento 10% boleta app'::character varying::text, 'Descuento Colaborador Unimarc 10%'::character varying::text, 'cupon dcto 5mil atraso pedido'::character varying::text, 'Cupon Prefuga 10 x 100'::character varying::text, 'Cupon Prefuga 15 x 150'::character varying::text, 'Cupon Fuga 5 x 20'::character varying::text, 'Cupon Fuga 5 x 30'::character varying::text, 'Cupon Fuga 5 x 60'::character varying::text, 'Cupon Fuga 10 x 80'::character varying::text, 'Cupon Fuga 20 x 150'::character varying::text, 'Cupon Prefuga 5 x 30'::character varying::text, 'Cupon Prefuga 5 x 60'::character varying::text, 'Cupon Prefuga 5 x 80'::character varying::text]) THEN opp.valor
            ELSE 0
        END AS ahorro_orden,
    split_part(opp.nombre::text, ' '::text, 1) AS id_promo_workflow,
    pdv.afecta_despacho,
    pdv.total_carro
   FROM ecommdata.ordenes_janis oj
     LEFT JOIN ecommdata.tiendas t ON oj.id_tienda_janis = t.id_janis
     LEFT JOIN ecommdata.orden_productos op ON op.id_orden = oj.id
     LEFT JOIN ecommdata.categorias c ON op.ref_id_categoria = c.ref_id
     LEFT JOIN ecommdata.orden_producto_promociones opp ON op.id = opp.orden_producto
	 LEFT JOIN (
		SELECT DISTINCT ON (id) id,afecta_despacho,total_carro
		FROM ecommdata.promociones_detalle_vtex
     ) pdv ON opp.ref_id = pdv.id
  WHERE oj.fecha_creacion::date = '{{ds}}'::date
