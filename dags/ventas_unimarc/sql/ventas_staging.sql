insert into staging.ventas_unimarc
select _t.fecha_facturacion 
		,_t.fecha_picking 
		, _t.id
		, _t.janis_id 
		, _t.glosa 
		, _t.canal_venta
		, _t.venta_facturada_bruta
		, _t.cobro_despacho_bruto
		, min(_t.ahorro_despacho) as ahorro_despacho_bruto
		, _t.ref_id
		, _t.descripcion 
		, _t.producto_substituto
		, _t.substituto_de
		, _t.unidad_medida
		, _t.categoria_n1
		, _t.categoria_n2
		, _t.categoria_n3
		, _t.unidades_pickeadas_original
		, _t.precio_unitario_bruto 
		, _t.pxq_bruto
		, _t.pxq_neto
		, coalesce(_t.costo_unitario_neto,0) as costo_unitario_neto 
		, coalesce(_t.unidades_pickeadas_original * _t.costo_unitario_neto,0) as cxq_neto
		, coalesce(_t.pxq_neto - (_t.unidades_pickeadas_original * _t.costo_unitario_neto),0) as contribucion_neta_1
		, coalesce(sum(case when _t.id_evento <> 400 then _t.ahorro_promocion else 0 end),0) as ahorro_promocion_cadena
		, coalesce(sum(case when _t.id_evento = 400 then _t.ahorro_promocion else 0 end),0) as ahorro_promocion_ecommerce
		, 0 as ahorro_promocion_personalizado
		, coalesce(sum(_t.ahorro_promocion),0) as ahorro_promocion_total
		, coalesce(sum(case when _t.id_evento <> 400 then _t.importe_negociado_unitario else 0 end),0) as importe_negociado_unitario_cadena
		, coalesce(sum(case when _t.id_evento = 400 then _t.importe_negociado_unitario else 0 end),0) as importe_negociado_unitario_ecommerce
		, 0 as importe_negociado_unitario_personalizado
		, sum(case when _t.tipo_financiamiento = 'SELL OUT' and _t.id_evento <> 400 then _t.pxq_importe_negociado else 0 end) as pxq_importe_negociado_sellout_cadena
		, sum(case when _t.tipo_financiamiento = 'SELL OUT' and _t.id_evento = 400 then _t.pxq_importe_negociado else 0 end) as pxq_importe_negociado_sellout_ecommerce
		, 0 as pxq_importe_negociado_sellout_personalizado
		, coalesce(sum(_t.pxq_importe_negociado),0) as pxq_importe_negociado_total
		, coalesce ((_t.pxq_neto - (_t.unidades_pickeadas_original * _t.costo_unitario_neto) + sum(_t.pxq_importe_negociado)), _t.pxq_neto - (_t.unidades_pickeadas_original * _t.costo_unitario_neto),0)  as contribucion_neta_2
		, bool_or(wp_promocion) as wp_promocion2
from 	(
		select 
			fecha_facturacion 
						,  fecha_picking 
						, id
						, janis_id 
						, glosa 
						, canal_venta
						, venta_facturada_bruta
						, cobro_despacho_bruto 
						, ref_id
						, descripcion 
						, producto_substituto
						, substituto_de
						, unidad_medida
						, categoria_n1
						, categoria_n2
						, categoria_n3
						, unidades_pickeadas_original
						, precio_unitario_bruto 
						, pxq_bruto
						, pxq_neto
						, costo_unitario_neto
						, ahorro_despacho
						, min(ahorro_promocion) as ahorro_promocion
						, min(importe_negociado_unitario) as importe_negociado_unitario
						, min(pxq_importe_negociado) as pxq_importe_negociado
						, tipo_financiamiento
						, every(n_promocion is not null) as wp_promocion
						, id_evento
from 
		(select 	 oj.fecha_facturacion
						,  oj.fecha_picking
						, oj.id
						, oj.janis_id 
						, t.glosa 
						, oj.canal_venta as canal_venta
						, oj.venta_facturada_bruta
						, oj.cobro_despacho_bruto 
						, op.ref_id
						, op.descripcion 
						, case 
								when op.unidades_solicitadas=0
								then true
								else false
							end as producto_substituto
						, case 
								when op.unidades_solicitadas=0
								then op2.ref_id
								else null
							end as substituto_de
						, case
								when split_part(op.ref_id, '-', 2) in ('KG', 'KGV')
								then 'KG'
								else 'UN'
							end as unidad_medida
						, c.n1 as categoria_n1
						, c.n2 as categoria_n2
						, c.n3 as categoria_n3
						, coalesce(opp.peso/1000.0, op.unidades_pickeadas) as unidades_pickeadas_original
						, round(coalesce(opp.precio, op.precio_venta),0) as precio_unitario_bruto 
						, case 
								when split_part(op.ref_id, '-', 2) in ('KG', 'KGV') and op.unidades_pickeadas <> 0
								then round(coalesce(opp.precio, op.precio_venta),0)
								else least(op.precio_venta * op.unidades_pickeadas,coalesce(op2.precio_venta * op2.unidades_solicitadas, op.precio_venta * op.unidades_pickeadas))
							end as pxq_bruto
						, case 
								when split_part(op.ref_id, '-', 2) in ('KG', 'KGV') and op.unidades_pickeadas <> 0
								then round(coalesce(opp.precio, op.precio_venta)/1.19,0)
								else round(least(op.precio_venta * op.unidades_pickeadas,coalesce(op2.precio_venta * op2.unidades_solicitadas, op.precio_venta * op.unidades_pickeadas))/1.19,0)
							end as pxq_neto
						, case 
								when costo.unidades_vendidas = 0
								then 0
								else round((costo.cogs/costo.unidades_vendidas)::numeric, 0)
							end * s.unidades_pack as costo_unitario_neto
						, case 
								when promo.nombre ilike '%despac%' or promo.nombre ilike '%flet%'
								then promo.valor
								else 0
							end as ahorro_despacho
						, case 
								when promo.nombre ilike '%despac%' or promo.nombre ilike '%flet%'
								then 0
								else promo.valor 
							end as ahorro_promocion
						, wp.importe_negociado as importe_negociado_unitario
						, wp.importe_negociado * coalesce(opp.peso/1000.0, op.unidades_pickeadas) as pxq_importe_negociado
						, wp.tipo_financiamiento 
						, wp.n_promocion 
						, wp.id_evento
				from ecommdata.ordenes_janis oj
				left join ecommdata.tiendas t on oj.id_tienda_janis = t.id_janis 
				left join ecommdata.orden_productos op on oj.id= op.id_orden
				left join ecommdata.orden_productos op2 on op.id_producto_substituido = op2.id
				left join ecommdata.orden_producto_pesables opp on op.id = opp.id_orden_producto
				left join ecommdata.productos p on op.producto_vtex_id = p.vtex_id 
				left join ecommdata.categorias c on p.id_categoria = c.id
				left join ecommdata.costos costo 
							on costo.fecha = oj.fecha_facturacion::DATE
							and costo.material =  p.material
							and costo.id_tienda = t.id
				left join ecommdata.skus s on op.sku_vtex_id = s.vtex_id
				left join ecommdata.orden_producto_promociones promo on op.id = promo.orden_producto 
				left join ecommdata.orden_producto_promocion_extrainfo promoextra on promo.id = promoextra.orden_producto_promocion and promoextra.campo = 'ID'
				left join ecommdata.workflow_promociones wp on promoextra.valor::int8 =  wp.n_promocion and p.material = wp.material and wp.id_evento not in (102)
				where oj.fecha_facturacion = to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') 
				) _h
group by fecha_facturacion 
						,  fecha_picking 
						, id
						, janis_id 
						, glosa 
						, canal_venta
						, venta_facturada_bruta
						, cobro_despacho_bruto 
						, ref_id
						, descripcion 
						, producto_substituto
						, substituto_de
						, unidad_medida
						, categoria_n1
						, categoria_n2
						, categoria_n3
						, unidades_pickeadas_original
						, precio_unitario_bruto 
						, pxq_bruto
						, pxq_neto
						, costo_unitario_neto
						, ahorro_despacho
						, tipo_financiamiento 
						, id_evento
				) _t
group by _t.fecha_facturacion 
		,_t.fecha_picking 
		, _t.id
		, _t.janis_id 
		, _t.glosa 
		, _t.canal_venta
		, _t.venta_facturada_bruta
		, _t.cobro_despacho_bruto 
		, _t.ref_id
		, _t.descripcion 
		, _t.producto_substituto
		, _t.substituto_de
		, _t.unidad_medida
		, _t.categoria_n1
		, _t.categoria_n2
		, _t.categoria_n3
		, _t.unidades_pickeadas_original
		, _t.precio_unitario_bruto 
		, _t.pxq_bruto
		, _t.pxq_neto
		, _t.costo_unitario_neto;