insert into staging.ventas_unimarc_contr2
select _t2.fecha_facturacion 
		, _t2.fecha_picking 
		, _t2.id_orden
		, _t2.id_orden_janis 
		, _t2.glosa_tienda 
		, _t2.canal_venta
		, _t2.venta_facturada_bruta
		, _t2.cobro_despacho_bruto
		, _t2.ahorro_despacho_bruto
		, _t2.ref_id
		, _t2.descripcion 
		, _t2.producto_substituto
		, _t2.substituto_de
		, _t2.unidad_medida
		, _t2.categoria_n1
		, _t2.categoria_n2
		, _t2.categoria_n3
		, _t2.unidades_pickeadas_original
		, _t2.precio_unitario_bruto 
		, _t2.pxq_bruto
		, _t2.pxq_neto
		, _t2.costo_unitario_neto 
		, _t2.cxq_neto
		, _t2.contribucion_neta_1
		, _t2.ahorro_promocion_cadena
		, _t2.ahorro_promocion_ecommerce
		, _t2.ahorro_promocion_total
		, coalesce(sum(case when (_t2.canal_distribucion <> '70') or (_t2.canal_distribucion is null) then _t2.importe_negociado_unitario_2 else 0 end), _t2.importe_negociado_unitario_cadena) as importe_negociado_unitario_cadena
		, coalesce(sum(case when _t2.canal_distribucion = '70' then _t2.importe_negociado_unitario_2 else 0 end), _t2.importe_negociado_unitario_ecommerce) as importe_negociado_unitario_ecommerce
		, coalesce(sum(case when (_t2.canal_distribucion <> '70') or (_t2.canal_distribucion is null) then _t2.pxq_importe_negociado_2 else 0 end) , _t2.pxq_importe_negociado_total) as pxq_importe_negociado_sellout_cadena
		, coalesce(sum(case when _t2.canal_distribucion = '70' then _t2.pxq_importe_negociado_2 else 0 end) , _t2.pxq_importe_negociado_total) as pxq_importe_negociado_sellout_ecommerce
		, coalesce(sum(_t2.pxq_importe_negociado_2) , _t2.pxq_importe_negociado_total) as pxq_importe_negociado_total 
		, coalesce ((_t2.pxq_neto - (_t2.unidades_pickeadas_original * _t2.costo_unitario_neto) + sum(_t2.pxq_importe_negociado_2)), _t2.contribucion_neta_2, 0) as contribucion_neta_2
from (
	select _t.fecha_facturacion 
			,_t.fecha_picking 
			, _t.id_orden
			, _t.id_orden_janis 
			, _t.glosa_tienda 
			, _t.canal_venta
			, _t.venta_facturada_bruta
			, _t.cobro_despacho_bruto
			, _t.ahorro_despacho_bruto
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
			, _t.costo_unitario_neto 
			, _t.cxq_neto
			, _t.contribucion_neta_1
			, _t.ahorro_promocion_cadena
			, _t.ahorro_promocion_ecommerce
			, _t.ahorro_promocion_personalizado
			, _t.ahorro_promocion_total
			, _t.importe_negociado_unitario_cadena
			, _t.importe_negociado_unitario_ecommerce
			, _t.importe_negociado_unitario_personalizado
			, _t.pxq_importe_negociado_total
			, _t.contribucion_neta_2
			, min(importe_negociado_unitario_2) as importe_negociado_unitario_2
			, min(pxq_importe_negociado_2) as pxq_importe_negociado_2
			, _t.canal_distribucion
	from (
		select vs.*
			, wp2.importe_negociado as importe_negociado_unitario_2 
			, wp2.importe_negociado * vs.unidades_pickeadas_original as pxq_importe_negociado_2
			, wp2.n_promocion 
			, wp2.canal_distribucion
		from staging.ventas_unimarc vs 
		left join ecommdata.workflow_promociones wp2 
			on split_part(vs.ref_id, '-', 1) = wp2.material 
			and not vs.wp_promocion2
			and wp2.tipo_financiamiento = 'SELL OUT'
			and vs.fecha_facturacion between wp2.fecha_inicio_de_promocion and wp2.fecha_fin_de_promocion and wp2.id_evento not in (102)
			and vs.producto_substituto
		where vs.fecha_facturacion = to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') 
	) _t
	group by _t.fecha_facturacion 
			, _t.fecha_picking 
			, _t.id_orden
			, _t.id_orden_janis 
			, _t.glosa_tienda 
			, _t.canal_venta
			, _t.venta_facturada_bruta
			, _t.cobro_despacho_bruto
			, _t.ahorro_despacho_bruto
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
			, _t.costo_unitario_neto 
			, _t.cxq_neto
			, _t.contribucion_neta_1
			, _t.ahorro_promocion_cadena
			, _t.ahorro_promocion_ecommerce
			, _t.ahorro_promocion_personalizado
			, _t.ahorro_promocion_total
			, _t.importe_negociado_unitario_cadena
			, _t.importe_negociado_unitario_ecommerce
			, _t.importe_negociado_unitario_personalizado
			, _t.pxq_importe_negociado_total
			, _t.pxq_importe_negociado_sellout_cadena
			, _t.pxq_importe_negociado_sellout_ecommerce
			, _t.pxq_importe_negociado_sellout_personalizado
			, _t.contribucion_neta_2
			, _t.n_promocion
			, _t.canal_distribucion
	) _t2
group by fecha_facturacion 
			, fecha_picking 
			, id_orden
			, id_orden_janis 
			, glosa_tienda 
			, canal_venta
			, venta_facturada_bruta
			, cobro_despacho_bruto
			, ahorro_despacho_bruto
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
			, cxq_neto
			, contribucion_neta_1
			, ahorro_promocion_cadena
			, ahorro_promocion_ecommerce
			, ahorro_promocion_personalizado
			, ahorro_promocion_total
			, importe_negociado_unitario_cadena
			, importe_negociado_unitario_ecommerce
			, importe_negociado_unitario_personalizado
			, pxq_importe_negociado_total
			, contribucion_neta_2;
