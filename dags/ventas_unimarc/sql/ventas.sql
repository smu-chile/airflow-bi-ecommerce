insert into ventas_unimarc.ventas
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
		, _t2.ahorro_promocion
		, coalesce(sum(_t2.importe_negociado_unitario_2), _t2.importe_negociado_unitario) as importe_negociado_unitario
		, coalesce(sum(_t2.pxq_importe_negociado_2) , _t2.pxq_importe_negociado_total) as pxq_importe_negociado_total
		, coalesce(sum(_t2.pxq_importe_negociado_2) , _t2.pxq_importe_negociado_sellout) as pxq_importe_negociado_sellout 
		, _t2.pxq_importe_negociado_sellin
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
			, _t.ahorro_promocion
			, _t.importe_negociado_unitario
			, _t.pxq_importe_negociado_total
			, _t.pxq_importe_negociado_sellout
			, _t.pxq_importe_negociado_sellin
			, _t.contribucion_neta_2
			, min(importe_negociado_unitario_2) as importe_negociado_unitario_2
			, min(pxq_importe_negociado_2) as pxq_importe_negociado_2
	from (
		select vs.*
			, wp2.importe_negociado as importe_negociado_unitario_2 
			, wp2.importe_negociado * vs.unidades_pickeadas_original as pxq_importe_negociado_2
			, wp2.n_promocion 
		from public.ventas_staging vs 
		left join ecommdata.workflow_promociones wp2 
			on split_part(vs.ref_id, '-', 1) = wp2.material 
			and not vs.wp_promocion2
			and wp2.tipo_financiamiento = 'SELL OUT'
			and vs.fecha_facturacion between wp2.fecha_inicio_de_promocion and wp2.fecha_fin_de_promocion
		where vs.fecha_facturacion at time zone 'UTC' at time zone 'America/Santiago' = to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') 
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
			, _t.ahorro_promocion
			, _t.importe_negociado_unitario
			, _t.pxq_importe_negociado_total
			, _t.pxq_importe_negociado_sellout
			, _t.pxq_importe_negociado_sellin
			, _t.contribucion_neta_2
			, _t.n_promocion
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
			, ahorro_promocion
			, importe_negociado_unitario
			, pxq_importe_negociado_total
			, pxq_importe_negociado_sellout
			, pxq_importe_negociado_sellin
			, contribucion_neta_2;