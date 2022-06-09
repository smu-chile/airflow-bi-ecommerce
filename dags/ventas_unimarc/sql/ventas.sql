select 
		v.fecha_facturacion 
		, v.fecha_picking 
		, v.id_orden
		, v.id_orden_janis 
		, v.glosa_tienda 
		, v.canal_venta
		, v.venta_facturada_bruta
		, v.cobro_despacho_bruto
		, v.ahorro_despacho_bruto
		, v.ref_id
		, v.descripcion 
		, v.producto_substituto
		, v.substituto_de
		, v.unidad_medida
		, v.categoria_n1
		, v.categoria_n2
		, v.categoria_n3
		, v.unidades_pickeadas_original
		, v.precio_unitario_bruto 
		, v.pxq_bruto
		, v.pxq_neto
		, round(coalesce(_cs.costo, v.costo_unitario_neto), 0) as costo_unitario_neto
		, round(coalesce(_cs.costo * v.unidades_pickeadas_original, v.cxq_neto), 0) as cxq_neto
		, round(coalesce(v.pxq_neto - (_cs.costo * v.unidades_pickeadas_original), v.contribucion_neta_1),0) as contribucion_neta_1
		, v.ahorro_promocion_cadena
		, v.ahorro_promocion_ecommerce
		, v.ahorro_promocion_total
		, v.importe_negociado_unitario_cadena
		, v.importe_negociado_unitario_ecommerce
		, v.pxq_importe_negociado_sellout_cadena
		, v.pxq_importe_negociado_sellout_ecommerce
		, v.pxq_importe_negociado_total 
		, round(coalesce(v.pxq_neto - (_cs.costo * v.unidades_pickeadas_original) + v.pxq_importe_negociado_total, v.contribucion_neta_2),0) as contribucion_neta_2
from staging.ventas_unimarc_contr2 v 
left join (
	select material, max(costo) as costo
	from ecommdata.costos_sap cs
	group by material
	) as _cs
on v.ref_id like concat(_cs.material,'%') and v.costo_unitario_neto = 0
where fecha_facturacion = to_date('{{execution_date.strftime('%Y-%m-%d')}}', '%YYYY-%mm-%dd') 
and v.costo_unitario_neto = 0
;
