with 
tabla as     ----------------------------Valores distintitivos --- fecha/id_tienda/operador -----------------
			(SELECT DISTINCT p.fecha_entrega,
			            p.id_tienda,
			            case 
			            	when p.operador  = 'Zubale + Rayo APP' then 'Zubale'
			            	when p.operador  = 'Boosmap + Rayo APP' then 'Boosmap'
			            	else p.operador
			            end
			            as operador
			            FROM forecast_and_planning.pedidos_prefactura_unimarc p),
sobrepeso  as  ----------------------------------------SobrepesoTimejobs ------------------------------------------------------------------
			(SELECT w.id_tienda,
			        w.fecha_entrega,
			        'Timejobs'::text AS operador,
			        sum(w.total_base) + sum(w.total_km) AS sobrepeso	
					from 
							(with 
							peso_orden  as ----------------------------------------------------------------------------------------------------------- Peso por orden
										(SELECT DISTINCT oj.id::text AS id_orden,
											    msp.sku_name,
											    oj.fecha_creacion::date AS fecha_creacion,
											    oj.fecha_picking::date AS fecha_picking,
											    msp.peso_neto,
											    msp.peso_bruto * op.unidades_pickeadas::double precision AS peso_bruto,
											    msp.um_count,
											    msp.umb,
											    msp.envase,
											    msp.categoria_sap,
											    op.unidades_pickeadas,
											    round(ms.volumen::numeric * op.unidades_pickeadas::numeric / 1000::numeric, 2) AS volumen_litro,
											    a.transportadora,
											    a.tienda,
											    a.operador
											   FROM ecommdata.ordenes_janis oj
											     LEFT JOIN ecommdata.orden_productos op ON oj.id = op.id_orden
											     LEFT JOIN ecommdata.productos p ON p.vtex_id = op.producto_vtex_id
											     LEFT JOIN ecommdata.maestra_sku_proveedor msp ON msp.material::text = p.material::text
											     LEFT JOIN ecommdata.maestra_slotting ms ON ms.material::text = p.material::text
											     LEFT JOIN ( SELECT DISTINCT d.id_orden,
											            d.id_transportadora,
											            t.nombre AS transportadora,
											            t2.glosa AS tienda,
											            t."nombre_compañia_logistica" AS operador
											           FROM ecommdata.despachos d
											             LEFT JOIN ecommdata.transportadoras t ON t.id::text = d.id_transportadora::text
											             LEFT JOIN ecommdata.tiendas t2 ON t2.id::text = t.id_tienda::text) a ON a.id_orden = oj.id),
							costo_armado as -------------------------------------------------------------------------------------------------------------Costo_armado
										(select p.id_orden, p.fecha_entrega , p.despachado, p.empleado, p.rut,
													p.tienda, p.id_tienda, p.transportadora , p.id_transportadora ,
													case                                                              --operadores
														when tp.operador = 'Zubale + Rayo APP' then 'Zubale'
														when tp.operador = 'Boosmap + Rayo APP' then 'Rayo APP'
														else tp.operador
													end as operador, 
													tp.modelo_cobro , p.sku , p.kilometros , tp.tarifa_sku , tp.tarifa_km ,   --datos
													tp.tarifa_base as total_base,
													tp.tarifa_sku * p.sku as total_sku,
													case																		                --kilómetros
														when p.kilometros is not null and p.despachado = 'si' then p.kilometros * tp.tarifa_km 
														else 0
													end as total_km,
													(tp.tarifa_base  + tp.tarifa_sku * p.sku  +													-- costo variable por pedido:  (base + sku + km)
														case
															when p.kilometros is not null and p.despachado = 'si' then p.kilometros * tp.tarifa_km 
															else 0
														end ) as costo_total_pedido,
													tp.tarifa_asegurado , f.dotacion          -- data asegurado     
											from  ---------------------------------------------------------------------------- TABLAS ----------------------------------
													forecast_and_planning.pedidos_prefactura_unimarc p      -- pedidos
											left join 
													forecast_and_planning.tarifas_prefacturas tp on tp.id_transportadora = p.id_transportadora      -- tarifas
											left join 
													forecast_and_planning.forecast f  							--modelo día-tienda
															on concat(p.fecha_entrega::date, p.id_tienda, tp.modelo_cobro, tp.operador) = concat(f.fecha::date, f.id_tienda, f.modelo, f.operador)
											where p.pickeada  = 'si' ---------------------------------------------------------------CONDICIONES-------------------------------
											and tp.id_transportadora  is not null	
											and 
											case 
												when tp.id_tienda = '0581' and tp.operador = 'Rayo APP' then f.dotacion  is null
											 	 else f.dotacion >0
											 end)
							SELECT pop.id_orden,   ------------------------------------QUERY COSTO POR SOBREPESO -----------------------------------------------------
							                    p.id_tienda,
							                    p.total_base,
							                    p.total_km,
							                    p.fecha_entrega,
							                    sum(pop.peso_bruto) AS peso,
							                    sum(pop.volumen_litro) AS volumen
							                 	FROM 
							                 		costo_armado p
							                     LEFT JOIN 
							                     	peso_orden pop ON p.id_orden::numeric = pop.id_orden::numeric
							               WHERE p.operador::text = 'Timejobs'::text 
							               AND p.despachado = 'si'::text
							               GROUP BY pop.id_orden, p.operador, p.total_base, p.total_km, p.id_tienda, p.fecha_entrega ) w
					WHERE w.peso > 80000::double precision OR w.volumen > 500::numeric
					GROUP BY w.id_tienda, w.fecha_entrega, 'Timejobs'::text	    ) ------------------------------------------------------------
SELECT 
	t.fecha_entrega,
    t.id_tienda,
    t.operador,
    1.08 * cc.costo_coordinador::numeric / 30::numeric AS costo_coordinador,
    a.monto::numeric / 30::numeric AS gastos_adicionales,
    s.sobrepeso
 FROM tabla t
 LEFT JOIN forecast_and_planning.costos_coordinadores cc 
 		ON concat(cc.id_tienda, cc.operador) = concat(t.id_tienda, t.operador)
 LEFT JOIN  sobrepeso s 
 		ON concat(s.id_tienda, s.fecha_entrega, s.operador) = concat(t.id_tienda, t.fecha_entrega, t.operador)
 LEFT JOIN forecast_and_planning.costos_fijos_prefacturas_unimarc a 
		ON concat(a.id_tienda, a.operador) = concat(t.id_tienda, t.operador)
WHERE t.fecha_entrega = '{{ds}}'::date
	