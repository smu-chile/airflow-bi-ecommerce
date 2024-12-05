select a.fecha_entrega ,a.id_tienda , a.tienda ,a.modelo_cobro , a.tarifa_asegurado , a.operador,
case 
	when a.operador = 'Timejobs' and a.modelo_cobro = 'Shopper'
		then sum(a.costo_total_pedido) *0.7
		else sum(a.costo_total_pedido)  
end as costo_armado, 
round((count(distinct a.id_orden)::numeric / p.pedidos_tienda)*  a.dotacion,0)::int  as dotacion,
(round((count(distinct a.id_orden)::numeric / p.pedidos_tienda)*  a.dotacion,0)::int*tarifa_asegurado) as minimo_asegurado,
case                                                                 -- Cálculo diferencia de asegurado max(asegurado - costo armado ;   0)
	when a.operador = 'Timejobs' and a.modelo_cobro = 'Shopper'
		then greatest ((round((count(distinct a.id_orden)::numeric / p.pedidos_tienda) * a.dotacion,0)::int*tarifa_asegurado)- (sum(a.costo_total_pedido)*0.7) ,0 )  ---timjobes rebaja 30% este valor
	when a.operador in ('Rayo APP' , 'Roca') -- se paga si o si el asegurado
		then greatest ((a.dotacion::int*tarifa_asegurado)- (sum(a.costo_total_pedido)*0) ,0 )
	else  greatest ((round((count(distinct a.id_orden)::numeric / p.pedidos_tienda) * a.dotacion,0)::int*tarifa_asegurado)-sum(a.costo_total_pedido) ,0 ) 
end as diferencia_asegurado
from -------------------------------TABLAS--------------------------------------------------------------------------------------------
		(------------ Armado de pedidos------------------------------------------------------------------------------------------
					select p.id_orden, p.fecha_entrega , p.despachado, p.empleado, p.rut,
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
					 end
					and fecha_entrega = '{{ds}}'::date)		a  -- costo armado pedido
	left join 
				(select id_tienda, fecha_entrega , modelo_cobro,count(distinct id_orden) as pedidos_tienda     -- pesos para dotación con asegurados distintos
						from (------------ Armado de pedidos------------------------------------------------------------------------------------------
								select p.id_orden, p.fecha_entrega , p.despachado, p.empleado, p.rut,
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
								 end
								and fecha_entrega = '{{ds}}'::date) e
						group by id_tienda, fecha_entrega, modelo_cobro ) p  
								on  concat(p.id_tienda,p.fecha_entrega, p.modelo_cobro) = concat(a.id_tienda,a.fecha_entrega, a.modelo_cobro)
	------FIN JOINS----------------------------END-------------END----------------------END------------------------------------------------------------FIN
	where a.fecha_entrega = '{{ds}}'::date
	group by a.fecha_entrega ,a.id_tienda , a.tienda ,a.modelo_cobro , a.tarifa_asegurado , a.dotacion ,p.pedidos_tienda, a.operador
