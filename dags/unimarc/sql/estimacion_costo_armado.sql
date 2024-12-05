------------ Armado de pedidos------------------------------------------------------------------------------------------
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
and fecha_entrega = '{{ds}}'::date
