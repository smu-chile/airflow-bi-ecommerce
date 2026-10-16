begin transaction;

-- =============================================================================
-- ESCENARIO BASE: Multiplicador de venta = 1 (150 ordenes aprox)
-- =============================================================================
truncate table ecommdata.maestra_slotting_mfc;

insert into ecommdata.maestra_slotting_mfc(ref_id, nombre, umv, ump, sku_key, n1, n2, n3, marca, proveedor, id_proveer, peso_bruto, altura, ancho, condicion_de_almacenaje, brnd_id, categoria_material_desc, gds_pd_tp_dsc, contenido_bruto, contenido_neto, grado_alcoholico, longitud, marca_propia, numerador_ump, pais_origen_id, peso_neto, um_contenido, unidad, unidad_de_medida_pedido, unidad_de_volumen, unidad_laa, unidad_peso, volumen, vida_util, vida_util_maxima, temperature_zone, ranking, ventatotal, promedioventadiario, leadtime, frecuenciarepo, fuenteapro, fillrate, fill_rate_ajustado, channel, cd, proveecd, capcaja, status0917, normaretiro, candidato_merma, stock_objetivo_teorico, stock_objetivo_real, cantidadmerma, restriccion_merma_10, restriccion_merma_05, restriccion_merma_00, volumen_proyectado, totes_volumen_proyectado, totes_volumen_aproximado, ganador, ganador_ranking, numero_ranking, venta_historica_unidades)
(with constantes(dias, merma1, merma2, merma3, volumenMax, pesoMax, fillrate_ajustado, fracTote, fact_ampl_venta, porc_dias_habiles) as(
values(7, 0.1, 0.05, 0.00, 61867.5*0.7, 22275, 0.6, 0.125, 1, 0.7) --mult. venta 2.5, 4 y 8 
), tablita as(
select ved.ref_id_sku as "ref_id"
, p.nombre
, ms.umv
, ms.ump
, ms.sku_key
, c.n1 as "n1"
, c.n2 as "n2"
, c.n3 as "n3"
, ms.marca
, ms.proveedor
, ms.id_proveer
, ms.peso_bruto
, ms.altura as "altura"
, ms.ancho as "ancho"
, ms.condicion_de_almacenaje
, ms.brnd_id
, ms.categoria_material_desc
, ms.gds_pd_tp_dsc
, ms.contenido_bruto
, ms.contenido_neto
, ms.grado_alcoholico
, ms.longitud::float4
, case 
when ms.marca_propia = 1 then true 
when ms.marca_propia = 0 then false 
else null
end as marca_propia
, ms.numerador_ump::integer
, ms.pais_origen_id
, ms.peso_neto
, ms.um_contenido
, ms.unidad
, ms.unidad_de_medida_pedido
, ms.unidad_de_volumen
, ms.unidad_laa
, ms.unidad_peso
, ms.volumen
, ms.vida_util
, ms.vida_util * 0.7 as vida_util_maxima
, ms.temperature_zone
, rpt.ranking
from constantes ctts, ecommdata.maestra_slotting ms 
inner join ecommdata.lista8 l 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ms.ref_id
inner join ecommdata.ventas_ecommerce_datawarehouse ved 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ved.ref_id_sku
inner join ecommdata.productos p 
on p.ref_id = ved.ref_id_sku 
inner join ecommdata.categorias c 
on c.id = p.id_categoria 
inner join ecommdata.ranking_productos_tienda rpt 
on rpt.ref_id_sku = ms.ref_id 
and rpt.id_tienda = l.id_tienda
where l.id_tienda in ('0917')
and ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and c.n1 not in ('No Trabajar', 'Congelados', 'Inactivos', 'Sustitución', 'Integración', 'Lácteos, Huevos y Refrigerados')--, 'Carnes') --, 'Carnes', 'Frutas y Verduras')--, 'Quesos y Fiambres' , 'Lácteos, Huevos y Refrigerados', 'Carnes', // 
and l.material not in ('964689','641390','611439','611438','606780','606782','606781','611440','611441','611442','611444','611443','611445','634301','634302','608113','608115','608103','608116','609126','622031','622035','613938','613937','622033','612172','612134','633966','612173','608601','608602','676471','610521','610890','608603','622032','622034','614627','614728','968898','969103','814831','609252','605168','604790','604789','604170','979614','969109','969110','969100','969105','624152','969101','969102','968899','969104','969106','969107','611527','609117','969108','979615','618138','618139','636591','645919002','645919','645919001','646074','641647','697682','646689','813249','151213','160536','616815','635703','708039','661855','661945','718741','928741','200713','641664','640072','18224','640143','652490','665715','653400','642726','649233','649227','649253','649078001','649078002','662181','646509','662219','662180','616932','616933','663049','654683','628876','628877','650176','655581','624859','624860','648518','645915','642725','648933','648932','645207','642724','642723','170771','638775','638776','635136','655580','655579','645360','624858','662255','645361','649231','648519','623247','664311','664310','645914','645913','638538','622703','665319','566801','662184','665059','665818','662170','663023','565721','668163') --restricciones logísticas
and ms.is_hazardous = false
and l.umv not in ('KG', 'KGV')
and ved.ref_id_sku not in ('000000000000007482-DIS', '000000000000007493-DIS', '000000000000007504-DIS', '000000000000008892-UN', '000000000000130191-DIS', '000000000000130196-DIS', '000000000000155394-DIS', '000000000000158479-DIS', '000000000000160531-UN', '000000000000176714-UN', '000000000000190186-DIS', '000000000000209675-UN', '000000000000506345-DIS', '000000000000578191-DIS', '000000000000626247-UN', '000000000000631249-UN', '000000000000634650-PAQ', '000000000000635931-DIS', '000000000000649045-UN', '000000000000661882-UN', '000000000000665043-DIS', '000000000000668149-UN', '000000000000901934-DIS', '000000000000985415-DIS', '000000000400046001-DIS', '000000000400070001-DIS', '000000000402613001-DIS', '000000000402613002-DIS', '000000000402629001-DIS', '000000000402629003-DIS', '000000000402629004-DIS', '000000000605007001-DIS', '000000000650059001-UN', '000000000650059002-UN')
--and ved.ref_id_sku not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
having 
(ms.altura::real)*10 <= 300 or
(ms.ancho::real)*10 <= 365 or
(ms.longitud::real)*10 <= 565 or
(ms.peso_bruto::integer) <= 25000-2725 or
(ms.volumen::real) <= (30*36.5*56.5)*0.7 --restriccion 30% aire
), tablaventa as(
select ved.ref_id_sku as ref_id 
, sum(ved.venta_umv)::integer as ventatotal
, (sum(ved.venta_umv)/180)::real as promedioventadiario
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and ved.id_tienda in ('0917')
group by 1
), datosmfc as(
select dlm.ref_id
, dlm.leadtime as leadtime
, dlm.frecuencia_reposicion as frecuenciarepo
, dlm.fuente_aprovisionamiento as fuenteapro
, dlm.fill_rate as fillrate
, case
when dlm.fill_rate < c.fillrate_ajustado then c.fillrate_ajustado
else dlm.fill_rate
end as fill_rate_ajustado
, dlm.channel as channel
, dlm.cd as cd
, dlm.proveedor_cd as proveecd
, dlm.capacidad_caja as capcaja
, dlm.status_0917 as status0917
from constantes c, ecommdata.datos_logisticos_mfc dlm
left join ecommdata.maestra_slotting ms on ms.ref_id = dlm.ref_id
group by 1,2,3,4,5,6,7,8,9,10,11
), tablahistorica as (
select ved.ref_id_sku as ref_id
, sum(ved.venta_umv)::integer as venta_total
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion <= current_date
and ved.id_tienda in ('0917')
group by 1
)
select t.*
, tv.ventatotal
, tv.promedioventadiario
, dm.leadtime
, dm.frecuenciarepo
, dm.fuenteapro
, dm.fillrate
, dm.fill_rate_ajustado
, dm.channel
, dm.cd
, dm.proveecd
, dm.capcaja
, dm.status0917
, t.vida_util * (1-c.porc_dias_habiles) as normaretiro
, case 
when tv.promedioventadiario = 0 
then 'Candidato a Merma'
when t.numerador_ump/tv.promedioventadiario > t.vida_util_maxima 
then 'Candidato a Merma'
else 'No mermable'
end as candidato_merma
, (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado as stock_objetivo_teorico
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end as stock_objetivo_real
, case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end as cantidadmerma
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma1 then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_10
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma2 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_05
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) > (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma3 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_00
/*
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma1 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_10,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma2 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_05,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= 0 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_00,
*/
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen
end as volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax
end as totes_volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax)/c.fracTote)*fracTote 
else ceil((ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax)/c.fracTote)*c.fracTote 
end as totes_volumen_aproximado
, case 
when (t.ranking <= 500) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador a Piso'
else ''
end as ganador
, case 
when (t.ranking <= 1000) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador por Ranking'
else ''
end as ganador_ranking
, t.ranking as numero_ranking
, th.venta_total as unidades_vendidas_historicas
from constantes c, tablita t 
inner join tablaventa tv 
on t.ref_id = tv.ref_id
left join datosmfc dm 
on dm.ref_id = t.ref_id
inner join tablahistorica th 
on th.ref_id = t.ref_id
where t.ref_id not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
and t.ref_id not in ('000000000000666148-UN',
'000000000000663748-UN',
'000000000000942475-UN',
'000000000000655618-UN') -- Restricciones solicitadas por correo 19/03/2025
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
order by tv.ventatotal desc);


-- =============================================================================
-- ESCENARIO 150: Multiplicador de venta = 2.5 (375 ordenes aprox)
-- =============================================================================
truncate table ecommdata.maestra_slotting_mfc_150;

insert into ecommdata.maestra_slotting_mfc_150(ref_id, nombre, umv, ump, sku_key, n1, n2, n3, marca, proveedor, id_proveer, peso_bruto, altura, ancho, condicion_de_almacenaje, brnd_id, categoria_material_desc, gds_pd_tp_dsc, contenido_bruto, contenido_neto, grado_alcoholico, longitud, marca_propia, numerador_ump, pais_origen_id, peso_neto, um_contenido, unidad, unidad_de_medida_pedido, unidad_de_volumen, unidad_laa, unidad_peso, volumen, vida_util, vida_util_maxima, temperature_zone, ranking, ventatotal, promedioventadiario, leadtime, frecuenciarepo, fuenteapro, fillrate, fill_rate_ajustado, channel, cd, proveecd, capcaja, status0917, normaretiro, candidato_merma, stock_objetivo_teorico, stock_objetivo_real, cantidadmerma, restriccion_merma_10, restriccion_merma_05, restriccion_merma_00, volumen_proyectado, totes_volumen_proyectado, totes_volumen_aproximado, ganador, ganador_ranking, numero_ranking, venta_historica_unidades)
(with constantes(dias, merma1, merma2, merma3, volumenMax, pesoMax, fillrate_ajustado, fracTote, fact_ampl_venta, porc_dias_habiles) as(
values(7, 0.1, 0.05, 0.00, 61867.5*0.7, 22275, 0.6, 0.125, 2.5, 0.7) --mult. venta 2.5, 4 y 8 
), tablita as(
select ved.ref_id_sku as "ref_id"
, p.nombre
, ms.umv
, ms.ump
, ms.sku_key
, c.n1 as "n1"
, c.n2 as "n2"
, c.n3 as "n3"
, ms.marca
, ms.proveedor
, ms.id_proveer
, ms.peso_bruto
, ms.altura as "altura"
, ms.ancho as "ancho"
, ms.condicion_de_almacenaje
, ms.brnd_id
, ms.categoria_material_desc
, ms.gds_pd_tp_dsc
, ms.contenido_bruto
, ms.contenido_neto
, ms.grado_alcoholico
, ms.longitud::float4
, case 
when ms.marca_propia = 1 then true 
when ms.marca_propia = 0 then false 
else null
end as marca_propia
, ms.numerador_ump::integer
, ms.pais_origen_id
, ms.peso_neto
, ms.um_contenido
, ms.unidad
, ms.unidad_de_medida_pedido
, ms.unidad_de_volumen
, ms.unidad_laa
, ms.unidad_peso
, ms.volumen
, ms.vida_util
, ms.vida_util * 0.7 as vida_util_maxima
, ms.temperature_zone
, rpt.ranking
from constantes ctts, ecommdata.maestra_slotting ms 
inner join ecommdata.lista8 l 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ms.ref_id
inner join ecommdata.ventas_ecommerce_datawarehouse ved 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ved.ref_id_sku
inner join ecommdata.productos p 
on p.ref_id = ved.ref_id_sku 
inner join ecommdata.categorias c 
on c.id = p.id_categoria 
inner join ecommdata.ranking_productos_tienda rpt 
on rpt.ref_id_sku = ms.ref_id 
and rpt.id_tienda = l.id_tienda
where l.id_tienda in ('0917')
and ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and c.n1 not in ('No Trabajar', 'Congelados', 'Inactivos', 'Sustitución', 'Integración', 'Lácteos, Huevos y Refrigerados')--, 'Carnes') --, 'Carnes', 'Frutas y Verduras')--, 'Quesos y Fiambres' , 'Lácteos, Huevos y Refrigerados', 'Carnes', // 
and l.material not in ('964689','641390','611439','611438','606780','606782','606781','611440','611441','611442','611444','611443','611445','634301','634302','608113','608115','608103','608116','609126','622031','622035','613938','613937','622033','612172','612134','633966','612173','608601','608602','676471','610521','610890','608603','622032','622034','614627','614728','968898','969103','814831','609252','605168','604790','604789','604170','979614','969109','969110','969100','969105','624152','969101','969102','968899','969104','969106','969107','611527','609117','969108','979615','618138','618139','636591','645919002','645919','645919001','646074','641647','697682','646689','813249','151213','160536','616815','635703','708039','661855','661945','718741','928741','200713','641664','640072','18224','640143','652490','665715','653400','642726','649233','649227','649253','649078001','649078002','662181','646509','662219','662180','616932','616933','663049','654683','628876','628877','650176','655581','624859','624860','648518','645915','642725','648933','648932','645207','642724','642723','170771','638775','638776','635136','655580','655579','645360','624858','662255','645361','649231','648519','623247','664311','664310','645914','645913','638538','622703','665319','566801','662184','665059','665818','662170','663023','565721','668163') --restricciones logísticas
and ms.is_hazardous = false
and l.umv not in ('KG', 'KGV')
and ved.ref_id_sku not in ('000000000000007482-DIS', '000000000000007493-DIS', '000000000000007504-DIS', '000000000000008892-UN', '000000000000130191-DIS', '000000000000130196-DIS', '000000000000155394-DIS', '000000000000158479-DIS', '000000000000160531-UN', '000000000000176714-UN', '000000000000190186-DIS', '000000000000209675-UN', '000000000000506345-DIS', '000000000000578191-DIS', '000000000000626247-UN', '000000000000631249-UN', '000000000000634650-PAQ', '000000000000635931-DIS', '000000000000649045-UN', '000000000000661882-UN', '000000000000665043-DIS', '000000000000668149-UN', '000000000000901934-DIS', '000000000000985415-DIS', '000000000400046001-DIS', '000000000400070001-DIS', '000000000402613001-DIS', '000000000402613002-DIS', '000000000402629001-DIS', '000000000402629003-DIS', '000000000402629004-DIS', '000000000605007001-DIS', '000000000650059001-UN', '000000000650059002-UN')
--and ved.ref_id_sku not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
having 
(ms.altura::real)*10 <= 300 or
(ms.ancho::real)*10 <= 365 or
(ms.longitud::real)*10 <= 565 or
(ms.peso_bruto::integer) <= 25000-2725 or
(ms.volumen::real) <= (30*36.5*56.5)*0.7 --restriccion 30% aire
), tablaventa as(
select ved.ref_id_sku as ref_id 
, sum(ved.venta_umv)::integer as ventatotal
, (sum(ved.venta_umv)/180)::real as promedioventadiario
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and ved.id_tienda in ('0917')
group by 1
), datosmfc as(
select dlm.ref_id
, dlm.leadtime as leadtime
, dlm.frecuencia_reposicion as frecuenciarepo
, dlm.fuente_aprovisionamiento as fuenteapro
, dlm.fill_rate as fillrate
, case
when dlm.fill_rate < c.fillrate_ajustado then c.fillrate_ajustado
else dlm.fill_rate
end as fill_rate_ajustado
, dlm.channel as channel
, dlm.cd as cd
, dlm.proveedor_cd as proveecd
, dlm.capacidad_caja as capcaja
, dlm.status_0917 as status0917
from constantes c, ecommdata.datos_logisticos_mfc dlm
left join ecommdata.maestra_slotting ms on ms.ref_id = dlm.ref_id
group by 1,2,3,4,5,6,7,8,9,10,11
), tablahistorica as (
select ved.ref_id_sku as ref_id
, sum(ved.venta_umv)::integer as venta_total
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion <= current_date
and ved.id_tienda in ('0917')
group by 1
)
select t.*
, tv.ventatotal
, tv.promedioventadiario
, dm.leadtime
, dm.frecuenciarepo
, dm.fuenteapro
, dm.fillrate
, dm.fill_rate_ajustado
, dm.channel
, dm.cd
, dm.proveecd
, dm.capcaja
, dm.status0917
, t.vida_util * (1-c.porc_dias_habiles) as normaretiro
, case 
when tv.promedioventadiario = 0 
then 'Candidato a Merma'
when t.numerador_ump/tv.promedioventadiario > t.vida_util_maxima 
then 'Candidato a Merma'
else 'No mermable'
end as candidato_merma
, (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado as stock_objetivo_teorico
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end as stock_objetivo_real
, case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end as cantidadmerma
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma1 then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_10
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma2 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_05
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) > (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma3 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_00
/*
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma1 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_10,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma2 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_05,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= 0 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_00,
*/
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen
end as volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax
end as totes_volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax)/c.fracTote)*fracTote 
else ceil((ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax)/c.fracTote)*c.fracTote 
end as totes_volumen_aproximado
, case 
when (t.ranking <= 500) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador a Piso'
else ''
end as ganador
, case 
when (t.ranking <= 1000) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador por Ranking'
else ''
end as ganador_ranking
, t.ranking as numero_ranking
, th.venta_total as unidades_vendidas_historicas
from constantes c, tablita t 
inner join tablaventa tv 
on t.ref_id = tv.ref_id
left join datosmfc dm 
on dm.ref_id = t.ref_id
inner join tablahistorica th 
on th.ref_id = t.ref_id
where t.ref_id not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
and t.ref_id not in ('000000000000666148-UN',
'000000000000663748-UN',
'000000000000942475-UN',
'000000000000655618-UN') -- Restricciones solicitadas por correo 19/03/2025
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
order by tv.ventatotal desc);


-- =============================================================================
-- ESCENARIO 250: Multiplicador de venta = 4 (600 ordenes aprox)
-- =============================================================================
truncate table ecommdata.maestra_slotting_mfc_250;

insert into ecommdata.maestra_slotting_mfc_250(ref_id, nombre, umv, ump, sku_key, n1, n2, n3, marca, proveedor, id_proveer, peso_bruto, altura, ancho, condicion_de_almacenaje, brnd_id, categoria_material_desc, gds_pd_tp_dsc, contenido_bruto, contenido_neto, grado_alcoholico, longitud, marca_propia, numerador_ump, pais_origen_id, peso_neto, um_contenido, unidad, unidad_de_medida_pedido, unidad_de_volumen, unidad_laa, unidad_peso, volumen, vida_util, vida_util_maxima, temperature_zone, ranking, ventatotal, promedioventadiario, leadtime, frecuenciarepo, fuenteapro, fillrate, fill_rate_ajustado, channel, cd, proveecd, capcaja, status0917, normaretiro, candidato_merma, stock_objetivo_teorico, stock_objetivo_real, cantidadmerma, restriccion_merma_10, restriccion_merma_05, restriccion_merma_00, volumen_proyectado, totes_volumen_proyectado, totes_volumen_aproximado, ganador, ganador_ranking, numero_ranking, venta_historica_unidades)
(with constantes(dias, merma1, merma2, merma3, volumenMax, pesoMax, fillrate_ajustado, fracTote, fact_ampl_venta, porc_dias_habiles) as(
values(7, 0.1, 0.05, 0.00, 61867.5*0.7, 22275, 0.6, 0.125, 4, 0.7) --mult. venta 2.5, 4 y 8 
), tablita as(
select ved.ref_id_sku as "ref_id"
, p.nombre
, ms.umv
, ms.ump
, ms.sku_key
, c.n1 as "n1"
, c.n2 as "n2"
, c.n3 as "n3"
, ms.marca
, ms.proveedor
, ms.id_proveer
, ms.peso_bruto
, ms.altura as "altura"
, ms.ancho as "ancho"
, ms.condicion_de_almacenaje
, ms.brnd_id
, ms.categoria_material_desc
, ms.gds_pd_tp_dsc
, ms.contenido_bruto
, ms.contenido_neto
, ms.grado_alcoholico
, ms.longitud::float4
, case 
when ms.marca_propia = 1 then true 
when ms.marca_propia = 0 then false 
else null
end as marca_propia
, ms.numerador_ump::integer
, ms.pais_origen_id
, ms.peso_neto
, ms.um_contenido
, ms.unidad
, ms.unidad_de_medida_pedido
, ms.unidad_de_volumen
, ms.unidad_laa
, ms.unidad_peso
, ms.volumen
, ms.vida_util
, ms.vida_util * 0.7 as vida_util_maxima
, ms.temperature_zone
, rpt.ranking
from constantes ctts, ecommdata.maestra_slotting ms 
inner join ecommdata.lista8 l 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ms.ref_id
inner join ecommdata.ventas_ecommerce_datawarehouse ved 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ved.ref_id_sku
inner join ecommdata.productos p 
on p.ref_id = ved.ref_id_sku 
inner join ecommdata.categorias c 
on c.id = p.id_categoria 
inner join ecommdata.ranking_productos_tienda rpt 
on rpt.ref_id_sku = ms.ref_id 
and rpt.id_tienda = l.id_tienda
where l.id_tienda in ('0917')
and ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and c.n1 not in ('No Trabajar', 'Congelados', 'Inactivos', 'Sustitución', 'Integración', 'Lácteos, Huevos y Refrigerados')--, 'Carnes') --, 'Carnes', 'Frutas y Verduras')--, 'Quesos y Fiambres' , 'Lácteos, Huevos y Refrigerados', 'Carnes', // 
and l.material not in ('964689','641390','611439','611438','606780','606782','606781','611440','611441','611442','611444','611443','611445','634301','634302','608113','608115','608103','608116','609126','622031','622035','613938','613937','622033','612172','612134','633966','612173','608601','608602','676471','610521','610890','608603','622032','622034','614627','614728','968898','969103','814831','609252','605168','604790','604789','604170','979614','969109','969110','969100','969105','624152','969101','969102','968899','969104','969106','969107','611527','609117','969108','979615','618138','618139','636591','645919002','645919','645919001','646074','641647','697682','646689','813249','151213','160536','616815','635703','708039','661855','661945','718741','928741','200713','641664','640072','18224','640143','652490','665715','653400','642726','649233','649227','649253','649078001','649078002','662181','646509','662219','662180','616932','616933','663049','654683','628876','628877','650176','655581','624859','624860','648518','645915','642725','648933','648932','645207','642724','642723','170771','638775','638776','635136','655580','655579','645360','624858','662255','645361','649231','648519','623247','664311','664310','645914','645913','638538','622703','665319','566801','662184','665059','665818','662170','663023','565721','668163') --restricciones logísticas
and ms.is_hazardous = false
and l.umv not in ('KG', 'KGV')
and ved.ref_id_sku not in ('000000000000007482-DIS', '000000000000007493-DIS', '000000000000007504-DIS', '000000000000008892-UN', '000000000000130191-DIS', '000000000000130196-DIS', '000000000000155394-DIS', '000000000000158479-DIS', '000000000000160531-UN', '000000000000176714-UN', '000000000000190186-DIS', '000000000000209675-UN', '000000000000506345-DIS', '000000000000578191-DIS', '000000000000626247-UN', '000000000000631249-UN', '000000000000634650-PAQ', '000000000000635931-DIS', '000000000000649045-UN', '000000000000661882-UN', '000000000000665043-DIS', '000000000000668149-UN', '000000000000901934-DIS', '000000000000985415-DIS', '000000000400046001-DIS', '000000000400070001-DIS', '000000000402613001-DIS', '000000000402613002-DIS', '000000000402629001-DIS', '000000000402629003-DIS', '000000000402629004-DIS', '000000000605007001-DIS', '000000000650059001-UN', '000000000650059002-UN')
--and ved.ref_id_sku not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
having 
(ms.altura::real)*10 <= 300 or
(ms.ancho::real)*10 <= 365 or
(ms.longitud::real)*10 <= 565 or
(ms.peso_bruto::integer) <= 25000-2725 or
(ms.volumen::real) <= (30*36.5*56.5)*0.7 --restriccion 30% aire
), tablaventa as(
select ved.ref_id_sku as ref_id 
, sum(ved.venta_umv)::integer as ventatotal
, (sum(ved.venta_umv)/180)::real as promedioventadiario
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and ved.id_tienda in ('0917')
group by 1
), datosmfc as(
select dlm.ref_id
, dlm.leadtime as leadtime
, dlm.frecuencia_reposicion as frecuenciarepo
, dlm.fuente_aprovisionamiento as fuenteapro
, dlm.fill_rate as fillrate
, case
when dlm.fill_rate < c.fillrate_ajustado then c.fillrate_ajustado
else dlm.fill_rate
end as fill_rate_ajustado
, dlm.channel as channel
, dlm.cd as cd
, dlm.proveedor_cd as proveecd
, dlm.capacidad_caja as capcaja
, dlm.status_0917 as status0917
from constantes c, ecommdata.datos_logisticos_mfc dlm
left join ecommdata.maestra_slotting ms on ms.ref_id = dlm.ref_id
group by 1,2,3,4,5,6,7,8,9,10,11
), tablahistorica as (
select ved.ref_id_sku as ref_id
, sum(ved.venta_umv)::integer as venta_total
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion <= current_date
and ved.id_tienda in ('0917')
group by 1
)
select t.*
, tv.ventatotal
, tv.promedioventadiario
, dm.leadtime
, dm.frecuenciarepo
, dm.fuenteapro
, dm.fillrate
, dm.fill_rate_ajustado
, dm.channel
, dm.cd
, dm.proveecd
, dm.capcaja
, dm.status0917
, t.vida_util * (1-c.porc_dias_habiles) as normaretiro
, case 
when tv.promedioventadiario = 0 
then 'Candidato a Merma'
when t.numerador_ump/tv.promedioventadiario > t.vida_util_maxima 
then 'Candidato a Merma'
else 'No mermable'
end as candidato_merma
, (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado as stock_objetivo_teorico
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end as stock_objetivo_real
, case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end as cantidadmerma
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma1 then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_10
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma2 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_05
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) > (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma3 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_00
/*
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma1 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_10,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma2 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_05,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= 0 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_00,
*/
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen
end as volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax
end as totes_volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax)/c.fracTote)*fracTote 
else ceil((ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax)/c.fracTote)*c.fracTote 
end as totes_volumen_aproximado
, case 
when (t.ranking <= 500) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador a Piso'
else ''
end as ganador
, case 
when (t.ranking <= 1000) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador por Ranking'
else ''
end as ganador_ranking
, t.ranking as numero_ranking
, th.venta_total as unidades_vendidas_historicas
from constantes c, tablita t 
inner join tablaventa tv 
on t.ref_id = tv.ref_id
left join datosmfc dm 
on dm.ref_id = t.ref_id
inner join tablahistorica th 
on th.ref_id = t.ref_id
where t.ref_id not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
and t.ref_id not in ('000000000000666148-UN',
'000000000000663748-UN',
'000000000000942475-UN',
'000000000000655618-UN') -- Restricciones solicitadas por correo 19/03/2025
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
order by tv.ventatotal desc);


-- =============================================================================
-- ESCENARIO 500: Multiplicador de venta = 8 (1200 ordenes aprox)
-- =============================================================================
truncate table ecommdata.maestra_slotting_mfc_500;

insert into ecommdata.maestra_slotting_mfc_500(ref_id, nombre, umv, ump, sku_key, n1, n2, n3, marca, proveedor, id_proveer, peso_bruto, altura, ancho, condicion_de_almacenaje, brnd_id, categoria_material_desc, gds_pd_tp_dsc, contenido_bruto, contenido_neto, grado_alcoholico, longitud, marca_propia, numerador_ump, pais_origen_id, peso_neto, um_contenido, unidad, unidad_de_medida_pedido, unidad_de_volumen, unidad_laa, unidad_peso, volumen, vida_util, vida_util_maxima, temperature_zone, ranking, ventatotal, promedioventadiario, leadtime, frecuenciarepo, fuenteapro, fillrate, fill_rate_ajustado, channel, cd, proveecd, capcaja, status0917, normaretiro, candidato_merma, stock_objetivo_teorico, stock_objetivo_real, cantidadmerma, restriccion_merma_10, restriccion_merma_05, restriccion_merma_00, volumen_proyectado, totes_volumen_proyectado, totes_volumen_aproximado, ganador, ganador_ranking, numero_ranking, venta_historica_unidades)
(with constantes(dias, merma1, merma2, merma3, volumenMax, pesoMax, fillrate_ajustado, fracTote, fact_ampl_venta, porc_dias_habiles) as(
values(7, 0.1, 0.05, 0.00, 61867.5*0.7, 22275, 0.6, 0.125, 8, 0.7) --mult. venta 2.5, 4 y 8 
), tablita as(
select ved.ref_id_sku as "ref_id"
, p.nombre
, ms.umv
, ms.ump
, ms.sku_key
, c.n1 as "n1"
, c.n2 as "n2"
, c.n3 as "n3"
, ms.marca
, ms.proveedor
, ms.id_proveer
, ms.peso_bruto
, ms.altura as "altura"
, ms.ancho as "ancho"
, ms.condicion_de_almacenaje
, ms.brnd_id
, ms.categoria_material_desc
, ms.gds_pd_tp_dsc
, ms.contenido_bruto
, ms.contenido_neto
, ms.grado_alcoholico
, ms.longitud::float4
, case 
when ms.marca_propia = 1 then true 
when ms.marca_propia = 0 then false 
else null
end as marca_propia
, ms.numerador_ump::integer
, ms.pais_origen_id
, ms.peso_neto
, ms.um_contenido
, ms.unidad
, ms.unidad_de_medida_pedido
, ms.unidad_de_volumen
, ms.unidad_laa
, ms.unidad_peso
, ms.volumen
, ms.vida_util
, ms.vida_util * 0.7 as vida_util_maxima
, ms.temperature_zone
, rpt.ranking
from constantes ctts, ecommdata.maestra_slotting ms 
inner join ecommdata.lista8 l 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ms.ref_id
inner join ecommdata.ventas_ecommerce_datawarehouse ved 
on concat(lpad(l.material, 18, '0'), '-', l.umv) = ved.ref_id_sku
inner join ecommdata.productos p 
on p.ref_id = ved.ref_id_sku 
inner join ecommdata.categorias c 
on c.id = p.id_categoria 
inner join ecommdata.ranking_productos_tienda rpt 
on rpt.ref_id_sku = ms.ref_id 
and rpt.id_tienda = l.id_tienda
where l.id_tienda in ('0917')
and ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and c.n1 not in ('No Trabajar', 'Congelados', 'Inactivos', 'Sustitución', 'Integración', 'Lácteos, Huevos y Refrigerados')--, 'Carnes') --, 'Carnes', 'Frutas y Verduras')--, 'Quesos y Fiambres' , 'Lácteos, Huevos y Refrigerados', 'Carnes', // 
and l.material not in ('964689','641390','611439','611438','606780','606782','606781','611440','611441','611442','611444','611443','611445','634301','634302','608113','608115','608103','608116','609126','622031','622035','613938','613937','622033','612172','612134','633966','612173','608601','608602','676471','610521','610890','608603','622032','622034','614627','614728','968898','969103','814831','609252','605168','604790','604789','604170','979614','969109','969110','969100','969105','624152','969101','969102','968899','969104','969106','969107','611527','609117','969108','979615','618138','618139','636591','645919002','645919','645919001','646074','641647','697682','646689','813249','151213','160536','616815','635703','708039','661855','661945','718741','928741','200713','641664','640072','18224','640143','652490','665715','653400','642726','649233','649227','649253','649078001','649078002','662181','646509','662219','662180','616932','616933','663049','654683','628876','628877','650176','655581','624859','624860','648518','645915','642725','648933','648932','645207','642724','642723','170771','638775','638776','635136','655580','655579','645360','624858','662255','645361','649231','648519','623247','664311','664310','645914','645913','638538','622703','665319','566801','662184','665059','665818','662170','663023','565721','668163') --restricciones logísticas
and ms.is_hazardous = false
and l.umv not in ('KG', 'KGV')
and ved.ref_id_sku not in ('000000000000007482-DIS', '000000000000007493-DIS', '000000000000007504-DIS', '000000000000008892-UN', '000000000000130191-DIS', '000000000000130196-DIS', '000000000000155394-DIS', '000000000000158479-DIS', '000000000000160531-UN', '000000000000176714-UN', '000000000000190186-DIS', '000000000000209675-UN', '000000000000506345-DIS', '000000000000578191-DIS', '000000000000626247-UN', '000000000000631249-UN', '000000000000634650-PAQ', '000000000000635931-DIS', '000000000000649045-UN', '000000000000661882-UN', '000000000000665043-DIS', '000000000000668149-UN', '000000000000901934-DIS', '000000000000985415-DIS', '000000000400046001-DIS', '000000000400070001-DIS', '000000000402613001-DIS', '000000000402613002-DIS', '000000000402629001-DIS', '000000000402629003-DIS', '000000000402629004-DIS', '000000000605007001-DIS', '000000000650059001-UN', '000000000650059002-UN')
--and ved.ref_id_sku not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
having 
(ms.altura::real)*10 <= 300 or
(ms.ancho::real)*10 <= 365 or
(ms.longitud::real)*10 <= 565 or
(ms.peso_bruto::integer) <= 25000-2725 or
(ms.volumen::real) <= (30*36.5*56.5)*0.7 --restriccion 30% aire
), tablaventa as(
select ved.ref_id_sku as ref_id 
, sum(ved.venta_umv)::integer as ventatotal
, (sum(ved.venta_umv)/180)::real as promedioventadiario
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion >= current_date - 180
and ved.fecha_facturacion <= current_date
and ved.fecha_facturacion not between '2024-09-30' and '2024-10-02'
--and ved.fecha_facturacion <= '2024-10-02'
and ved.fecha_facturacion not between '2024-11-28' and '2024-12-04'
and ved.id_tienda in ('0917')
group by 1
), datosmfc as(
select dlm.ref_id
, dlm.leadtime as leadtime
, dlm.frecuencia_reposicion as frecuenciarepo
, dlm.fuente_aprovisionamiento as fuenteapro
, dlm.fill_rate as fillrate
, case
when dlm.fill_rate < c.fillrate_ajustado then c.fillrate_ajustado
else dlm.fill_rate
end as fill_rate_ajustado
, dlm.channel as channel
, dlm.cd as cd
, dlm.proveedor_cd as proveecd
, dlm.capacidad_caja as capcaja
, dlm.status_0917 as status0917
from constantes c, ecommdata.datos_logisticos_mfc dlm
left join ecommdata.maestra_slotting ms on ms.ref_id = dlm.ref_id
group by 1,2,3,4,5,6,7,8,9,10,11
), tablahistorica as (
select ved.ref_id_sku as ref_id
, sum(ved.venta_umv)::integer as venta_total
from ecommdata.ventas_ecommerce_datawarehouse ved 
where ved.fecha_facturacion <= current_date
and ved.id_tienda in ('0917')
group by 1
)
select t.*
, tv.ventatotal
, tv.promedioventadiario
, dm.leadtime
, dm.frecuenciarepo
, dm.fuenteapro
, dm.fillrate
, dm.fill_rate_ajustado
, dm.channel
, dm.cd
, dm.proveecd
, dm.capcaja
, dm.status0917
, t.vida_util * (1-c.porc_dias_habiles) as normaretiro
, case 
when tv.promedioventadiario = 0 
then 'Candidato a Merma'
when t.numerador_ump/tv.promedioventadiario > t.vida_util_maxima 
then 'Candidato a Merma'
else 'No mermable'
end as candidato_merma
, (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado as stock_objetivo_teorico
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end as stock_objetivo_real
, case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end as cantidadmerma
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma1 then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_10
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) >= (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma2 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_05
, case
when (case
when tv.promedioventadiario = 0 
then t.numerador_ump
when (case
when dm.channel in ('Picking Unitario', 'PBL') 
then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima) < 0 
then 0
else (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end) - (c.fact_ampl_venta*tv.promedioventadiario*t.vida_util_maxima)
end
) > (case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
end)*c.merma3 
then 'Rompe Restriccion Merma'
else 'No Rompe Restriccion Merma'
end as restriccion_merma_00
/*
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma1 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_10,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= (case
						when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
						else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
					end)*c.merma2 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_05,
case
	when (case
			when tv.promedioventadiario = 0 then t.numerador_ump
			when (case
					when dm.channel in ('Picking Unitario', 'PBL') then ((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)
					else (ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump)
				end) < 0 then 0
			else (case
					when dm.channel in ('Picking Unitario', 'PBL') then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado 
					else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump
				 end) - (tv.promedioventadiario*t.vida_util_maxima)
			end) >= 0 then 'Rompe Restriccion Merma'
	else 'No Rompe Restriccion Merma'
end as restriccion_merma_00,
*/
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen
end as volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then (tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax
else ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax
end as totes_volumen_proyectado
, case
when dm.channel in ('Picking Unitario', 'PBL') 
then ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado * t.volumen /c.volumenMax)/c.fracTote)*fracTote 
else ceil((ceil(((tv.promedioventadiario*c.fact_ampl_venta)*(c.dias+dm.leadtime)/dm.fill_rate_ajustado)/t.numerador_ump)*t.numerador_ump * t.volumen /c.volumenMax)/c.fracTote)*c.fracTote 
end as totes_volumen_aproximado
, case 
when (t.ranking <= 500) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador a Piso'
else ''
end as ganador
, case 
when (t.ranking <= 1000) and (dm.fill_rate_ajustado >= 0.85) 
then 'Ganador por Ranking'
else ''
end as ganador_ranking
, t.ranking as numero_ranking
, th.venta_total as unidades_vendidas_historicas
from constantes c, tablita t 
inner join tablaventa tv 
on t.ref_id = tv.ref_id
left join datosmfc dm 
on dm.ref_id = t.ref_id
inner join tablahistorica th 
on th.ref_id = t.ref_id
where t.ref_id not in ('000000000000806586-UN', '000000000000190298-UN', '000000000000210563-UN', '000000000000647749-UN', '000000000000627622-UN', '000000000000034974-UN', '000000000000779841-UN', '000000000000663282-UN', '000000000000663283-UN', '000000000000652714-UN', '000000000000659412-UN', '000000000000638916-UN', '000000000000648055-UN', '000000000000622487-UN', '000000000000638915-UN', '000000000000622491-UN', '000000000000648056-UN', '000000000655976002-UN', '000000000000652176-UN', '000000000000649246-UN', '000000000000649247-UN', '000000000000649944-UN', '000000000000622488-UN', '000000000000634409-UN', '000000000000648912-UN', '000000000000748296-UN', '000000000000748600-UN', '000000000000344508-UN', '000000000000627635-UN', '000000000000578195-UN', '000000000000898722-UN', '000000000000627636-UN', '000000000000647710-UN', '000000000000554395-UN', '000000000000118350-UN', '000000000000646204-UN', '000000000000640493-UN', '000000000000661945-UN', '000000000000662966-UN', '000000000000634986-UN', '000000000000665210-UN', '000000000000646518-UN', '000000000000166752-UN', '000000000000664044-UN', '000000000000661996-UN', '000000000000026931-UN', '000000000000662363-UN', '000000000000653643-UN', '000000000647993001-UN', '000000000000942478-UN', '000000000000618668-UN', '000000000000622894-DIS', '000000000647993002-UN', '000000000000815166-UN', '000000000000628027-UN', '000000000000652281-UN', '000000000649669002-UN', '000000000650777003-UN', '000000000650777001-UN', '000000000650777002-UN', '000000000000636543-UN', '000000000000649324-UN', '000000000636563002-UN', '000000000636563001-UN', '000000000000649323-UN', '000000000000155964-UN', '000000000652949001-UN', '000000000000764939-UN', '000000000000764940-UN', '000000000000139941-UN', '000000000402106001-UN', '000000000402053001-UN', '000000000000663956-UN', '000000000639559001-UN', '000000000000063095-UN', '000000000000565611-UN', '000000000626948001-UN', '000000000626948003-UN', '000000000626948002-UN', '000000000402197012-UN', '000000000402197013-UN', '000000000000193700-UN', '000000000000913288-UN', '000000000654959001-UN', '000000000000576935-UN', '000000000629405002-UN', '000000000000629403-UN', '000000000000640664-UN', '000000000000611199-UN', '000000000000638865-UN', '000000000000014362-UN', '000000000000167550-UN', '000000000000635684-UN', '000000000000014296-UN', '000000000626713001-UN', '000000000000983940-UN', '000000000000983938-UN', '000000000000983939-UN', '000000000000538532-UN', '000000000000752685-UN', '000000000000913285-UN', '000000000000913286-UN', '000000000000786073-UN', '000000000400531002-UN', '000000000000649724-UN', '000000000000013880-UN', '000000000000636691-UN', '000000000000818375-UN', '000000000000018616-UN', '000000000000018622-UN', '000000000000018618-UN', '000000000000136557-UN', '000000000000172635-UN', '000000000000208631-UN', '000000000000608901-UN', '000000000000608899-UN', '000000000000649851-UN', '000000000000664077-UN', '000000000401867008-UN', '000000000000520256-UN', '000000000000170132-UN', '000000000000170198-UN', '000000000000510814-UN', '000000000401810001-UN', '000000000000815902-UN', '000000000000573878-UN', '000000000000663096-UN', '000000000000653469-UN', '000000000000653468-UN', '000000000000573880-UN', '000000000000663400-UN', '000000000000640610-UN', '000000000401930002-UN', '000000000401930001-UN', '000000000652891001-UN', '000000000648438003-UN', '000000000648438001-UN', '000000000000014229-UN', '000000000000972010-UN', '000000000637459001-UN', '000000000000170201-UN', '000000000639492001-UN', '000000000000625810-UN', '000000000000652564-UN', '000000000000171578-UN', '000000000000019067-UN', '000000000000632922-UN', '000000000624299003-UN', '000000000616726003-UN', '000000000616726002-UN', '000000000000642531-UN', '000000000000638439-UN', '000000000000626614-UN', '000000000000563556-UN', '000000000000991222-UN', '000000000000991223-UN', '000000000000991225-UN', '000000000000991226-UN', '000000000000832664-UN', '000000000633275001-UN', '000000000000781352-UN', '000000000000781348-UN', '000000000000781349-UN', '000000000000628528-UN', '000000000000636692-UN', '000000000628439001-UN', '000000000628439002-UN', '000000000000628527-UN', '000000000000636150-UN', '000000000000794095-UN', '000000000000616738-UN', '000000000000626711-UN', '000000000402340001-UN', '000000000000163394-UN', '000000000000018250-UN', '000000000000018288-UN', '000000000000544605-UN', '000000000000199738-UN', '000000000000018292-UN', '000000000000018287-UN', '000000000000018286-UN', '000000000000201453-UN', '000000000000158332-UN', '000000000000712263-UN', '000000000000321467-UN', '000000000000653163-UN', '000000000000649672-UN', '000000000000649673-UN', '000000000000202797-UN', '000000000000628239-UN', '000000000000035200-UN', '000000000000218460-UN', '000000000000187099-UN', '000000000000606932-UN', '000000000000610354-UN', '000000000000144019-UN', '000000000000665917-UN', '000000000000665916-UN', '000000000000547078-UN', '000000000000052107-UN', '000000000000652515-UN', '000000000000027332-UN', '000000000000035162-UN', '000000000000035183-UN', '000000000000641384-UN', '000000000000647801-UN', '000000000000662342-UN', '000000000000641385-UN', '000000000000664327-UN', '000000000000607456-UN', '000000000000027330-UN', '000000000000649629-UN', '000000000000201451-UN', '000000000000614049-UN', '000000000000648431-UN', '000000000000664980-UN', '000000000000664979-UN', '000000000000664977-UN', '000000000000664974-UN', '000000000000650007-UN', '000000000000649970-UN', '000000000000650009-UN', '000000000000650008-UN', '000000000000653267-UN', '000000000000653268-UN', '000000000000653269-UN', '000000000000038172-UN', '000000000000038308-UN', '000000000000038038-UN', '000000000000614386-UN', '000000000649719002-UN', '000000000000743005-UN', '000000000000655848-UN', '000000000000668143-UN', '000000000000668144-UN', '000000000000665156-UN', '000000000000500747-UN', '000000000000500749-UN', '000000000000500710-UN', '000000000000639440-UN', '000000000000655683-UN', '000000000649940002-UN', '000000000000146753-UN', '000000000000653792-UN', '000000000650305001-UN', '000000000650305002-UN')
and t.ref_id not in ('000000000000666148-UN',
'000000000000663748-UN',
'000000000000942475-UN',
'000000000000655618-UN') -- Restricciones solicitadas por correo 19/03/2025
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
order by tv.ventatotal desc);


commit;
