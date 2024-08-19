BEGIN TRANSACTION;
truncate table ecommdata.maestra_informacion_slotting_mfc;
INSERT INTO ecommdata.maestra_informacion_slotting_mfc (
    ref_id, 
    descripcion, 
    mfc_is_item_side, 
    ranking, 
    tipo_abastecimiento, 
    temperature_zone, 
    mfc_is_hazardous, 
    mfc_is_heavy, 
    gross_weight, 
    mfc_is_safety, 
    mfc_is_egg, 
    useful_life, 
    min_remaining_shelf_life, 
    case_measurement_unit, 
    conversion_to_base_measurement_unit, 
    case_qty_measurement_unit, 
    volumen_c3, 
    es_conveyable, 
    es_voluminoso, 
    es_pesable, 
    peso_gramos, 
    restriccion_logistica, 
    comanda, 
    is_promo, 
    criterio_merma, 
    domingo, 
    lunes, 
    martes, 
    miercoles, 
    jueves, 
    viernes, 
    sabado, 
    venta_promedio, 
    cantidad_dias, 
    "1/1", 
    "1/2", 
    "1/4", 
    "1/8", 
    totes_teoricos, 
    "Cant_productos", 
    ubicaciones, 
    "Totes", 
    stock_manual, 
    stock_osr, 
    stock_mfc, 
    inventario_objetivo, 
    inventario_objetivo_transporte
)
with venta_por_dia as (
	SELECT 
	    ref_id,
	    id_tienda,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 0 THEN venta_dia ELSE NULL END), 2) AS domingo,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 1 THEN venta_dia ELSE NULL END), 2) AS lunes,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 2 THEN venta_dia ELSE NULL END), 2) AS martes,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 3 THEN venta_dia ELSE NULL END), 2) AS miercoles,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 4 THEN venta_dia ELSE NULL END), 2) AS jueves,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 5 THEN venta_dia ELSE NULL END), 2) AS viernes,
	    ROUND(AVG(CASE WHEN dia_de_la_semana = 6 THEN venta_dia ELSE NULL END), 2) AS sabado,
	    round(avg(venta_dia),2) as venta_promedio,
	    count(venta_dia) as cantidad_dias
	FROM (
	    SELECT 
	        ref_id,
	        id_tienda,
	        DATE_PART('dow', fecha_facturacion) AS dia_de_la_semana,
	        SUM(venta_umv) AS venta_dia
	    FROM ecommdata.venta_regular_mfc vrm 
	    WHERE apo IS NOT TRUE  
	      AND porcenta_descuento < 0.3
	    GROUP BY ref_id, id_tienda, fecha_facturacion
	) AS ventas_por_dia
	GROUP BY ref_id, id_tienda
	ORDER BY ref_id, id_tienda),
mfc as (SELECT 
	    "Article number",
	    "Article name",
	    SUM(CASE WHEN "Stock location type" = '01-ene' THEN 1 ELSE 0 END) AS "1/1",
	    SUM(CASE WHEN "Stock location type" = '01-feb' THEN 1 ELSE 0 END) AS "1/2",
	    SUM(CASE WHEN "Stock location type" = '01-abr' THEN 1 ELSE 0 END) AS "1/4",
	    SUM(CASE WHEN "Stock location type" = '01-ago' THEN 1 ELSE 0 END) AS "1/8",
	    sum(CASE WHEN "Stock location type" = '01-ene' THEN 1 ELSE 0 END)* 1 +
	    sum(CASE WHEN "Stock location type" = '01-feb' THEN 1 ELSE 0 END)* 0.5 +
	    sum(CASE WHEN "Stock location type" = '01-abr' THEN 1 ELSE 0 END)* 0.25  +
	    sum(CASE WHEN "Stock location type" = '01-ago' THEN 1 ELSE 0 END)* 0.125 AS "totes_teoricos",
	    SUM(quantity) AS "Cant_productos",
	    COUNT("Load unit") as "ubicaciones",
	    count(distinct "Load unit") as "Totes"
	FROM 
	    ecommdata.inventario_osr_mfc iom
	WHERE 
	    fecha_carga = (select max(fecha_carga) from ecommdata.inventario_osr_mfc where fecha_carga > '{{ds}}'::date-30)
	GROUP BY 
	    "Article number",
	    "Article name"
 order by 10 desc),
promo as (select distinct concat(material,'-', replace(umv,'ST','UN')) as ref_id
	from ecommdata.workflow_promociones wp 
	WHERE (wp.id_mecanica <> ALL (ARRAY[124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59]))
	AND wp.fecha_inicio_de_promocion <= '{{ds}}'::date--'{{ds}}'::date--cambiar a la fecha que se desee ver sumandole o restandole dias a la fecha actual
	AND wp.fecha_fin_de_promocion >= '{{ds}}'::date--'{{ds}}'::date
	and wp.tipo_promocion <> 3
	and wp.promo_event_mechanism = 'APO'
	and wp.descripcion_evento_promocional not in ('UNI,CL CATALOGO','UNI CATALOGO')
	and wp.n_promocion not in (5552152024,4000512024)
	AND wp.nombre_promocion::text !~~ '%MFC%'::text 
	AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text 
	AND wp.nombre_promocion::text !~~ '%917%'::text
	AND wp.nombre_promocion::text !~~ '%BANCO ESTADO%'::text
	AND wp.nombre_promocion::text !~~ '%LOC%'::text
	and wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
	AND wp.nombre_promocion::text !~~ '%HUACHALALUME%'::text
	and wp.nombre_promocion::text !~~ '%LOCAL%'::text),
inventario_osr as (
with ubi AS (
SELECT DISTINCT fecha_carga, "TOM ID" AS ref_id, "Quantity On-Hand" AS stock,
    CASE
        WHEN "Storage Area" LIKE '%DYNAMIC%' THEN 'DYNAMIC'
        WHEN "Storage Area" LIKE '%MANUAL%' THEN 'MANUAL'
        WHEN "Storage Area" LIKE '%OSR%' THEN 'OSR'
        ELSE NULL
    END AS ubi
FROM ecommdata.inventario_manual_mfc imm 
WHERE fecha_carga = '{{ds}}'::date--'{{ds}}'::date
)
select
	u.ref_id,
    SUM(CASE WHEN u.ubi = 'MANUAL' THEN u.stock ELSE 0 END) AS stock_manual,
    SUM(CASE WHEN u.ubi = 'OSR' THEN u.stock ELSE 0 END) AS stock_osr
from ubi as u
group by u.ref_id
)
select concat(l.material,'-',l.umv) as ref_id,l.descripcion , um.mfc_is_item_side , rpt.ranking ,tam.tipo_abastecimiento,
um.temperature_zone, um.mfc_is_hazardous, um.mfc_is_heavy , um.gross_weight, um.mfc_is_safety, um.mfc_is_egg,um.useful_life,
um.min_remaining_shelf_life, um.case_measurement_unit, um.conversion_to_base_measurement_unit, um.case_qty_measurement_unit::int,
ROUND(CAST(um.length * um.height * um.width AS numeric), 2) as volumen_c3,
case 
	when um.length > 0.555*100 then 'No conveyable 1'
	when um.width > 0.356*100 then 'No conveyable 2'
	else 'conveyable'
end as es_conveyable,
case 
	when (um.length * um.height * um.width)/100000>0.073 then true --es voluminoso?
	else false
end as es_voluminoso,
case 
	when l.umv in ('KG','KGV') then true --es pesable?
	else false
end es_pesable,
case 
	when um.gross_measurement_unit = 'G' then um.gross_weight
	when um.gross_measurement_unit = 'KG' then um.gross_weight*1000 --peso en gramos
	else 0
end as peso_gramos,
case
	when rlm.material is not null then true -- tiene restriccion logistica?
	else false
end as restriccion_logistica,
case
	when mrm.material is not null then true --está en la comanda?
	else false
end as comanda,
case 
	when p.ref_id is not null then true  --es promo?
	else false
end is_promo,
case 
	when um.useful_life * 0.7 * ROUND(CAST((v.domingo+v.lunes+v.martes+v.miercoles+v.jueves+v.viernes+v.sabado)/7 AS numeric), 2) >= um.case_qty_measurement_unit::int * 0.9 then True
    else False
end as criterio_merma,
v.domingo,v.lunes,v.martes,v.miercoles,v.jueves,v.viernes,v.sabado,ROUND(CAST((v.domingo+v.lunes+v.martes+v.miercoles+v.jueves+v.viernes+v.sabado)/7 AS numeric), 2) as venta_promedio,v.cantidad_dias,
mfc."1/1",mfc."1/2",mfc."1/4",mfc."1/8",mfc."totes_teoricos",mfc."Cant_productos",mfc."ubicaciones",mfc."Totes",
io.stock_manual,io.stock_osr,io.stock_manual+io.stock_osr as stock_mfc, ROUND(CAST(v.domingo+v.lunes+v.martes+v.miercoles+v.jueves+v.viernes+v.sabado AS numeric), 2) as inventario_objetivo,
CEIL(ROUND(CAST(v.domingo+v.lunes+v.martes+v.miercoles+v.jueves+v.viernes+v.sabado AS numeric), 2) / um.case_qty_measurement_unit::int) * um.case_qty_measurement_unit::int as inventario_objetivo_transporte
from ecommdata.lista8 l 
left join ecommdata.ubicacion_mfc um on l.material = um.sap_code and l.umv = um.measurement_unit 
left join ecommdata.ranking_productos_tienda rpt on l.id_tienda = rpt.id_tienda and rpt.ref_id_sku = concat(l.material,'-',l.umv)
left join venta_por_dia as v on v.ref_id = concat(l.material,'-',l.umv) and v.id_tienda = l.id_tienda
left join ecommdata.tipo_abastecimiento_mfc tam on tam.material = um.sap_code and tam.id_tienda = um.store 
left join mfc as mfc on concat(l.material,'-',l.umv) = mfc."Article number"
left join ecommdata.maestra_reposicion_mfc mrm on mrm.material = l.material 
left join ecommdata.restricciones_logisticas_mfc rlm on rlm.material = l.material 
left join promo as p on p.ref_id = concat(l.material,'-',l.umv)
left join inventario_osr as io on io.ref_id = concat(l.material,'-',l.umv)
where l.id_tienda = '1917'
order by rpt.ranking;
COMMIT