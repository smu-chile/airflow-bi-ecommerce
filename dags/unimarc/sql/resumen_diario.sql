insert into ecommdata.resumen_diario
select ved.fecha_facturacion::date as fecha_facturacion
, ved.id_tienda
, SUM(ved.venta_neta) as venta_neta
, COUNT(distinct ved.id_orden) as total_ordenes
, SUM(ved.venta_neta)/COUNT(distinct ved.id_orden) as ticket_promedio
, _t.productos_completos
, _t.productos_substituidos
, _t.productos_solicitados
, _t.ordenes_perfectas
, _t3.ordenes_ontime
, _t3.ordenes_earlytime
, _t4.venta_neta as venta_sala
from ecommdata.ventas_ecommerce_datawarehouse ved
inner join ecommdata.ordenes_janis oj on ved.id_orden = oj.id 
inner join (select _t2.id_tienda
	, _t2.fecha_facturacion::date as fecha_facturacion
	, sum(_t2.productos_completos) as productos_completos
	, sum(_t2.productos_substituidos) as productos_substituidos
	, sum(_t2.productos_solicitados) as productos_solicitados
	, sum(
        CASE
            WHEN _t2.estado_foundrate_orden = 3 THEN 1
            ELSE 0
        END) as ordenes_perfectas
    , count(DISTINCT _t2.orden) as total_ordenes_found_rate
   FROM (select fr.fecha_facturacion,
        	fr.orden,
            fr.id_tienda,
            sum(
                CASE
                    WHEN fr.producto_substituto = false THEN 1
                    ELSE 0
                END) AS productos_solicitados,
            sum(
                CASE
                    WHEN fr.estado_foundrate = 3 THEN 1
                    ELSE 0
                END) AS productos_completos,
            sum(
                CASE
                    WHEN fr.estado_foundrate = 1 THEN 1
                    ELSE 0
                END) AS productos_incompletos,
            sum(
                CASE
                    WHEN fr.producto_substituido = true THEN 1
                    ELSE 0
                END) AS productos_substituidos,
            min(fr.estado_foundrate) AS estado_foundrate_orden
            FROM operaciones_unimarc.found_rate_productos fr
            WHERE fr.fecha_facturacion::date = '{{ds}}'
            GROUP BY fr.fecha_facturacion, fr.orden, fr.id_tienda) _t2
	GROUP BY _t2.fecha_facturacion, _t2.id_tienda) _t on _t.id_tienda = ved.id_tienda and _t.fecha_facturacion = ved.fecha_facturacion
inner join (select cd.fecha_facturacion ,
    cd.id_tienda, sum(
        CASE
            WHEN cd.cumplimiento_ontime = 10 THEN 1
            ELSE 0
        END) AS ordenes_ontime
	, sum(
        CASE
            WHEN cd.cumplimiento_ontime = 20 THEN 1
            ELSE 0
        END) AS ordenes_earlytime
	from operaciones_unimarc.cumplimiento_despacho cd
	where cd.fecha_facturacion::date = '{{ds}}' 
	group by cd.fecha_facturacion , cd.id_tienda) _t3 on _t3.fecha_facturacion::date = ved.fecha_facturacion::date and _t3.id_tienda = ved.id_tienda
inner join ecommdata.venta_locales_pbi _t4 on _t4.id_tienda = ved.id_tienda and _t4.fecha = ved.fecha_facturacion::date
where ved.fecha_facturacion = '{{ds}}' and ved.canal_venta = 'E-COMMERCE'
group by ved.fecha_facturacion::date, ved.id_tienda, _t.productos_completos, _t.productos_substituidos, _t.productos_solicitados, _t.ordenes_perfectas, _t3.ordenes_ontime, _t3.ordenes_earlytime, _t4.venta_neta
order by ved.id_tienda asc
on conflict (fecha_facturacion, id_tienda) do update
set fecha_facturacion = EXCLUDED.fecha_facturacion
	, id_tienda = EXCLUDED.id_tienda
	, venta_neta = EXCLUDED.venta_neta
	, total_ordenes = EXCLUDED.total_ordenes
	, ticket_promedio = EXCLUDED.ticket_promedio
	, productos_completos = EXCLUDED.productos_completos
	, productos_substituidos = EXCLUDED.productos_substituidos
	, productos_solicitados = EXCLUDED.productos_solicitados
	, ordenes_perfectas = EXCLUDED.ordenes_perfectas
	, ordenes_ontime = EXCLUDED.ordenes_ontime
	, ordenes_earlytime = EXCLUDED.ordenes_earlytime
	, venta_sala = EXCLUDED.venta_sala
