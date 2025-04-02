SELECT oj.id AS id_orden,
    eoj.nombre_estado,
        CASE
            WHEN cd.fecha_entrega IS NOT NULL THEN cd.fecha_entrega
            ELSE d.fecha_despacho::date
        END AS fecha_entrega,
    d.fecha_despacho::date AS fecha_despacho,
        CASE
            WHEN e.id_orden IS NOT NULL THEN 'si'::text
            ELSE 'no'::text
        END AS pickeada,
        CASE
            WHEN ce.estado_nuevo IS NOT NULL THEN 'si'::text
            ELSE 'no'::text
        END AS despachado,
    concat(a.nombre, ' ', a.apellido) AS empleado,
    a.id_empleado AS rut,
    t2.glosa AS tienda,
    t2.id AS id_tienda,
    t.nombre AS transportadora,
    t.id AS id_transportadora,
    t."nombre_compañia_logistica" AS operador,
    t.tipo,
    oj.productos_solicitados AS sku,
        CASE
            WHEN t."nombre_compañia_logistica"::text = 'Timejobs'::text THEN round(1.4 * ro.distancia_metros::numeric / 1000.0, 1)
            WHEN t."nombre_compañia_logistica"::text = 'Boosmap'::text THEN round(2.15 * ro.distancia_metros::numeric / 1000.0, 1)
            WHEN t."nombre_compañia_logistica"::text = 'Touch'::text THEN round(1.47 * ro.distancia_metros::numeric / 1000.0, 1)
            WHEN t."nombre_compañia_logistica"::text = 'Zubale'::text THEN round(1.8 * ro.distancia_metros::numeric / 1000.0, 1)
            ELSE round(1.7 * ro.distancia_metros::numeric / 1000.0, 1)
        END AS kilometros
   FROM ecommdata.ordenes_janis oj
     LEFT JOIN ecommdata.recorrido_orden ro ON ro.id_orden = oj.id
     LEFT JOIN ecommdata.despachos d ON d.id_orden = oj.id
     LEFT JOIN ecommdata.transportadoras t ON t.id::text = d.id_transportadora::text
     LEFT JOIN ecommdata.tiendas t2 ON t2.id::text = t.id_tienda::text
     LEFT JOIN ecommdata.administradores a ON oj.id_picker = a.id
     LEFT JOIN ecommdata.estado_orden_janis eoj ON oj.estado_janis = eoj.id_estado
     LEFT JOIN operaciones_unimarc.cumplimiento_despacho cd ON cd.id_orden = oj.id
     LEFT JOIN ( SELECT DISTINCT ocde.id,
            ocde.id_orden,
            ocde.estado_anterior,
            ocde.estado_nuevo,
            ocde.creado_por,
            ocde.fecha_creacion,
            ocde.fecha_creacion_unixtime
           FROM ecommdata.orden_cambios_de_estado ocde
          WHERE ocde.estado_nuevo = 70) ce ON ce.id_orden = oj.janis_id
     LEFT JOIN ( SELECT oj_1.id AS id_orden,
            max(ocde.fecha_creacion::date) AS fecha_picking,
            count(ocde.id) AS estados_pck,
            t_1.glosa AS tienda
           FROM ecommdata.orden_cambios_de_estado ocde
             LEFT JOIN ecommdata.ordenes_janis oj_1 ON oj_1.janis_id = ocde.id_orden
             LEFT JOIN ecommdata.estado_orden_janis eoj_1 ON ocde.estado_nuevo = eoj_1.id_estado
             LEFT JOIN ecommdata.tiendas t_1 ON t_1.id_janis = oj_1.id_tienda_janis
          WHERE ocde.fecha_creacion::date = '{{ds}}'::date AND (eoj_1.nombre_estado::text = ANY (ARRAY['En Picking'::character varying::text, 'Pendiente de Auditoria'::character varying::text, 'Pendiente Re-Picking'::character varying::text, 'Pickeado por Facturar'::character varying::text, 'Procesando Picking'::character varying::text, 'Procesando Promociones'::character varying::text, 'Auditoria Rechazada'::character varying::text, 'Devolución total'::character varying::text]))
          GROUP BY oj_1.id, t_1.glosa
          ORDER BY oj_1.id, (max(ocde.fecha_creacion::date))) e ON e.id_orden = oj.id
  WHERE oj.fecha_creacion::date = '{{ds}}'::date