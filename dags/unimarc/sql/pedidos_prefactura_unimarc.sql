INSERT INTO forecast_and_planning.pedidos_prefactura_unimarc (
  id_orden, nombre_estado, fecha_entrega, fecha_despacho, pickeada, despachado,
  empleado, rut, tienda, id_tienda, transportadora, id_transportadora,
  operador, tipo, sku, kilometros
)
WITH cambios_estado_70 AS (
         SELECT DISTINCT ocde.id,
            ocde.id_orden,
            ocde.estado_anterior,
            ocde.estado_nuevo,
            ocde.creado_por,
            ocde.fecha_creacion,
            ocde.fecha_creacion_unixtime
           FROM ecommdata.orden_cambios_de_estado ocde
          WHERE ocde.estado_nuevo = 70
        ), estado_en_picking AS (
         SELECT oj_1.id AS id_orden,
            max(ocde.fecha_creacion::date) AS fecha_picking,
            count(ocde.id) AS estados_pck,
            t_1.glosa AS tienda
           FROM ecommdata.orden_cambios_de_estado ocde
             LEFT JOIN ecommdata.ordenes_janis oj_1 ON oj_1.janis_id = ocde.id_orden
             LEFT JOIN ecommdata.estado_orden_janis eoj_1 ON ocde.estado_nuevo = eoj_1.id_estado
             LEFT JOIN ecommdata.tiendas t_1 ON t_1.id_janis = oj_1.id_tienda_janis
          WHERE ocde.fecha_creacion::date = '{{ macros.ds_add(ds, -1) }}'::date AND (eoj_1.nombre_estado::text = ANY (ARRAY['En Picking'::text, 'Pendiente de Auditoria'::text, 'Pendiente Re-Picking'::text, 'Pickeado por Facturar'::text, 'Procesando Picking'::text, 'Procesando Promociones'::text, 'Auditoria Rechazada'::text, 'Devolución total'::text]))
          GROUP BY oj_1.id, t_1.glosa
        )
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
    round((COALESCE(fk.factor_kilometro, fk_default.factor_kilometro) * ro.distancia_metros::double precision / 1000.0::double precision)::numeric, 1) AS kilometros
   FROM ecommdata.ordenes_janis oj
     LEFT JOIN ecommdata.recorrido_orden ro ON ro.id_orden = oj.id
     LEFT JOIN ecommdata.despachos d ON d.id_orden = oj.id
     LEFT JOIN ecommdata.transportadoras t ON t.id::text = d.id_transportadora::text
     LEFT JOIN operaciones_unimarc.cumplimiento_despacho cd ON cd.id_orden = oj.id
     LEFT JOIN forecast_and_planning.factores_kilometros fk ON t."nombre_compañia_logistica"::text = fk."nombre_compañia_logistica"::text AND
        CASE
            WHEN cd.fecha_entrega IS NOT NULL THEN cd.fecha_entrega
            ELSE d.fecha_despacho::date
        END >= fk.fecha_inicio AND
        CASE
            WHEN cd.fecha_entrega IS NOT NULL THEN cd.fecha_entrega
            ELSE d.fecha_despacho::date
        END <= COALESCE(fk.fecha_fin, '9999-12-31'::date)
     LEFT JOIN forecast_and_planning.factores_kilometros fk_default ON fk_default."nombre_compañia_logistica"::text = 'default'::text
     LEFT JOIN ecommdata.tiendas t2 ON t2.id::text = t.id_tienda::text
     LEFT JOIN ecommdata.administradores a ON oj.id_picker = a.id
     LEFT JOIN ecommdata.estado_orden_janis eoj ON oj.estado_janis = eoj.id_estado
     LEFT JOIN cambios_estado_70 ce ON ce.id_orden = oj.janis_id
     LEFT JOIN estado_en_picking e ON e.id_orden = oj.id
  WHERE oj.fecha_creacion::date = '{{ macros.ds_add(ds, -1) }}'::date
ON CONFLICT (id_orden) DO NOTHING;
