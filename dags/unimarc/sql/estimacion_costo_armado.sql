INSERT INTO forecast_and_planning.estimacion_costo_armado (
    id_orden, fecha_entrega, despachado, empleado, rut, tienda, id_tienda,
    transportadora, id_transportadora, operador, modelo_cobro, sku, kilometros,
    tarifa_sku, tarifa_km, total_base, total_sku, total_km, costo_total_pedido,
    tarifa_asegurado, dotacion, duplicado
)
SELECT p.id_orden,
    p.fecha_entrega,
    p.despachado,
    p.empleado,
    p.rut,
    p.tienda,
    p.id_tienda,
    p.transportadora,
    p.id_transportadora,
        CASE
            WHEN tp.operador::text = 'Zubale + Rayo APP'::text THEN 'Zubale'::character varying
            WHEN tp.operador::text = 'Boosmap + Rayo APP'::text THEN 'Rayo APP'::character varying
            ELSE tp.operador
        END AS operador,
    tp.modelo_cobro,
    p.sku,
    p.kilometros,
    tp.tarifa_sku,
    tp.tarifa_km,
    tp.tarifa_base AS total_base,
    tp.tarifa_sku * p.sku AS total_sku,
        CASE
            WHEN p.kilometros IS NOT NULL AND p.despachado = 'si'::text THEN p.kilometros * tp.tarifa_km::numeric
            ELSE 0::numeric
        END AS total_km,
    (tp.tarifa_base + tp.tarifa_sku * p.sku)::numeric +
        CASE
            WHEN p.kilometros IS NOT NULL AND p.despachado = 'si'::text THEN p.kilometros * tp.tarifa_km::numeric
            ELSE 0::numeric
        END AS costo_total_pedido,
    tp.tarifa_asegurado,
    f.dotacion,
    f.duplicado
   FROM forecast_and_planning.pedidos_prefactura p
     LEFT JOIN forecast_and_planning.tarifas_prefacturas tp ON tp.id_transportadora::text = p.id_transportadora::text
     LEFT JOIN forecast_and_planning.forecast f ON p.fecha_entrega = f.fecha::date AND p.id_tienda::text = f.id_tienda::text AND tp.modelo_cobro::text = f.modelo::text AND tp.operador::text = f.operador::text AND p.id_transportadora::text = f.id_transportadora::text
  WHERE p.pickeada = 'si'::text AND tp.id_transportadora IS NOT NULL AND f.dotacion > 0 AND p.fecha_entrega = '{{ macros.ds_add(ds, -1) }}'::date
ON CONFLICT (id_orden) DO NOTHING;
