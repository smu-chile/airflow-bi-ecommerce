INSERT INTO forecast_and_planning.estimacion_costo_asegurado (
    fecha_entrega,
    tienda,
    id_tienda,
    modelo_cobro,
    tarifa_asegurado,
    operador,
    id_transportadora,
    id_transportadoras,
    costo_armado,
    dotacion,
    minimo_asegurado,
    diferencia_asegurado,
    tipo_origen
)
WITH estimacion AS (
  SELECT *
  FROM forecast_and_planning.estimacion_costo_armado
  WHERE fecha_entrega = '{{ macros.ds_add(ds, -1) }}'
),
duplicados AS (
  SELECT
    a.fecha_entrega,
    a.tienda,
    a.id_tienda,
    a.modelo_cobro,
    a.tarifa_asegurado::BIGINT,
    a.operador,
    NULL::TEXT AS id_transportadora,
    STRING_AGG(DISTINCT a.id_transportadora::TEXT, ', ') AS id_transportadoras,
    SUM(a.costo_total_pedido) * COALESCE(f.factor_pago, 1)::NUMERIC AS costo_armado,
    MIN(a.dotacion) AS dotacion,
    MIN(a.dotacion) * a.tarifa_asegurado * COALESCE(f.factor_asegurado, 1) AS minimo_asegurado,
    GREATEST(
      (MIN(a.dotacion) * a.tarifa_asegurado * COALESCE(f.factor_asegurado, 1)) -
      (SUM(a.costo_total_pedido) * COALESCE(f.factor_pago, 1)),
      0
    ) AS diferencia_asegurado,
    'duplicado' AS tipo_origen
  FROM estimacion a
  LEFT JOIN forecast_and_planning.factores_asegurado f
    ON f.operador = a.operador
   AND f.modelo_cobro = a.modelo_cobro
   AND a.fecha_entrega BETWEEN f.fecha_inicio AND COALESCE(f.fecha_fin, '9999-12-31')
  WHERE a.duplicado = 1
  GROUP BY
    a.fecha_entrega, a.tienda, a.id_tienda,
    a.modelo_cobro, a.tarifa_asegurado, a.operador,
    f.factor_pago, f.factor_asegurado
),
no_duplicados AS (
  SELECT
    a.fecha_entrega,
    a.tienda,
    a.id_tienda,
    a.modelo_cobro,
    a.tarifa_asegurado::BIGINT,
    a.operador,
    a.id_transportadora,
    NULL::TEXT AS id_transportadoras,
    SUM(a.costo_total_pedido) * COALESCE(f.factor_pago, 1)::NUMERIC AS costo_armado,
    a.dotacion,
    a.dotacion * a.tarifa_asegurado * COALESCE(f.factor_asegurado, 1) AS minimo_asegurado,
    GREATEST(
      (a.dotacion * a.tarifa_asegurado * COALESCE(f.factor_asegurado, 1)) -
      (SUM(a.costo_total_pedido) * COALESCE(f.factor_pago, 1)),
      0
    ) AS diferencia_asegurado,
    'no_duplicado' AS tipo_origen
  FROM estimacion a
  LEFT JOIN forecast_and_planning.factores_asegurado f
    ON f.operador = a.operador
   AND f.modelo_cobro = a.modelo_cobro
   AND a.fecha_entrega BETWEEN f.fecha_inicio AND COALESCE(f.fecha_fin, '9999-12-31')
  WHERE a.duplicado = 0
  GROUP BY
    a.fecha_entrega, a.tienda, a.id_tienda,
    a.id_transportadora, a.modelo_cobro,
    a.tarifa_asegurado, a.dotacion, a.operador,
    f.factor_pago, f.factor_asegurado
),
forecast_filtrado AS (
  SELECT
    f.fecha,
    f.id_tienda,
    f.id_transportadora,
    f.modelo,
    f.operador,
    f.dotacion,
    f.ordenes,
    f.duplicado
  FROM forecast_and_planning.forecast f
  WHERE f.fecha = '{{ macros.ds_add(ds, -1) }}'
),
forecast_sin_pedidos AS (
  SELECT
    ff.fecha,
    ff.id_tienda,
    ff.id_transportadora,
    ff.modelo,
    ff.operador,
    ff.dotacion,
    ff.duplicado
  FROM forecast_filtrado ff
  LEFT JOIN forecast_and_planning.estimacion_costo_armado est
    ON est.fecha_entrega = ff.fecha
   AND est.id_tienda = ff.id_tienda
   AND est.modelo_cobro = ff.modelo
   AND est.operador = ff.operador
  WHERE est.id_tienda IS NULL
    AND ff.dotacion > 0
),
forecast_sin_pedidos_unico AS (
  SELECT
    fecha,
    id_tienda,
    modelo,
    operador,
    MAX(duplicado) AS duplicado,
    STRING_AGG(DISTINCT id_transportadora::TEXT, ', ') AS id_transportadoras,
    MAX(id_transportadora)::TEXT AS id_transportadora_unica,
    MIN(dotacion) AS dotacion
  FROM forecast_sin_pedidos
  GROUP BY fecha, id_tienda, modelo, operador
),
sin_pedidos AS (
  SELECT
    sp.fecha AS fecha_entrega,
    NULL::TEXT AS tienda,
    sp.id_tienda,
    sp.modelo AS modelo_cobro,
    COALESCE(tp.tarifa_asegurado, 0)::BIGINT AS tarifa_asegurado,
    sp.operador,
    CASE WHEN sp.duplicado = 1 THEN NULL ELSE sp.id_transportadora_unica END AS id_transportadora,
    CASE WHEN sp.duplicado = 1 THEN sp.id_transportadoras ELSE NULL END AS id_transportadoras,
    0::NUMERIC AS costo_armado,
    sp.dotacion,
    sp.dotacion * COALESCE(tp.tarifa_asegurado, 0)::BIGINT * COALESCE(fa.factor_asegurado, 1)::NUMERIC AS minimo_asegurado,
    sp.dotacion * COALESCE(tp.tarifa_asegurado, 0)::BIGINT * COALESCE(fa.factor_asegurado, 1)::NUMERIC AS diferencia_asegurado,
    'sin_pedido' AS tipo_origen
  FROM forecast_sin_pedidos_unico sp
  LEFT JOIN forecast_and_planning.tarifas_prefacturas tp
    ON tp.id_transportadora = sp.id_transportadora_unica
   AND sp.fecha BETWEEN tp.fecha_inicio AND COALESCE(tp.fecha_termino, '9999-12-31')
  LEFT JOIN forecast_and_planning.factores_asegurado fa
    ON fa.operador = sp.operador
   AND fa.modelo_cobro = sp.modelo
   AND sp.fecha BETWEEN fa.fecha_inicio AND COALESCE(fa.fecha_fin, '9999-12-31')
)
SELECT * FROM duplicados
UNION ALL
SELECT * FROM no_duplicados
UNION ALL
SELECT * FROM sin_pedidos
ON CONFLICT (fecha_entrega, id_tienda, modelo_cobro, operador, tipo_origen) DO NOTHING;
