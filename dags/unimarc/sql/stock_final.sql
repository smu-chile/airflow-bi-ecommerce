-- NOTA: Este archivo ha sido reemplazado en el DAG etl_stock_incremental_load por:
-- 1. sql/prepare_surtido_activo.sql (Fase 1: staging de surtido activo)
-- 2. _save_stock_final_batched en etl_stock_incremental_load.py (Fase 2: carga tienda por tienda con throttling)
--
-- Se mantiene esta version limpia de emergencia/manual (sin bloqueos de CREATE INDEX ni geqo=off):

SET work_mem = '64MB';
SET max_parallel_workers_per_gather = 2;

BEGIN TRANSACTION;

DELETE FROM ecommdata.stock
WHERE fecha = '{{ds}}'::date;

INSERT INTO ecommdata.stock (
    fecha,
    id_tienda,
    glosa_tienda,
    id_bodega,
    nombre_bodega,
    ref_id,
    material,
    descripcion,
    c1,
    c2,
    c3,
    multiplicador_unidad_medida,
    unidades_pack,
    stock_janis,
    stock_seguridad_janis,
    stock_infinito_janis,
    tipo_operacion_janis,
    stock_vtex,
    stock_reservado_vtex,
    stock_disponible_vtex,
    stock_infinito_vtex,
    fecha_publicacion_janis,
    fecha_modificacion_janis,
    ultima_actualizacion,
    surtido_ecommerce,
    infaltable
)
SELECT 
    '{{ds}}'::date as fecha,
    t.id as id_tienda,
    t.glosa as glosa_tienda,
    b.id as id_bodega,
    b.nombre as nombre_bodega,
    s.ref_id,
    p.material,
    s.nombre_sku as descripcion,
    c.n1 as c1,
    c.n2 as c2,
    c.n3 as c3,
    s.multiplicador_unidad_medida,
    s.unidades_pack,
    su.stock as stock_janis,
    su.min_stock as stock_seguridad_janis,
    su.infinite_stock::int::bool as stock_infinito_janis,
    su.operation_type as tipo_operacion_janis,
    svu.cantidad_total as stock_vtex,
    svu.cantidad_reservada as stock_reservado_vtex,
    (svu.cantidad_total - svu.cantidad_reservada) as stock_disponible_vtex,
    svu.cantidad_ilimitada as stock_infinito_vtex,
    su.date_published as fecha_publicacion_janis,
    su.date_modified as fecha_modificacion_janis,
    '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' as ultima_actualizacion,
    (sa.ref_id IS NOT NULL) as surtido_ecommerce,
    (li.material IS NOT NULL) as infaltable
FROM staging.stock_vtex_unimarc svu
JOIN ecommdata.bodegas b 
  ON svu.id_warehouse = b.id 
 AND b.dock_activo IS TRUE
JOIN ecommdata.tiendas t 
  ON b.id_tienda = t.id 
 AND t.status = 1
LEFT JOIN ecommdata.skus s 
  ON svu.vtex_id = s.vtex_id
LEFT JOIN staging.stock_unimarc su 
  ON s.id = su.item_id 
 AND t.id_janis = su.store_id 
 AND b.id_janis = su.warehouse_id
LEFT JOIN ecommdata.productos p 
  ON s.ref_id = p.ref_id
LEFT JOIN ecommdata.categorias c 
  ON p.id_categoria = c.id
LEFT JOIN staging.surtido_activo_unimarc sa 
  ON sa.id_tienda = t.id 
 AND sa.ref_id = s.ref_id
LEFT JOIN ecommdata.lista_infaltables li 
  ON p.material = li.material
WHERE NOT (
    (t.id = '0018' AND b.id = '9051') OR
    (t.id = '0069' AND b.id = '0576') OR
    (t.id = '0088' AND b.id = '0324')
);

COMMIT;
