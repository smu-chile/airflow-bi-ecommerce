SET work_mem = '512MB';
SET maintenance_work_mem = '512MB';
SET max_parallel_workers_per_gather = 4;
SET geqo = off;

-- Asegurar índices base en ecommdata y staging para evitar escaneos secuenciales
CREATE INDEX IF NOT EXISTS idx_ecommdata_stock_fecha ON ecommdata.stock (fecha);
CREATE INDEX IF NOT EXISTS idx_skus_vtex_id ON ecommdata.skus (vtex_id);
CREATE INDEX IF NOT EXISTS idx_skus_ref_id ON ecommdata.skus (ref_id);
CREATE INDEX IF NOT EXISTS idx_productos_ref_id ON ecommdata.productos (ref_id);
CREATE INDEX IF NOT EXISTS idx_lista_infaltables_material ON ecommdata.lista_infaltables (material);
CREATE INDEX IF NOT EXISTS idx_staging_stock_unimarc ON staging.stock_unimarc (item_id, store_id, warehouse_id);
CREATE INDEX IF NOT EXISTS idx_staging_stock_vtex ON staging.stock_vtex_unimarc (vtex_id, id_warehouse);

ANALYZE staging.stock_vtex_unimarc;
ANALYZE staging.stock_unimarc;

-- Paso 1: Pre-filtrar combinaciones válidas de VTEX, Bodegas y Tiendas activas (Reduce drásticamente el espacio de búsqueda)
DROP TABLE IF EXISTS tmp_stock_base;
CREATE TEMP TABLE tmp_stock_base AS
SELECT 
    svu.vtex_id,
    svu.id_warehouse,
    svu.cantidad_total as stock_vtex,
    svu.cantidad_reservada as stock_reservado_vtex,
    (svu.cantidad_total - svu.cantidad_reservada) as stock_disponible_vtex,
    svu.cantidad_ilimitada as stock_infinito_vtex,
    b.id as id_bodega,
    b.nombre as nombre_bodega,
    b.id_janis as b_id_janis,
    t.id as id_tienda,
    t.glosa as glosa_tienda,
    t.id_janis as t_id_janis
FROM staging.stock_vtex_unimarc svu
JOIN ecommdata.bodegas b ON svu.id_warehouse = b.id AND b.dock_activo IS TRUE
JOIN ecommdata.tiendas t ON b.id_tienda = t.id AND t.status = 1
WHERE NOT (
    (t.id = '0018' AND b.id = '9051') OR
    (t.id = '0069' AND b.id = '0576') OR
    (t.id = '0088' AND b.id = '0324')
);

CREATE INDEX ON tmp_stock_base (vtex_id);
CREATE INDEX ON tmp_stock_base (id_tienda);
CREATE INDEX ON tmp_stock_base (t_id_janis, b_id_janis);
ANALYZE tmp_stock_base;

-- Paso 2: Calcular el dataset final en memoria temporal (fuera de la transacción de bloqueo)
DROP TABLE IF EXISTS tmp_stock_final_payload;
CREATE TEMP TABLE tmp_stock_final_payload AS
SELECT 
    '{{ds}}'::date as fecha,
    base.id_tienda,
    base.glosa_tienda,
    base.id_bodega,
    base.nombre_bodega,
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
    base.stock_vtex,
    base.stock_reservado_vtex,
    base.stock_disponible_vtex,
    base.stock_infinito_vtex,
    su.date_published as fecha_publicacion_janis,
    su.date_modified as fecha_modificacion_janis,
    '{{ts}}' at time zone 'America/Santiago' + interval '4 hours' as ultima_actualizacion,
    (l.material is not null and l.excluido is false and l.bloq_centro is null and l.bloq_formato is null and l.catalogado is true) as surtido_ecommerce,
    case when li.material is null then false else true end as infaltable
FROM tmp_stock_base base
LEFT JOIN ecommdata.skus s ON base.vtex_id = s.vtex_id
LEFT JOIN staging.stock_unimarc su 
       ON s.id = su.item_id 
      AND base.t_id_janis = su.store_id 
      AND base.b_id_janis = su.warehouse_id
LEFT JOIN ecommdata.productos p ON s.ref_id = p.ref_id
LEFT JOIN ecommdata.categorias c ON p.id_categoria = c.id
LEFT JOIN ecommdata.lista8 l 
       ON l.id_tienda = base.id_tienda 
      AND l.material = split_part(s.ref_id, '-', 1) 
      AND l.umv = split_part(s.ref_id, '-', 2)
LEFT JOIN ecommdata.lista_infaltables li ON p.material = li.material;

ANALYZE tmp_stock_final_payload;

-- Paso 3: Reemplazo atómico e instantáneo en la tabla destino
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
SELECT * FROM tmp_stock_final_payload;

COMMIT;

ANALYZE ecommdata.stock;
