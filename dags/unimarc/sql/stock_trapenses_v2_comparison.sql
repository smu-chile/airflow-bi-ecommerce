BEGIN TRANSACTION;

-- 1. Crear tabla temporal con el catálogo activo y su stock agrupado por ref_id (para evitar duplicados)
CREATE TEMP TABLE temp_active_catalog ON COMMIT DROP AS
SELECT 
    l.material,
    l.umv,
    CONCAT(l.material, '-', l.umv) AS ref_id,
    MAX(COALESCE(su.stock, 0)) AS stock
FROM ecommdata.lista8 l
LEFT JOIN ecommdata.skus s ON CONCAT(l.material, '-', l.umv) = s.ref_id
LEFT JOIN staging.stock_unimarc su ON s.id = su.item_id AND su.warehouse_id = 3968
WHERE l.id_tienda = '0917'
  AND l.excluido IS FALSE
  AND l.bloq_centro IS NULL
  AND l.bloq_formato IS NULL
  AND l.catalogado IS TRUE
GROUP BY l.material, l.umv;

-- 2. Crear tabla temporal con los cambios detectados
CREATE TEMP TABLE temp_detected_changes ON COMMIT DROP AS
SELECT 
    COALESCE(cat.ref_id, v2.ref_id) AS ref_id,
    COALESCE(cat.material, v2.material) AS material,
    COALESCE(cat.umv, v2.umv) AS umv,
    (cat.ref_id IS NOT NULL) AS src_activo,
    COALESCE(cat.stock, 0) AS src_stock,
    v2.activo AS target_activo,
    v2.stock AS target_stock
FROM temp_active_catalog cat
FULL OUTER JOIN ecommdata.stock_trapenses_v2 v2 ON cat.ref_id = v2.ref_id
WHERE 
    -- Caso A: Producto nuevo (no está en v2)
    v2.stock IS NULL
    -- Caso B: Producto activo en ambos, pero con stock diferente
    OR (cat.ref_id IS NOT NULL AND v2.activo = TRUE AND cat.stock IS DISTINCT FROM v2.stock)
    -- Caso C: Producto desactivado (era activo en v2, ya no está en catálogo)
    OR (cat.ref_id IS NULL AND COALESCE(v2.activo, TRUE) = TRUE)
    -- Caso D: Producto reactivado (estaba inactivo en v2, vuelve a catálogo)
    OR (cat.ref_id IS NOT NULL AND v2.activo = FALSE);

-- 3. Registrar cantidad de cambios detectados en la tabla de logs
INSERT INTO ecommdata.stock_trapenses_v2_changes (fecha_hora, cantidad_cambios)
SELECT 
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Santiago',
    COUNT(*)
FROM temp_detected_changes;

-- 4. Actualizar la tabla de imagen v2 (dejando publicado = false para procesar por integración)
INSERT INTO ecommdata.stock_trapenses_v2 (material, umv, ref_id, stock, activo, publicado, fecha_actualizacion)
SELECT 
    material,
    umv,
    ref_id,
    src_stock,
    src_activo,
    FALSE AS publicado,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Santiago' AS fecha_actualizacion
FROM temp_detected_changes
ON CONFLICT (ref_id) DO UPDATE SET 
    stock = EXCLUDED.stock,
    activo = EXCLUDED.activo,
    publicado = EXCLUDED.publicado,
    fecha_actualizacion = EXCLUDED.fecha_actualizacion;

COMMIT;
