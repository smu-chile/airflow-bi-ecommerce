BEGIN TRANSACTION;

-- 1. Crear tabla temporal con el catálogo activo y su stock agrupado por ref_id (para evitar duplicados)
CREATE TEMP TABLE temp_active_catalog ON COMMIT DROP AS
SELECT 
    l.material,
    l.umv,
    CONCAT(l.material, '-', l.umv) AS ref_id,
    MAX(COALESCE(su.stock, 0)) AS stock,
    BOOL_OR(COALESCE(su.not_found, FALSE)) AS not_found
FROM ecommdata.lista8 l
LEFT JOIN staging.stock_unimarc_api su ON CONCAT(l.material, '-', l.umv) = su.ref_id
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
    COALESCE(cat.not_found, FALSE) AS src_not_found,
    v2.activo AS target_activo,
    v2.stock AS target_stock,
    COALESCE(v2.not_found, FALSE) AS target_not_found
FROM temp_active_catalog cat
FULL OUTER JOIN ecommdata.stock_trapenses_v2 v2 ON cat.ref_id = v2.ref_id
WHERE 
    -- Caso A: Producto nuevo (no está en v2)
    v2.stock IS NULL
    -- Caso B: Producto activo en ambos, pero con stock diferente o cambio en estado not_found
    OR (cat.ref_id IS NOT NULL AND v2.activo = TRUE AND (cat.stock IS DISTINCT FROM v2.stock OR cat.not_found IS DISTINCT FROM v2.not_found))
    -- Caso C: Producto desactivado (era activo en v2, ya no está en catálogo)
    OR (cat.ref_id IS NULL AND COALESCE(v2.activo, TRUE) = TRUE)
    -- Caso D: Producto reactivado (estaba inactivo en v2, vuelve a catálogo)
    OR (cat.ref_id IS NOT NULL AND v2.activo = FALSE);

-- 3. Registrar cantidad de cambios detectados en la tabla de logs
INSERT INTO ecommdata.stock_trapenses_v2_changes (fecha_hora, cantidad_cambios, total_skus_api, estado)
SELECT 
    CURRENT_TIMESTAMP,
    COUNT(*),
    {{ ti.xcom_pull(task_ids='fetch_janis_api_stock') }} AS total_skus_api,
    'EXITO' AS estado
FROM temp_detected_changes;

-- 4. Actualizar la tabla de imagen v2 (marcando publicado_janis y publicado_vtex en FALSE al haber cambios)
INSERT INTO ecommdata.stock_trapenses_v2 (
    material, 
    umv, 
    ref_id, 
    stock, 
    activo, 
    not_found,
    publicado_janis, 
    publicado_vtex, 
    intentos_janis, 
    intentos_vtex, 
    fecha_actualizacion
)
SELECT 
    dc.material,
    dc.umv,
    dc.ref_id,
    dc.src_stock,
    dc.src_activo,
    dc.src_not_found,
    FALSE AS publicado_janis,
    FALSE AS publicado_vtex,
    0 AS intentos_janis,
    0 AS intentos_vtex,
    CURRENT_TIMESTAMP AS fecha_actualizacion
FROM temp_detected_changes dc
ON CONFLICT (ref_id) DO UPDATE SET 
    stock = EXCLUDED.stock,
    activo = EXCLUDED.activo,
    not_found = EXCLUDED.not_found,
    publicado_janis = EXCLUDED.publicado_janis,
    publicado_vtex = EXCLUDED.publicado_vtex,
    intentos_janis = EXCLUDED.intentos_janis,
    intentos_vtex = EXCLUDED.intentos_vtex,
    fecha_actualizacion = EXCLUDED.fecha_actualizacion;

COMMIT;
