-- Fase 1: Asegurar indices en tablas de staging y aislar el surtido activo de lista8

CREATE INDEX IF NOT EXISTS idx_staging_stock_unimarc ON staging.stock_unimarc (item_id, store_id, warehouse_id);
CREATE INDEX IF NOT EXISTS idx_staging_stock_vtex ON staging.stock_vtex_unimarc (vtex_id, id_warehouse);

ANALYZE staging.stock_vtex_unimarc;
ANALYZE staging.stock_unimarc;

-- Crear tabla staging liviana con Primary Key para busqueda instantanea
CREATE TABLE IF NOT EXISTS staging.surtido_activo_unimarc (
    id_tienda VARCHAR(10),
    ref_id VARCHAR(50),
    PRIMARY KEY (id_tienda, ref_id)
);

TRUNCATE staging.surtido_activo_unimarc;

-- Extraer unicamente el catalogo e-commerce activo para tiendas activas
INSERT INTO staging.surtido_activo_unimarc (id_tienda, ref_id)
SELECT DISTINCT 
    l.id_tienda,
    CONCAT(l.material, '-', l.umv) AS ref_id
FROM ecommdata.lista8 l
JOIN ecommdata.tiendas t ON l.id_tienda = t.id AND t.status = 1
WHERE l.material IS NOT NULL
  AND l.umv IS NOT NULL
  AND l.excluido IS FALSE 
  AND l.bloq_centro IS NULL 
  AND l.bloq_formato IS NULL 
  AND l.catalogado IS TRUE
ON CONFLICT DO NOTHING;

ANALYZE staging.surtido_activo_unimarc;
