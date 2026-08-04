TRUNCATE TABLE ecommdata_alvi.catalogo_activo_alvi;

WITH productos_activos_lista8 AS (
    SELECT DISTINCT 
        (material::text || '-' || umv::text) AS ref_id
    FROM ecommdata_alvi.lista8
)
INSERT INTO ecommdata_alvi.catalogo_activo_alvi (
    vtex_id, ref_id, nombre, nombre_categoria, categoria_valida
)
SELECT 
    p.vtex_id::VARCHAR,
    l.ref_id,
    p.nombre,
    c.n1 AS nombre_categoria,
    CASE 
        WHEN c.n1 IN ('No trabajar', 'No Trabajar', 'Inactivos', 'Integración') THEN FALSE 
        WHEN c.n1 IS NULL THEN FALSE
        ELSE TRUE 
    END AS categoria_valida
FROM productos_activos_lista8 l
INNER JOIN ecommdata_alvi.productos p 
    ON l.ref_id = p.ref_id
LEFT JOIN ecommdata_alvi.categorias c 
    ON p.id_categoria = c.id
WHERE p.vtex_id IS NOT NULL;
