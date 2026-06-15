-------Query con nombre e imagen------
WITH RankedCatalog AS (
    SELECT 
        s.ean_primario AS "EAN",
        l.material AS "SKU",
        s.ref_id,
        ec.n1 AS "SECCION",
        p.nombre AS "NOMBRE",
        CASE 
            WHEN t.imagen IS NOT NULL AND t.imagen <> '' THEN CONCAT('https://unimarc.vteximg.com.br', t.imagen)
            ELSE NULL 
        END AS "IMAGEN",
        -- Se aplica ROW_NUMBER temprano para evitar multiplicar filas antes de agrupar y hacer join con precios
        ROW_NUMBER() OVER (PARTITION BY l.material ORDER BY s.ref_id ASC) as rn
    FROM ecommdata.lista8 l
    INNER JOIN ecommdata.skus s 
        ON l.material || '-' || l.umv = s.ref_id
    INNER JOIN ecommdata.productos p 
        ON s.ref_id = p.ref_id
    LEFT JOIN ecommdata.categorias ec
        ON p.id_categoria = ec.id
    LEFT JOIN ecommdata.imagenes_sku t 
        ON s.ref_id = t.ref_id AND t.orden = 1
    WHERE (ec.n1 NOT IN ('No Trabajar', 'Inactivos','Integración') OR ec.n1 IS NULL)
      --AND (ec.status = 'activo' OR ec.status IS NULL)
      AND l.excluido IS NOT TRUE
)
SELECT 
    rc."EAN",
    rc."SKU",
    MAX(pr.precio) AS "Precio", -- Buscamos el precio más alto asociado al SKU/EAN dentro de las tablas
    rc."SECCION",
    RC."NOMBRE" ,
    RC."IMAGEN" 
FROM RankedCatalog rc
-- Hacemos el JOIN con precios DESPUÉS de haber filtrado el catálogo base (rn = 1)
-- Esto reduce exponencialmente el cruce de datos, agilizando mucho la consulta.
INNER JOIN ecommdata.precios pr
    ON rc.ref_id = pr.ref_id
    -- Exceptuamos explícitamente las tiendas 0001 y 0006 (o 1 y 6) para el cálculo del precio máximo
    AND pr.id_tienda_janis NOT IN (
        SELECT id_janis 
        FROM ecommdata.tiendas 
        WHERE id_janis IN ('0006', '6')
    )
WHERE rc.rn = 1
GROUP BY 
    rc."EAN", 
    rc."SKU", 
    rc."SECCION",
    RC."NOMBRE" ,
    rc."IMAGEN" 
HAVING 
    MAX(pr.precio) IS NOT null and rc."SECCION" is not NULL
ORDER BY 
    rc."SKU" ASC;
