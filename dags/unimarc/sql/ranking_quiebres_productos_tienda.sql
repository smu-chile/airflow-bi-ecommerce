WITH BreakData AS (
    SELECT 
        op.ref_id, 
        t.id AS id_tienda, 
        SUM(op.unidades_solicitadas - op.unidades_pickeadas) AS unidades_quebradas, 
        COUNT(DISTINCT oj.id) AS ordenes_afectadas
    FROM 
        ecommdata.ordenes_janis oj
    LEFT JOIN 
        ecommdata.orden_productos op ON oj.id = op.id_orden
    LEFT JOIN 
        ecommdata.tiendas t ON t.id_janis = oj.id_tienda_janis
    WHERE 
        oj.fecha_facturacion IS NOT NULL
        AND op.unidades_solicitadas > op.unidades_pickeadas
        AND t.status = 1
        AND oj.fecha_picking::date >= '{{ds}}'::date - 30
    GROUP BY 
        op.ref_id, t.id
),
RankedData AS (
    SELECT
        ref_id,
        id_tienda,
        unidades_quebradas,
        ordenes_afectadas,
        DENSE_RANK() OVER (ORDER BY unidades_quebradas DESC) AS unidades_quebradas_rank,
        DENSE_RANK() OVER (ORDER BY ordenes_afectadas DESC) AS ordenes_afectadas_rank
    FROM
        BreakData
)
SELECT
    r.id_tienda,
    ROW_NUMBER() OVER (PARTITION BY r.id_tienda ORDER BY (0.5 * unidades_quebradas_rank + 0.5 * ordenes_afectadas_rank)) AS ranking,
    r.ref_id AS ref_id_sku,
    s.nombre_sku,
    c.n1 AS nivel_categoria_1,
    c.n2 AS nivel_categoria_2,
    c.n3 AS nivel_categoria_3,
    m.nombre AS nombre_marca,
    r.unidades_quebradas,
    r.ordenes_afectadas
FROM
    RankedData r
    LEFT JOIN ecommdata.skus s ON s.ref_id = r.ref_id
    LEFT JOIN ecommdata.productos p ON p.ref_id = r.ref_id
    LEFT JOIN ecommdata.categorias c ON p.id_categoria = c.id
    LEFT JOIN ecommdata.marcas m ON m.id = p.id_marca
ORDER BY
    ranking, id_tienda;