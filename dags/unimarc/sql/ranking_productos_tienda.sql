SELECT
    id_tienda,
    ranking,
    ref_id_sku,
    nombre_sku,
    stock_dia,
    nivel_categoria_1,
    nivel_categoria_2,
    nivel_categoria_3,
    nombre_marca,
    recurrencia_boleta,
    venta_unidades,
    venta_pesos
FROM (
    WITH SalesData AS (
        SELECT
            ved.ref_id_sku,
            ved.id_tienda,
            COUNT(ved.ref_id_sku) AS recurrencia_boleta,
            SUM(venta_umv / s.multiplicador_unidad_medida) AS venta_unidades,
            SUM(venta_neta) AS venta_pesos
        FROM
            ecommdata.ventas_ecommerce_datawarehouse ved
            LEFT JOIN ecommdata.skus s ON s.ref_id = ved.ref_id_sku
        WHERE
            fecha_facturacion >= '{ds}'::date - 30
            AND ved.ref_id_sku <> '000000000000630792-UN'
            and ved.canal_venta = 'E-COMMERCE'
        GROUP BY
            ved.ref_id_sku, ved.id_tienda
    ),
    RankedData AS (
        SELECT
            ref_id_sku,
            id_tienda,
            recurrencia_boleta,
            venta_unidades,
            venta_pesos,
            DENSE_RANK() OVER (ORDER BY recurrencia_boleta DESC) AS recurrencia_boleta_rank,
            DENSE_RANK() OVER (ORDER BY venta_unidades DESC) AS unidades_rank,
            DENSE_RANK() OVER (ORDER BY venta_pesos DESC) AS plata_rank
        FROM
            SalesData
    )
    SELECT
        r.id_tienda,
        ROW_NUMBER() OVER (PARTITION BY r.id_tienda ORDER BY (0.5 * recurrencia_boleta_rank + 0.3 * unidades_rank + 0.2 * plata_rank)) AS ranking,
        r.ref_id_sku,
        s.nombre_sku,
        CASE
            WHEN s2.stock_janis IS NULL THEN 0
            ELSE s2.stock_janis
        END as stock_dia,
        c.n1 as nivel_categoria_1,
        c.n2 as nivel_categoria_2,
        c.n3 as nivel_categoria_3,
        m.nombre as nombre_marca,
        r.recurrencia_boleta,
        ROUND(r.venta_unidades::numeric) AS venta_unidades,
        r.venta_pesos
    FROM
        RankedData r
        LEFT JOIN ecommdata.skus s ON s.ref_id = r.ref_id_sku
        LEFT JOIN ecommdata.stock s2 ON r.id_tienda = s2.id_tienda AND r.ref_id_sku = s2.ref_id
        left join ecommdata.productos p on p.ref_id  = r.ref_id_sku
        left join ecommdata.categorias c on p.id_categoria = c.id
        left join ecommdata.marcas m on m.id = p.id_marca 
    WHERE
        s2.fecha = '{ds}'::date
        and s2.surtido_ecommerce = true
    ORDER BY
        ranking, id_tienda
) AS Subquery;