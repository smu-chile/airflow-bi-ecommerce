SELECT
    ranking,
    ref_id_sku,
    nombre_sku,
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
            ved.ref_id_sku
    ),
    RankedData AS (
        SELECT
            ref_id_sku,
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
        ROW_NUMBER() OVER (ORDER BY (0.5 * recurrencia_boleta_rank + 0.3 * unidades_rank + 0.2 * plata_rank)) AS ranking,
        r.ref_id_sku,
        s.nombre_sku,
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
        left join ecommdata.productos p on p.ref_id  = r.ref_id_sku
        left join ecommdata.categorias c on p.id_categoria = c.id
        left join ecommdata.marcas m on m.id = p.id_marca 
    ORDER BY
        ranking
) AS Subquery;