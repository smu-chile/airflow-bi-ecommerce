SELECT du.user_profile_id, du.email, du.nombre, du.apellido,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%Colaborador%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_colaborador,
    SUM(CASE 
        WHEN opp.nombre ILIKE '%referido%' THEN opp.valor
        ELSE 0 
    END)::int AS descuento_referido
FROM ecommdata.ordenes_janis oj
LEFT JOIN ecommdata.orden_productos op ON oj.id = op.id_orden
LEFT JOIN ecommdata.orden_producto_promociones opp ON opp.orden_producto = op.id
INNER JOIN analytics_and_growth.perfil_usuario pu ON pu.id_cliente_janis = oj.id_cliente_janis
INNER JOIN analytics_and_growth.detalle_usuario du ON pu.user_profile_id = du.user_profile_id
WHERE (opp.nombre ILIKE '%Colaborador%' 
     OR opp.nombre ILIKE '%referido%')
     AND opp.nombre NOT ILIKE '%despacho%'
     -- Filtro directo por mes actual: no depende de ecommdata.calendario
     -- (el calendario puede no estar actualizado a las 00:45 cuando este ETL corre)
     -- Usamos NOW() AT TIME ZONE para obtener la fecha real en Chile, independiente del timezone del servidor
     AND DATE_TRUNC('month', oj.fecha_facturacion) = DATE_TRUNC('month', NOW() AT TIME ZONE 'America/Santiago')
GROUP BY du.user_profile_id, du.email, du.nombre, du.apellido
HAVING (ABS(SUM(CASE 
        WHEN opp.nombre ILIKE '%Colaborador%' THEN opp.valor
        ELSE 0 
    END)) > 75000 
    OR ABS(SUM(CASE 
        WHEN opp.nombre ILIKE '%referido%' THEN opp.valor
        ELSE 0 
    END)) > 75000)
ORDER BY descuento_colaborador, descuento_referido ASC;