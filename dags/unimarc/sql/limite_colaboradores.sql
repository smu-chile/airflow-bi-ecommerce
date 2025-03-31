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
INNER JOIN ecommdata.calendario c ON oj.fecha_facturacion = c.fecha
WHERE (opp.nombre ILIKE '%Colaborador%' 
     OR opp.nombre ILIKE '%referido%')
     and opp.nombre not ilike '%despacho%'
     AND c.mes_relativo = 0
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