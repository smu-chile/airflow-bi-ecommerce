SELECT 
    dt.id_tarea,
    dt.nombre_tarea,
    dt.hora_inicio,
    dt.hora_termino,
    dt.duracion,
    dt.prioridad,
    CASE 
        WHEN p.id_tarea IS NOT NULL THEN p.personas
        ELSE dt.min_operadores
    END AS min_operadores,
    CASE 
        WHEN p.id_tarea IS NOT NULL  and dt.id_tarea <> 4 THEN p.personas + 2
        WHEN p.id_tarea IS NOT NULL  and dt.id_tarea = 4 THEN 2
        ELSE dt.max_operadores
    END AS max_operadores
FROM 
    ecommdata.dotacion_tareas dt
LEFT JOIN (
    SELECT 
        dp.id_tarea,
        MAX(
            CASE 
                WHEN dp.id_tarea = 4 THEN LEAST(CEILING(ROUND(f.ordenes * dp.porcentaje * do2.porcentaje_ordenes * vd.unidades_promedio) / dp.productividad), 2)
                WHEN dp.id_tarea = 6 THEN 
                    CASE 
                        WHEN CEILING(ROUND(f.ordenes * dp.porcentaje * do2.porcentaje_ordenes * vd.unidades_promedio) / dp.productividad) - 2 < 1 THEN 1
                        ELSE CEILING(ROUND(f.ordenes * dp.porcentaje * do2.porcentaje_ordenes * vd.unidades_promedio) / dp.productividad)
                    END
                ELSE CEILING(ROUND(f.ordenes * dp.porcentaje * do2.porcentaje_ordenes * vd.unidades_promedio) / dp.productividad)
            END
        ) as personas
    FROM 
        ecommdata.dotacion_productividad dp
    LEFT JOIN 
        forecast_and_planning.forecast f ON f.id_tienda = '1917' AND f.fecha = '{ds}'::date
    LEFT JOIN 
        ecommdata.dotacion_olas do2 ON do2.id_tienda = f.id_tienda::int
    CROSS JOIN (
        SELECT 
            ROUND(SUM(vdw.venta_umv) / COUNT(DISTINCT vdw.id_orden)) AS unidades_promedio
        FROM 
            ecommdata.ventas_ecommerce_datawarehouse vdw
        WHERE 
            vdw.canal_venta LIKE 'E%'
            AND vdw.id_tienda = '1917'
            AND vdw.fecha_facturacion >= '{ds}'::date -30
    ) AS vd
    GROUP BY 
        dp.id_tarea
) as p ON p.id_tarea = dt.id_tarea;