SELECT 
    dm.rut,
    nombre_operador,
    CONCAT(SPLIT_PART(dm.entrada, ':', 1), ':00') AS entrada,
    CONCAT(SPLIT_PART(dm.salida, ':', 1), ':00') AS salida,
    dro.id_tarea_principal,
    dro.id_tarea_secundaria
FROM
    ecommdata.dotacion_operadores do2
LEFT JOIN 
    ecommdata.dotacion_mfc dm ON do2.rut = dm.rut
left join
	ecommdata.dotacion_ranking_operador dro on dro.rut = do2.rut
WHERE 
    fecha = '{ds}'::date
    AND bloque IN ('N', 'T', 'M', 'S')
ORDER BY
    entrada;