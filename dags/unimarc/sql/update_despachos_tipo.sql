UPDATE ecommdata.despachos as d
SET tipo_despacho = CASE 
						WHEN subquery.tipo_despacho = 1 THEN 'delivery' 
						WHEN subquery.tipo_despacho = 2 THEN 'pickup'
						else 'error'
					END
FROM ecommdata.transportadoras as subquery
WHERE d.id_transportadora = subquery.id
AND d.tipo_despacho IS NULL;
