select dm.fecha,
	dm.bloque,
	count(dm.rut) as numero_operadores,
	dm.entrada,
	dm.salida
from ecommdata.dotacion_mfc dm
where dm.fecha = '{ds}'::date +1
group by dm.bloque,dm.fecha,dm.entrada,dm.salida