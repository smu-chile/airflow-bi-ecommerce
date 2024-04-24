select dm.fecha, dm.bloque, dm.jornada,dm.entrada,dm.salida, count(rut) as numero_operadores
from ecommdata.dotacion_mfc dm
where dm.bloque in ('M','T','N','S')
and fecha = '{ds}'::date
group by dm.fecha, dm.bloque, dm.jornada, dm.entrada,dm.salida