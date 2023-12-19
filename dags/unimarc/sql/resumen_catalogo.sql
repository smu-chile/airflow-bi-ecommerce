insert into catalogo.resumen_catalogo
select fecha_hora::date as fecha, id_tienda, sum(total_surtido) as total_surtido, sum(publicacion_valida) as publicacion_valida, sum(disponible_web) as disponible_web
from catalogo.publicacion_dia_tienda_surtido pdts
where fecha_hora::time = '12:00:00' and fecha_hora::date = '{{ds}}'
group by fecha_hora, id_tienda
on conflict (fecha, id_tienda) do update
set fecha = EXCLUDED.fecha
	, id_tienda = EXCLUDED.id_tienda
	, total_surtido = EXCLUDED.total_surtido
	, publicacion_valida = EXCLUDED.publicacion_valida
	, disponible_web = EXCLUDED.disponible_web