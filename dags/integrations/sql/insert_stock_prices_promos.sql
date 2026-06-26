BEGIN TRANSACTION;
--PRODUCTOS REGULARES
insert into integraciones.lm_stock_precio_promo 
select _t.id_tienda,
	_t.ean,
	_t.material,
	_t.unidad_de_medida,
	_t.multiplicador_unidad,
	_t.nombre,
	_t.marca,
	_t.stock_unitario,
	_t2.precio,
	_t3.precio_promocional
from (
	select s.sku_product as material 
		, s.ou_id as id_tienda 
		, s.nbr_itm as stock_unitario
		, p.ean as ean 
		, p.cont_conv_umb as multiplicador_unidad
		, p.nm as nombre
		, p.brand_desc as marca
		, case when p.unidad_de_medida = 'ST' then 'UN' else p.unidad_de_medida end as unidad_de_medida   
	from integraciones.stock s 
	left join integraciones.productos p 
		on p.sku_key = s.sku_key 
	left join ecommdata.tiendas t 
		on s.ou_id = t.id  
	where p.ean is not null 
	and p.cont_conv_umb is not null 
	and p.nm is not null 
	and p.brand_desc is not null 
	and p.unidad_de_medida is not null) _t
join (
	select t.id as id_tienda
		, p.ref_id
		, split_part(p.ref_id, '-', 1) as material 
		, split_part(p.ref_id, '-', 2) as umv
		, p.precio
	from ecommdata.precios p 
	join ecommdata.tiendas t 
		on p.id_tienda_janis = t.id_janis 
		and t.status = 1
	join ecommdata.lista8 l 
		on l.material || '-' || l.umv = p.ref_id 
		and l.id_tienda = t.id 
	left join ecommdata.productos ep on ep.ref_id = p.ref_id
	left join ecommdata.categorias ec on ep.id_categoria = ec.id
	where p.fecha_carga = '{{ds}}'
	and (ec.n1 NOT IN ('No Trabajar', 'Inactivos') OR ec.n1 IS NULL)
) _t2
on _t.material = _t2.material 
and _t.unidad_de_medida = _t2.umv
and _t.id_tienda = _t2.id_tienda
left join (
	select ean
            , min(precio_promocional) AS precio_promocional 
    from ecommdata.workflow_promociones wp 
    where wp.fecha_inicio_de_promocion <= '{{macros.ds_add(ds,1)}}'
    and wp.fecha_fin_de_promocion >= '{{macros.ds_add(ds,1)}}'
    and wp.tipo_promocion IN (1,4)
    and wp.registro_valido = True
    and wp.organizacion_ventas = '1000'
    and wp.canal_distribucion = '10'
   	and wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123,124)
	and wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
	AND wp.nombre_promocion::text !~~ '%ZONA%'::text
	AND wp.nombre_promocion::text !~~ '%MFC%'::text
    AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
    AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
	AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
	AND wp.nombre_promocion::text !~~ '%917%'::text
	AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
	and wp.nombre_promocion::text !~~ '% LOC%'::text
	and wp.nombre_promocion::text !~~ '%CYBER%'::text
	and wp.nombre_promocion::text !~~ '%LIQ%'::text
	AND wp.nombre_promocion NOT ILIKE '%REGIO%'
	and wp.n_promocion  not in  ('5720882025','5640502024','5552392024','1120012024',
'1120022024',
'1120032024',
'1120042024',
'1120052024',
'1120062024',
'1120082024',
'1120092024',
'1120102024',
'1120112024',
'1120122024',
'4000512024','5552792024','5552852024','4000662024','4000942024','4000962024','4000972024','1120012025','1120022025','1120032025','1120042025',
'5770232025','1120162025','1120062025','1120092025','1120212025','5551272026')
    group by wp.ean
) _t3
on _t.ean = _t3.ean
where floor(_t.stock_unitario/_t.multiplicador_unidad) > 0
;
COMMIT