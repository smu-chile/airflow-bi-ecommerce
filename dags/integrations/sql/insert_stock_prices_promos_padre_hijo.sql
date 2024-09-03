BEGIN TRANSACTION;
--TRADUCCION PRODUCTOS PADRE - HIJO
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
	select split_part(s2.ref_id, '-', 1) as material
		, s.ou_id as id_tienda 
		, s.nbr_itm as stock_unitario
		, s2.ean_primario as ean
		, p.cont_conv_umb as multiplicador_unidad
		, s2.nombre_sku as nombre
		, p.brand_desc as marca
		, case when p.unidad_de_medida = 'ST' then 'UN' else p.unidad_de_medida end as unidad_de_medida
	from integraciones.stock s 
	left join integraciones.productos p 
		on p.sku_key = s.sku_key
	join ecommdata.skus s2 
		on s2.erp_id = s.sku_product
		and s2.erp_id::int8 <> split_part(s2.ref_id, '-', 1)::int8 
	where p.ean is not null 
		and p.cont_conv_umb is not null 
		and p.nm is not null 
		and p.brand_desc is not null 
		and p.unidad_de_medida is not null
	) _t
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
	where p.fecha_carga = '{{ds}}'
) _t2
on _t.material = _t2.material 
and _t.unidad_de_medida = _t2.umv
and _t.id_tienda = _t2.id_tienda
left join (
	select ean, material
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
	and wp.nombre_promocion::text !~~ '%LIQ%'::text
	and wp.n_promocion not in  ('5640502024','5552392024','1120012024',
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
'4000512024','5552792024','5552852024')
    group by wp.ean , wp.material
) _t3
on _t.material = _t3.material
where floor(_t.stock_unitario/_t.multiplicador_unidad) > 0
;
COMMIT;