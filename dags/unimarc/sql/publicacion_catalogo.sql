BEGIN TRANSACTION;

ALTER TABLE ecommdata.publicacion_catalogo ADD COLUMN IF NOT EXISTS instock smallint;

DELETE FROM ecommdata.publicacion_catalogo WHERE fecha_hora = '{{ts}}' at time zone 'America/Santiago' + interval '4 hours';

INSERT INTO ecommdata.publicacion_catalogo (
	fecha_hora,
	material,
	ref_id,
	descripcion,
	c1,
	c2,
	c3,
	id_tienda,
	id_bodega,
	marca,
	foto_valida,
	cantidad_foto,
	foto_en_preparacion,
	categoria_valida,
	stock_valido,
	precio_valido,
	tienda_valida,
	publicacion_valida,
	disponible_web,
	infaltable,
	stock_janis,
	stock_seguridad_janis,
	stock_infinito_janis,
	stock_vtex,
	stock_reservado_vtex,
	stock_infinito_vtex,
	surtido_ecommerce,
	top_300,
	mfc,
	instock
)
SELECT s.ultima_actualizacion AS fecha_hora
, s.material
, s.ref_id
, s.descripcion 
, s.c1
, s.c2
, s.c3
, s.id_tienda 
, s.id_bodega
, m.nombre AS marca
, CASE WHEN foto.ref_id IS NOT NULL THEN TRUE ELSE FALSE END AS foto_valida
, foto.q_foto AS cantidad_foto
, foto.foto_en_preparacion
, CASE WHEN c.status = 'activo' THEN TRUE ELSE FALSE END AS categoria_valida
, CASE WHEN (s.stock_disponible_vtex > 0) OR (s.stock_infinito_vtex IS TRUE) THEN TRUE ELSE FALSE END AS stock_valido
, CASE WHEN pr.id IS NOT NULL THEN TRUE ELSE FALSE END AS precio_valido
, CASE WHEN pt.ref_id IS NOT NULL THEN TRUE ELSE FALSE END AS tienda_valida
, CASE
	WHEN foto.ref_id IS NOT NULL AND c.status = 'activo' AND pt.ref_id IS NOT NULL THEN TRUE 
	ELSE FALSE
END AS publicacion_valida
, CASE
	WHEN foto.ref_id IS NOT NULL AND c.status = 'activo' AND ((s.stock_disponible_vtex > 0) OR (s.stock_infinito_vtex IS TRUE)) AND pt.ref_id IS NOT NULL THEN TRUE 
	ELSE FALSE
END AS disponible_web
, CASE
	WHEN li.material IS NOT NULL THEN TRUE
	ELSE FALSE
END AS infaltable
, s.stock_janis
, s.stock_seguridad_janis
, s.stock_infinito_janis
, s.stock_vtex
, s.stock_reservado_vtex
, s.stock_infinito_vtex
, s.surtido_ecommerce
, CASE
	WHEN tp.material IS NOT NULL THEN TRUE
	ELSE FALSE
END AS top_300
, CASE
	WHEN (smt.ref_id IS NOT NULL AND smt.quantity_on_hand > 0 AND um.mfc_is_item_side = 'REG') THEN TRUE
	ELSE FALSE
END AS mfc
, ins.instock AS instock
FROM ecommdata.stock s
LEFT JOIN (
	SELECT isku.ref_id,
		   count(1) AS q_foto,
		   bool_or(isku.imagen ILIKE ANY(ARRAY['%foto-en%','%foto-unimarc%'])) AS foto_en_preparacion
	FROM ecommdata.imagenes_sku isku
	GROUP BY isku.ref_id
) foto ON s.ref_id = foto.ref_id
LEFT JOIN ecommdata.productos p ON s.ref_id = p.ref_id
LEFT JOIN ecommdata.categorias c ON p.id_categoria = c.id
LEFT JOIN ecommdata.tiendas t ON s.id_tienda = t.id
LEFT JOIN ecommdata.precios pr ON t.id_janis = pr.id_tienda_janis AND s.ref_id = pr.ref_id
LEFT JOIN ecommdata.productos_tienda pt ON s.ref_id = pt.ref_id AND s.id_tienda = pt.id_tienda
LEFT JOIN ecommdata.marcas m ON p.id_marca = m.id
LEFT JOIN ecommdata.lista_infaltables li ON s.material = li.material
LEFT JOIN ecommdata.top300 tp ON s.material = tp.material
LEFT JOIN (
	SELECT tom_id AS ref_id, quantity_on_hand, '1917' AS id_tienda
	FROM ecommdata.stock_mfc_takeoff
	WHERE fecha = (SELECT max(fecha) FROM ecommdata.stock_mfc_takeoff smt)
) smt ON smt.ref_id = s.ref_id AND s.id_tienda = smt.id_tienda
LEFT JOIN ecommdata.ubicacion_mfc um ON concat(um.sap_code, '-', um.measurement_unit) = s.ref_id AND um.store = s.id_tienda
LEFT JOIN ecommdata.instock ins 
  ON s.id_tienda = ins.tienda 
 AND s.material = ins.material 
 AND ins.fecha = (s.fecha - interval '1 day')::date
WHERE s.fecha = '{{ds}}'::date
  AND s.id_tienda NOT IN ('0486', '0053', '0398', '0054');

COMMIT;

ANALYZE ecommdata.publicacion_catalogo;