BEGIN TRANSACTION;
delete from ecommdata.p_and_l where fecha = '{{ds}}'::date;
insert into ecommdata.p_and_l (fecha, canal_venta, sku_key, ref_id, material, umv, nombre, nombre_sap, 
                                id_tienda, nombre_local, categoria_sap, grupo_sap, linea_sap,negocio_sap, seccion_sap, categoria_ecom_1, categoria_ecom_2, 
                                categoria_ecom_3, venta, venta_umb, costo_neto, numero_aprox_trx)
select cvst.fecha ,cvst.canal_venta ,
cvst.sku_key,
concat(cvst.material, '-',cvst.umv) as ref_id,
cvst.material,cvst.umv,
p.nombre,
cvst.descripcion as nombre_sap,
cvst.id_tienda, cvst.nombre_local,
cvst.categoria as  categoria_sap, cvst.grupo as grupo_sap, cvst.linea as linea_sap, cvst.negocio as negocio_sap, cvst.seccion as seccion_sap,
c.n1 as categoria_ecom_1,c.n2 as categoria_ecom_2, c.n3 as categoria_ecom_3,
cvst.venta ,cvst.venta_umb ,cvst.costo_neto ,cvst.numero_aprox_trx
from ecommdata.costos_ventas_sku_tienda cvst 
left join ecommdata.productos p on p.ref_id = concat(cvst.material, '-',cvst.umv)
left join ecommdata.categorias c on c.id = p.id_categoria 
where id_tienda not in (select id from ecommdata_alvi.tiendas t) --saca Alvi
and cvst.fecha = '{{ds}}'::date;
COMMIT