BEGIN TRANSACTION;
delete from ecommdata.p_and_l where fecha = '{{ds}}'::date;
insert into ecommdata.p_and_l (fecha, canal_venta, sku_key, ref_id, material, umv, nombre, nombre_sap, 
                                id_tienda, nombre_local, categoria_sap, grupo_sap, linea_sap,negocio_sap, seccion_sap, categoria_ecom_1, categoria_ecom_2, 
                                categoria_ecom_3, venta, venta_umb, costo_neto, numero_aprox_trx)
select cvst.fecha ,cvst.canal_venta ,
cvst.sku_key,
concat(msp.material, '-',REPLACE(msp.umb, 'ST', 'UN')) as ref_id, msp.material,REPLACE(msp.umb, 'ST', 'UN') as umv ,p.nombre, msp.sku_name as nombre_sap ,
cvst.id_tienda, cvst.nombre_local, msp.categoria_sap,msp.grupo_sap , msp.linea_sap , msp.negocio_sap , msp.seccion_sap,
c.n1 as categoria_ecom_1,c.n2 as categoria_ecom_2, c.n3 as categoria_ecom_3,
cvst.venta ,cvst.venta_umb ,cvst.costo_neto ,cvst.numero_aprox_trx
from ecommdata.costos_ventas_sku_tienda cvst 
left join ecommdata.maestra_sku_proveedor msp on cvst.sku_key = msp.sku_key::varchar
left join ecommdata.productos p on p.ref_id = concat(msp.material, '-',REPLACE(msp.umb, 'ST', 'UN'))
left join ecommdata.categorias c on c.id = p.id_categoria 
where id_tienda not in (select id from ecommdata_alvi.tiendas t) --saca Alvi
and cvst.fecha = '{{ds}}'::date
and msp.material is not null;
COMMIT