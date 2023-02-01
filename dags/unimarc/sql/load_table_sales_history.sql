insert into ecommdata.historia_venta_dw
select CONCAT(lnr.material,'-',lnr.umv), lnr.id_tienda,
case 
    when '{{ds}}'::date = any (t1.fechas_facturacion) then true
    else false
end as venta_ayer,
case 
    when '{{ds}}'::date - interval '1 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_2,
case 
    when '{{ds}}'::date - interval '2 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_3,
case 
    when '{{ds}}'::date - interval '3 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_4,
case 
    when '{{ds}}'::date - interval '4 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_5,
case 
    when '{{ds}}'::date - interval '5 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_6,
case 
    when '{{ds}}'::date - interval '6 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_7,
case 
    when '{{ds}}'::date - interval '7 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_8,
case 
    when '{{ds}}'::date - interval '8 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_9,
case 
    when '{{ds}}'::date - interval '9 day' = any (t1.fechas_facturacion) then true
    else false
end as venta_10
from ecommdata.lista8 lnr
left join (
    select LPAD(vst.material, 18, '0') as material, LPAD(vst.id_tienda, 4, '0') as id_tienda , array_agg(vst.fecha) as fechas_facturacion
    from ecommdata.venta_sku_tienda vst
    group by LPAD(vst.material, 18, '0'), LPAD(vst.id_tienda, 4, '0'))t1 on lnr.material = t1.material and lnr.id_tienda = t1.id_tienda;
        """