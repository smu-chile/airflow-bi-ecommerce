begin;
update operaciones_alvi.cumplimiento_despacho
set cumplimiento_ondate = 999
where termino_ventana between 
                        ('{{ts}}' at time zone 'America/Santiago')::date - interval '1 day' 
                        and 
                        ('{{ts}}' at time zone 'America/Santiago')::date
and cumplimiento_ondate = 40;
update operaciones_alvi.cumplimiento_despacho
set cumplimiento_ontime = 999
where termino_ventana between 
                        ('{{ts}}' at time zone 'America/Santiago') - interval '1 day'  
                        and 
                        ('{{ts}}' at time zone 'America/Santiago')
and cumplimiento_ontime = 40;
commit;
