from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum
from datetime import datetime, timedelta

def stock_lista8(ds):
    import pandas as pd
    stock_tiendas_query = """select _t.*
                    from( 
                    select s.fecha,
                    s.ref_id,
                    l.id_tienda,
                    l.stock_x_umv,
                    s.stock_janis,
                    case
                        when (l.umv in ('DIS','CJ')) then trunc(l.stock_x_umv,0) 
                        when (l.umv in ('KG','KGV')) then round(l.stock_x_umv/s.multiplicador_unidad_medida,0)
                        else l.stock_x_umv
                    end as stock_sap,
                    s.multiplicador_unidad_medida
                    from ecommdata.lista8 as l
                    inner Join ecommdata.stock as s
                    on l.fecha = s.fecha and l.id_tienda = s.id_tienda and s.ref_id = CONCAT(LPAD(l.material, 18, '0'), '-', l.umv)  
                    where l.fecha = """+ds+"""
                    and l.umv <> 'PAQ') as _t 
                    group by 
                    _t.fecha,
                    _t.ref_id,
                    _t.id_tienda,
                    _t.stock_x_umv,
                    _t.stock_janis,
                    _t.stock_sap,
                    _t.multiplicador_unidad_medida"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    #print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["fecha","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","multiplicador_medida"]
    cursor.close()
    pg_connection.close()
    return results

def skus_carnes_padre_hijo():
    import pandas as pd
    stock_tiendas_query = """select sk.erp_id ,sk.ref_id, sk.nombre_sku, st.c1, st.id_tienda
                    from ecommdata.skus as sk
                    left join ecommdata.stock as st
                    on sk.ref_id = st.ref_id
                    where substring(sk.ref_id,strpos(sk.ref_id,'-')+1,length(sk.ref_id)-strpos(sk.ref_id,'-')) in ('KG','KGV')
                    and split_part(sk.ref_id,'-',1) <> erp_id
                    and st.c1 = 'Carnes'"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["material","ref_id","descripcion","categoria","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results

def render_netezza_view(id_tienda,id_material,ds):
    

    return df
