from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum


from datetime import datetime, timedelta

def _get_stock():
    import pandas as pd
    from io import StringIO
    
    stock_query = "select id_tienda, glosa_tienda, ref_id, stock_janis, stock_seguridad_janis, date_part('dow',fecha) as dia, date_part('week',fecha) as semana  from ecommdata.stock   where fecha >= current_date -30   and stock_janis is not null and surtido_ecommerce = 'True' and id_tienda ='"+tienda+"'"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_ventas():
    import pandas as pd
    from io import StringIO
    
    stock_query = "select LPAD(v.id_tienda , 4, '0') as id_tienda, CONCAT(LPAD(v.material, 18, '0'), '-', v.umv) as ref_id, case  	when (v.umv = 'UN') then round(v.venta_bruta/v.venta_umv,0) 	else v.venta_bruta   end as precio_venta, p.precio_lista, v.venta_umv, date_part('dow',v.fecha) as dia, date_part('week',v.fecha) as semana from ecommdata.venta_sku_tienda as v  left join ecommdata.tiendas t on LPAD(v.id_tienda , 4, '0') = t.id left join ecommdata.precios as p on CONCAT(LPAD(v.material, 18, '0'), '-', v.umv) = p.ref_id and p.id_tienda_janis = t.id_janis where v.fecha >= current_date -30 and v.venta_umv > 0 and v.venta_bruta <> 0 and v.organizacion = 'Unimarc' and LPAD(v.id_tienda , 4, '0') = '"+tienda+"'"
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def _get_stock_tiendas():
    import pandas as pd
    from io import StringIO
    
    tiendas = ['Mirador','Los Militares','Los Leones','Coyhaique','La Chimba']
    id_tiendas = {'Los Militares':'0469','Los Leones':'0333','Coyhaique':'0442','Mirador':'0581','La Chimba':'0034'}
    stock_data = []
    for x in tiendas:
        print(x+" "+id_tiendas[x])
        data = pd.DataFrame(_get_stock(id_tiendas[x]))
        stock_data.append(data)
    stock_data = pd.concat(stock_data)
    stock_data.columns=["id_tienda","glosa_tienda","ref_id","stock_janis","stock_seguridad","dia","semana"]


def _get_venta_tiendas():
    import pandas as pd
    from io import StringIO
    
    tiendas = ['Mirador','Los Militares','Los Leones','Coyhaique','La Chimba']
    id_tiendas = {'Los Militares':'0469','Los Leones':'0333','Coyhaique':'0442','Mirador':'0581','La Chimba': '0034'}
    diccionario_glosa = {'Los Militares':'0469 - LOS MILITARES','Los Leones':'0333 - LOS LEONES','Coyhaique':'0442 - COYHAIQUE','Mirador':'0581 - MIRADOR'}
    ventas_tiendas_data = []
    for x in tiendas:
        print(x+" "+id_tiendas[x])
        data = pd.DataFrame(_get_ventas(id_tiendas[x]))
        ventas_tiendas_data.append(data)
    ventas_tiendas_data = pd.concat(ventas_tiendas_data)
    ventas_tiendas_data.columns =["id_tienda","ref_id","venta","precio_lista","cantidad","dia","semana"]

def _stock_de_seguridad():
    ventas_tiendas_data.precio_lista.fillna(ventas_tiendas_data.venta, inplace=True)
    ventas_tiendas_data["venta"]=ventas_tiendas_data["venta"].astype(str).astype(int)
    ventas_tiendas_data["promo"] = ((ventas_tiendas_data["venta"]/ventas_tiendas_data["precio_lista"])-1)*-100
    ventas_tiendas_data = ventas_tiendas_data[ventas_tiendas_data["promo"] <= 20]
    ventas_tiendas_data=ventas_tiendas_data[["id_tienda","ref_id","cantidad","dia","semana"]]

    xd = ventas_tiendas_data.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    xd2 = xd.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    xd2=xd2[["id_tienda","ref_id","dia","semana","cantidad"]]

    df_stock_seguridad = stock_data.merge(xd2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad=df_stock_seguridad.fillna(0)

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]
  
    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)




default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    'etl_stock_seguridad',
    default_args=default_args,
    description="cambia el valor de stock de seguridad",
    schedule_interval="09 23 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "ventas", "ecommdata_unimarc", "stock", "janis", "Unimarc"],
) as dag: