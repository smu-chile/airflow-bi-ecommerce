from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum
from datetime import datetime, timedelta

def materiales_dentro_ventas(list_material,ds):
    import pandas as pd
    stock_tiendas_query = """select material,
                    max(fecha_inicio_de_promocion) as fecha_inicio,
                    max( fecha_fin_de_promocion) as fecha_fin
                    from ecommdata.workflow_promociones wp
                    where fecha_fin_de_promocion < '"""+ds+"""'::date 
                    and fecha_inicio_de_promocion >= '"""+ds+"""'::date -30
                    and material in ('"""+list_material+"""')
                    and id_mecanica not in (12,22,25,27,36,50,67,72,84,99,37,51,53,59,77,82,93,96,123)
                    and id_evento not in (551)
                    group by material 
                    order by max( fecha_fin_de_promocion)"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["material","fecha_inicio","fecha_fin"]
    cursor.close()
    pg_connection.close()
    return results

def venta_maxima(list_material,ds):
    import pandas as pd
    stock_tiendas_query = f"""select _t.id_tienda, _t.material, _t.dia, max(_t.venta) as maxima
                                from(select lpad(vst.id_tienda,4,'0') as id_tienda,
                                    lpad(vst.material,18,'0') as material,
                                    date_part('dow',vst.fecha) as dia,
                                    date_part('week',vst.fecha) as semana,
                                    sum(vst.venta_umv) as venta
                                    from ecommdata.venta_sku_tienda vst 
                                    left join ecommdata.tiendas t
                                    on t.id = lpad(vst.id_tienda,4,'0')
                                    where vst.fecha >= '{ds}'::date -45
                                    and lpad(vst.material,18,'0') in ('{list_material}')
                                    and lpad(vst.id_tienda,4,'0') in ('1917')
                                    group by id_tienda, material,date_part('dow',vst.fecha),date_part('week',vst.fecha)) as _t
                                group by _t.id_tienda,_t.material,_t.dia"""
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","material","dia","venta_maxima"]
    cursor.close()
    pg_connection.close()
    return results

def promociones(ds):
    import pandas as pd
    promociones_query = """select distinct wp.material
                    from ecommdata.workflow_promociones wp
                    left join ecommdata.lista8 l 
                    on wp.material = l.material
                    where wp.fecha_inicio_de_promocion <= '"""+ds+"""'::date 
                    and wp.fecha_fin_de_promocion >= '"""+ds+"""'::date 
                    and wp.id_mecanica not in (12,22,25,27,36,50,67,72,84,99,37,51,53,59,77,82,93,96,123)
                    and wp.id_evento not in (551)
                    and l.material is not null"""
    print(promociones_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    if len(results)==0:
        print("\n\n\tNo hay productos en periodo promocional")
        return 
    else:
        results=pd.DataFrame(results)
        results.columns = ["material"]
        cursor.close()
        pg_connection.close()
        return results


def ventas(list_material,fecha_inicio,fecha_fin):
    import pandas as pd
    ventas_skus_tienda_query = """select lpad(vst.id_tienda,4,'0'),
            lpad(vst.material,18,'0'),
            vst.venta_umv,
            date_part('dow',vst.fecha) as dia,
            date_part('week',vst.fecha) as semana
            from ecommdata.venta_sku_tienda vst 
            left join ecommdata.tiendas t
            on t.id = lpad(vst.id_tienda,4,'0')
            where vst.fecha >= '"""+fecha_inicio+"""'::date --fecha inicio
            and vst.fecha <= '"""+fecha_fin+"""'::date -- fecha fin
            and lpad(vst.material,18,'0') in ('"""+list_material+"""')
            and t.status = 1
            and lpad(vst.id_tienda,4,'0') in ('1917')"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    try:
        results=pd.DataFrame(results)   
        results.columns = ["id_tienda","material","venta","dia","semana"]
        cursor.close()
        pg_connection.close()
        return results
    except Exception as e:
        print(f"No hay venta con los materiales de la lista \n error: {e}")
        return


def forecast_to_s3(ds):
    import pandas as pd
    import numpy as np
    import math
    import io
    
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"mfc_forcast_suply/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #####################
    #extraccion de datos#
    #####################

    df_materiales = promociones(ds)
    list_material = df_materiales['material'].tolist()
    print(len(list_material))
    list_material = list(dict.fromkeys(list_material))
    list_material = ' '.join(list_material)
    list_material = list_material.replace(" ", "','")
    print(list_material)

    df = materiales_dentro_ventas(list_material,ds)
    
    df_grouped = df.groupby(['fecha_inicio', 'fecha_fin']).agg(lista_materiales=('material', list)).reset_index()

    df_final = pd.DataFrame()
    for i in range(len(df_grouped.index)):
        aux_list = []
        aux_list = df_grouped.lista_materiales[i]
        aux_list = ' '.join(aux_list)
        aux_list = aux_list.replace(" ", "','")
        try:
            df_aux = ventas(aux_list,str(df_grouped.fecha_inicio[i]),str(df_grouped.fecha_fin[i]))
            print(df_aux)
            df_final = pd.concat([df_final, df_aux])
        except Exception as e:
            print(f"No hay venta con los materiales: {aux_list} \n error: {e}")
            continue 

    #df_final["venta"]= df_final["venta"].apply(np.ceil)

    df_final.info()
    df_final = df_final.groupby(['id_tienda', 'material','semana','dia'])['venta'].sum().reset_index()
    df_final_aux = venta_maxima(list_material,ds)
    df_final = df_final.groupby(['id_tienda', 'material','dia'])['venta'].mean().reset_index()
    df_final_final = df_final_aux.merge(df_final, how = "left",on =["id_tienda","material","dia"]).reset_index()
    df_final_final["venta"] = df_final_final["venta"].fillna(0)

    df_final_final = df_final_final[df_final_final["id_tienda"] == '1917']

    condlist = [df_final_final["venta"] == 0,
                df_final_final["venta"] != 0]
    choicelist = [df_final_final["venta_maxima"],df_final_final["venta"]]

    df_final_final["forecast"] = np.select(condlist, choicelist)

    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"mfc_forecast_suply/{exec_date}/mfc_forcast_suply_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    print(f"File load on S3: {prefix}")

    return filename

def forecast_to_postgresql(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["forecast_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df["id_tienda"] = df["id_tienda"].apply(lambda x: str(x).zfill(4))
    df["material"] = df["material"].apply(lambda x: str(x).zfill(18))
    df = df[["id_tienda","material","venta","venta_maxima","forecast","dia"]]
    condlist = [df["dia"] == 0,
                df["dia"] == 1,
                df["dia"] == 2,
                df["dia"] == 3,
                df["dia"] == 4,
                df["dia"] == 5,
                df["dia"] == 6]
    choicelist = ["Domingo","Lunes","Martes","Miercoles","Jueves","Viernes","Sabado"]
    df["dia"] = np.select(condlist, choicelist)
    df["dia"] = df["dia"].astype("str", errors="ignore")
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.forecast_supply") 
        df.to_sql(name="forecast_supply",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_forecast_supply',
    default_args=default_args,
    description="cargar venta promedio apos en postgresql",
    schedule_interval="0 12 * * 0",
    start_date=pendulum.datetime(2023, 9, 21, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata_unimarc", "MFC", "stock_seguridad", "ventas", "unimarc", "apoteosicos", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = """
    Carga venta a postgres de los productos en periodo promocional\n
    guardar en S3.
    """ 

    t0 = PythonOperator(
        task_id = "forecast_to_s3",
        python_callable = forecast_to_s3,
    )

    t1 = PythonOperator(
        task_id = "forecast_to_postgresql",
        python_callable = forecast_to_postgresql,
    )

    t0 >> t1