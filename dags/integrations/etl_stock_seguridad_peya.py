from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

from datetime import datetime, timedelta

def _check_time(ts):
    
    exec_datetime = datetime.strptime(ts[:16], "%Y-%m-%dT%H:%M")
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    exec_datetime_local_str = exec_datetime_local.strftime("%Y-%m-%dT%H:%M")
    print(exec_datetime_local_str)

    time_str = exec_datetime_local_str.split("T")[1]
    if (time_str == "17:10") or (time_str == "21:10") or (time_str == "01:10") or (time_str == "13:10"):
        return "task_skip"
    elif (time_str == "05:10"):
        return "stock_ventas_tiendas_to_s3_am"
    else:
        return "stock_ventas_tiendas_to_s3_pm"

def stock(ds):
    stock_tiendas_query = f"""select id_tienda ,
                        concat(material,'-',unidad_de_medida) as ref_id, 
                        date_part('dow','{ds}'::date) as dia,
                        date_part('week','{ds}'::date) as semana
                        from integraciones.lm_stock_precio_promo lspp
                        """
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    print(stock_tiendas_query)
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_tiendas_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def matriz_ss():
    import pandas as pd
    matriz_query = """select *
                    from catalogo.matriz_ss_peya ms """
    print(matriz_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(matriz_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","peso"]
    cursor.close()
    pg_connection.close()
    return results

def promociones():
    import pandas as pd
    promociones_query = """select distinct concat(material,'-',unidad_de_medida) as ref_id
                    from integraciones.lm_stock_precio_promo lspp 
                    where lspp.precio_promocional is not null """
    print(promociones_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id"]
    cursor.close()
    pg_connection.close()

    return results


def venta_tienda():
    ventas_skus_tienda_query = """select 
                lpad(id_tienda,4,'0') as id_tienda,
                concat(lpad(material,18,'0'),'-' ,umv) as ref_id, 
                date_part('dow',fecha) as dia,
                date_part('week',fecha) as semana,
                sum(venta_umv) as cantidad
                from ecommdata.venta_sku_tienda vst
                where id_tienda in (select ltrim(id,'0')
                    from integraciones.tiendas_last_millers tlm 
                    where (id_peya is not null 
                    or id_peya_botilleria is not null
                    or peya_market is not null))
                group by id_tienda,concat(lpad(material,18,'0'),'-',umv),fecha"""
    print(ventas_skus_tienda_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_skus_tienda_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def stock_ventas_tiendas_to_s3_am(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_peya/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(stock(ds))
    print("se ha cargado stock\n")
    print(df_stock)
    df_stock.columns = ["id_tienda","ref_id","dia","semana"]
    df_venta_tienda = pd.DataFrame(venta_tienda())
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    df_venta_tienda.columns = ["id_tienda","ref_id","dia","semana","cantidad"]
    df_promociones = promociones()
    df_promociones = df_promociones.drop_duplicates(subset='ref_id')
    print("se ha cargado promociones \n")
    print(df_promociones)
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    print(df_aux1.head())
    df_aux1 = df_aux1[["id_tienda","ref_id","dia","cantidad"]]
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    print(df_aux2.head())
    df_aux2 = df_aux2[["id_tienda","ref_id","dia","cantidad"]]

    df_max = df_aux1.groupby(['id_tienda', 'ref_id', 'dia']).max().reset_index()

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)
    print(df_stock_seguridad["cantidad"])
    df_stock_seguridad["cantidad"] = df_stock_seguridad["cantidad"]*0.5
    print(df_stock_seguridad["cantidad"])

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)

    ###############################################
    #        filtrado por dia y promociones       #
    ###############################################

    lista_promos = df_promociones['ref_id'].unique()

    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_final=df_stock_seguridad_aux.merge(df_max, on = ['ref_id','id_tienda','dia'])
    df_final=df_final[df_stock_seguridad_aux["dia"] == dia]
    print(df_final)
    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad","cantidad"]]
    print(df_final)

    df_final["dia"] = df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"] = df_final["nuevo_stock_seguridad"].astype(int)

    df_final['is_promo'] = df_final['ref_id'].apply(lambda x: 'X' if x in lista_promos else '')

    print("transformacion de datos listo \n")
    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()

    df_final = df_final.merge(df_matriz, how='left', on=["id_tienda"])
    df_final["peso"] = df_final["peso"].fillna(1)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad","cantidad","is_promo"]]

    condlist = [df_final["nuevo_stock_seguridad"]>=2,
                df_final["nuevo_stock_seguridad"]<2]
    choicelist = [df_final["nuevo_stock_seguridad"], 2]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"],2)
    df_final.columns = ["id_tienda","ref_id","dia","stock_seguridad","maximo","is_promo"]
    print(df_final.head())

    condlist = [df_final["is_promo"] == 'X',
                df_final["is_promo"] != 'X']
    choicelist = [df_final["maximo"], df_final["stock_seguridad"]]

    df_final["stock_seguridad"] = np.select(condlist, choicelist)

    condlist = [df_final["stock_seguridad"]>=2,
                df_final["stock_seguridad"]<2]
    choicelist = [df_final["stock_seguridad"], 2]

    df_final["stock_seguridad"] = np.select(condlist, choicelist)


    df_stock_incluir = df_stock[df_stock["dia"] == dia]
    df_stock_incluir.info()
    df_incluir = df_stock_incluir.merge(df_final, how = 'left', on = ['id_tienda','ref_id'])
    print("\nmerge listo\n")
    df_incluir = df_incluir[df_incluir["dia_y"].isnull()]
    print("\nfiltro null listo\n")
    df_incluir = df_incluir[['id_tienda','ref_id']]
    df_incluir["dia"] = dia
    df_incluir["stock_seguridad"] = 2
    df_incluir["is_promo"] = ''
    df_incluir["maximo"] = 0
    df_incluir.info()
    print(df_incluir.head(30))

    df_final = pd.concat([df_final, df_incluir], ignore_index=True)

    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_peya/{exec_date}/stock_seguridad_peya_am_{date_aux}.csv"
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

def stock_ventas_tiendas_to_s3_pm(ds):
    import pandas as pd
    import numpy as np
    import io
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"stock_seguridad_peya/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock = pd.DataFrame(stock(ds))
    print("se ha cargado stock\n")
    print(df_stock)
    df_stock.columns = ["id_tienda","ref_id","dia","semana"]
    df_venta_tienda = pd.DataFrame(venta_tienda())
    print("se ha cargado ventas\n")
    print(df_venta_tienda)
    df_venta_tienda.columns = ["id_tienda","ref_id","dia","semana","cantidad"]
    df_promociones = promociones()
    df_promociones = df_promociones.drop_duplicates(subset='ref_id')
    print("se ha cargado promociones \n")
    print(df_promociones)
    
    print("\nse ha terminado de extraer data \n")

    #########################
    #transformacion de datos#
    #########################

    df_aux1 = df_venta_tienda.groupby(by=["id_tienda","ref_id","dia","semana"], as_index=False).sum()
    print(df_aux1.head())
    df_aux1 = df_aux1[["id_tienda","ref_id","dia","cantidad"]]
    df_aux2 = df_aux1.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    print(df_aux2.head())
    df_aux2 = df_aux2[["id_tienda","ref_id","dia","cantidad"]]

    df_max = df_aux1.groupby(['id_tienda', 'ref_id', 'dia']).max().reset_index()

    df_stock_seguridad = df_stock.merge(df_aux2, how='left', on=["id_tienda","ref_id","dia"])
    df_stock_seguridad = df_stock_seguridad.fillna(0)

    condlist = [df_stock_seguridad["cantidad"]>=2,
                df_stock_seguridad["cantidad"]<2]
    choicelist = [df_stock_seguridad["cantidad"], 2]

    df_stock_seguridad["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_stock_seguridad["nuevo_stock_seguridad"] = round(df_stock_seguridad["nuevo_stock_seguridad"],2)

    df_stock_seguridad=df_stock_seguridad[["ref_id","id_tienda","dia","nuevo_stock_seguridad"]]
    df_stock_seguridad_aux = df_stock_seguridad.groupby(by=["id_tienda","ref_id","dia"], as_index=False).mean()
    df_stock_seguridad_aux["nuevo_stock_seguridad"] =round(df_stock_seguridad_aux["nuevo_stock_seguridad"],0)

    ###############################################
    #        filtrado por dia y promociones       #
    ###############################################

    lista_promos = df_promociones['ref_id'].unique()

    fecha_str = ds
    formato_str = "%Y-%m-%d"

    dia = datetime.strptime(fecha_str, formato_str) 
    dia = dia.weekday()
    dia = (dia + 1) % 7
    df_final=df_stock_seguridad_aux.merge(df_max, on = ['ref_id','id_tienda','dia'])
    df_final=df_final[df_stock_seguridad_aux["dia"] == dia]
    print(df_final)
    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad","cantidad"]]
    print(df_final)

    df_final["dia"] = df_final["dia"].astype(int)
    df_final["nuevo_stock_seguridad"] = df_final["nuevo_stock_seguridad"].astype(int)

    df_final['is_promo'] = df_final['ref_id'].apply(lambda x: 'X' if x in lista_promos else '')

    print("transformacion de datos listo \n")

    #################
    #Matrix de Pesos#
    #################
    
    df_matriz = matriz_ss()

    df_final = df_final.merge(df_matriz, how='left', on=["id_tienda"])
    df_final["peso"] = df_final["peso"].fillna(1)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"] * df_final["peso"],0)

    df_final = df_final[["id_tienda","ref_id","dia","nuevo_stock_seguridad","cantidad","is_promo"]]

    condlist = [df_final["nuevo_stock_seguridad"]>=2,
                df_final["nuevo_stock_seguridad"]<2]
    choicelist = [df_final["nuevo_stock_seguridad"], 2]

    df_final["nuevo_stock_seguridad"] = np.select(condlist, choicelist)
    df_final["nuevo_stock_seguridad"] = round(df_final["nuevo_stock_seguridad"],2)
    df_final.columns = ["id_tienda","ref_id","dia","stock_seguridad","maximo","is_promo"]
    print(df_final.head())

    condlist = [df_final["is_promo"] == 'X',
                df_final["is_promo"] != 'X']
    choicelist = [df_final["maximo"], df_final["stock_seguridad"]]
    df_final["stock_seguridad"] = np.select(condlist, choicelist)

    condlist = [df_final["stock_seguridad"]>=2,
                df_final["stock_seguridad"]<2]
    choicelist = [df_final["stock_seguridad"], 2]

    df_final["stock_seguridad"] = np.select(condlist, choicelist)

    df_stock_incluir = df_stock[df_stock["dia"] == dia]
    df_stock_incluir.info()
    df_incluir = df_stock_incluir.merge(df_final, how = 'left', on = ['id_tienda','ref_id'])
    print("\nmerge listo\n")
    df_incluir = df_incluir[df_incluir["dia_y"].isnull()]
    print("\nfiltro null listo\n")
    df_incluir = df_incluir[['id_tienda','ref_id']]
    df_incluir["dia"] = dia
    df_incluir["stock_seguridad"] = 2
    df_incluir["is_promo"] = ''
    df_incluir["maximo"] = 0
    df_incluir.info()
    print(df_incluir.head(30))

    df_final = pd.concat([df_final, df_incluir], ignore_index=True)

    ##############
    #cargar datos#
    ##############

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"stock_seguridad_peya/{exec_date}/stock_seguridad_peya_pm_{date_aux}.csv"
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

def stock_seguridad_to_postgres_am(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_am"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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

    df['stock_seguridad'] = pd.to_numeric(df['stock_seguridad'], errors='coerce').astype('Int64')
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df = df[['id_tienda','ref_id','dia','stock_seguridad']]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("truncate table integraciones.stock_seguridad_peya")
        df.to_sql(name="stock_seguridad_peya",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def stock_seguridad_to_postgres_pm(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_ventas_tiendas_to_s3_pm"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
    df['stock_seguridad'] = pd.to_numeric(df['stock_seguridad'], errors='coerce').astype('Int64')
    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df = df[['id_tienda','ref_id','dia','stock_seguridad']]

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("truncate table integraciones.stock_seguridad_peya")
        df.to_sql(name="stock_seguridad_peya",
                    con=conn,         
                    schema="integraciones",         
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
    'etl_stock_seguridad_peya',
    default_args=default_args,
    description="cargar stock de seguridad a peya",
    schedule="10 1/4 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "integraciones", "stock", "stock_seguidad", "ventas", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Carga stock de seguridad de peya\n
    guardar en S3.
    """ 
    t0 = BranchPythonOperator(
        task_id='check_time',
        python_callable=_check_time,
    )

    t_dummy = EmptyOperator(
            task_id='task_skip',
        )

    t1_am = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_am",
        python_callable = stock_ventas_tiendas_to_s3_am,
    )

    t1_pm = PythonOperator(
        task_id = "stock_ventas_tiendas_to_s3_pm",
        python_callable = stock_ventas_tiendas_to_s3_pm,
    )

    t2_am = PythonOperator(
        task_id = "stock_seguridad_to_postgres_am",
        python_callable = stock_seguridad_to_postgres_am,
    )

    t2_pm = PythonOperator(
        task_id = "stock_seguridad_to_postgres_pm",
        python_callable = stock_seguridad_to_postgres_pm,
    )

    t0 >> t1_am >> t2_am
    t0 >> t1_pm >> t2_pm
    t0 >> t_dummy


