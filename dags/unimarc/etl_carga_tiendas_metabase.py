from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def lista8():
    import pandas as pd
    promociones_query = """select concat(material,'-',umv) as ref_id, id_tienda, fecha
                    from ecommdata.lista8"""
    print(promociones_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(promociones_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","fecha"]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def productos():
    import pandas as pd
    productos_query = """select ref_id, nombre 
                    from ecommdata.productos"""
    print(productos_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","nombre_producto"]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def tiendas():
    import pandas as pd
    tiendas_query = """select id, status, nombre_tienda_janis
                    from ecommdata.tiendas t 
                    where status = 1"""
    print(tiendas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(tiendas_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["id_tienda","status","nombre_tienda_janis"]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def skus():
    import pandas as pd
    skus_query = """select ref_id, nombre_sku
                    from ecommdata.skus"""
    print(skus_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(skus_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","nombre_sku"]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def producto_tienda_janis():
    import pandas as pd
    productos_tienda_query = """select ref_id, id_tienda, activo
                        from ecommdata.productos_tienda
                        where activo is true"""
    print(productos_tienda_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_tienda_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","activo"]
    results = results[["ref_id","id_tienda"]]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def excluidos_x_tiendas():
    import pandas as pd
    excluidos_query = """select ref_id,id_tienda,is_mfc,all_stores,fecha_carga
                    from ecommdata.producto_tienda_excluidos"""
    print(excluidos_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(excluidos_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]
    results = results[["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def publicacion_1917_today(ts):
    import pandas as pd
    mfc_query = f"""select pc.ref_id, pc.id_tienda,
                    TO_CHAR(DATE_TRUNC('DAY', fecha_hora),'YYYY-MM-DD') AS fecha
                    from ecommdata.publicacion_catalogo pc
                    where pc.mfc is true
                    and pc.fecha_hora::date >= '{ts}'::date+1
                    and pc.stock_janis > 0
                    ;"""

    print(mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(mfc_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","fecha",]
    results = results[["ref_id","id_tienda","fecha"]]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results


def load_tables_to_s3(ts,ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"carga_tiendas/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_producto_tienda_janis = producto_tienda_janis()
    print(f"Ready productos por tienda en janis de hoy{len(df_producto_tienda_janis.index)}\n")
    df_lista8 = lista8()
    print(f"Ready lista8 de hoy {len(df_lista8.index)}\n")
    df_productos = productos()
    print("Ready productos\n")
    df_skus = skus()
    print("Ready skus\n")
    df_tiendas = tiendas()
    print("Ready tiendas activas\n")
    df_excluidos_x_tiendas = excluidos_x_tiendas()
    print("Ready excluidos_x_tiendas activas\n")
    df_publicacion_mfc_hoy = publicacion_1917_today(ts)
    print("Ready publicacion_1917_today activas\n")

    #Activos
    df_excluidos_x_tiendas_clean = df_excluidos_x_tiendas[df_excluidos_x_tiendas["all_stores"]== 1]
    df_excluidos_x_tiendas_mfc = df_excluidos_x_tiendas[df_excluidos_x_tiendas["is_mfc"]== 1]

    df_productos_sin_skus = df_productos.merge(df_lista8, on = ["ref_id"], how = 'left')
    df_skus_sin_producto = df_productos_sin_skus.merge(df_skus, on = ["ref_id"], how = 'left')
    df_skus_sin_producto = df_skus_sin_producto[(df_skus_sin_producto["id_tienda"].notna()) &
                                                (df_skus_sin_producto["nombre_sku"].isna())
                                                ].drop_duplicates(subset=['ref_id']).reset_index(drop=True)

    df_skus_sin_producto = df_skus_sin_producto[["ref_id"]]
    lista_skus_sin_producto = df_skus_sin_producto["ref_id"].to_list()
    lista_skus_excluidos = df_excluidos_x_tiendas_clean["ref_id"].to_list()

    print(f"\nRegistros de Lista8: {len(df_lista8.index)}\n")
    print(f"\nRegistros de mfc: {len(df_publicacion_mfc_hoy.index)}\n")

    df_lista8_hoy = pd.concat([df_lista8, df_publicacion_mfc_hoy], axis=0)

    print(f"\nRegistros de mfc + L8: {len(df_lista8_hoy.index)}\n")
    print(f"\nRegistros de productos tienda en janis: {len(df_producto_tienda_janis.index)}\n")

    df_activos = (df_lista8_hoy.merge(df_producto_tienda_janis, on=["ref_id","id_tienda"], how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge',axis= 1))

    df_activos = df_activos[["ref_id","id_tienda"]]

    print(f"\nRegistros que no se encuentran en janis pero si en L8 + MFC : {len(df_activos.index)}\n")

    df_activos = df_activos.drop_duplicates()
    df_activos = df_activos.reset_index(drop=True)

    print(f"\nRegistros Productos tienda actualizables en total sin duplicados: {len(df_activos.index)}\n")

    df_activos = df_activos[~df_activos['ref_id'].isin(lista_skus_excluidos)]
    print(f"\nfiltro quitando excluidos: {len(df_activos.index)}\n")

    tiendas_activas = df_tiendas["id_tienda"].to_list()
    df_activos = df_activos[df_activos['id_tienda'].isin(tiendas_activas)]
    print(f"\nfiltro por tiendas activas: {len(df_activos.index)}\n")

    productos_validos = df_productos["ref_id"].to_list()
    df_activos = df_activos[df_activos['ref_id'].isin(productos_validos)]
    print(f"\nfiltro por producto valido: {len(df_activos.index)}\n")

    skus_validos = df_skus["ref_id"].to_list()
    df_activos = df_activos[df_activos['ref_id'].isin(skus_validos)]
    print(f"\nfiltro por skus valido: {len(df_activos.index)}\n")

    df_activos = df_activos[~df_activos['ref_id'].isin(lista_skus_sin_producto)]
    print(f"\nfiltro por skus sin producto: {len(df_activos.index)}\n")

    df_activos = df_activos.drop_duplicates()
    df_activos = df_activos.reset_index(drop=True)


    df_activos_skus = df_activos_productos = df_activos

    df_activos_productos = df_activos


    valores_unicos_skus = df_activos_skus['ref_id'].unique()
    print(f"\nSkus unicos: {len(valores_unicos_skus)}")
    valores_unicos_productos = df_activos_productos['ref_id'].unique()
    print(f"\nProductos unicos: {len(valores_unicos_productos)}")

    tiendas_activas = df_tiendas["id_tienda"].to_list()

    df_lista8_clean = df_lista8[df_lista8['ref_id'].isin(valores_unicos_productos)]
    df_lista8_clean = df_lista8_clean[df_lista8_clean['id_tienda'].isin(tiendas_activas)]
    df_lista8_clean = df_lista8_clean[~df_lista8_clean['ref_id'].isin(lista_skus_excluidos)]
    print(f"\nregistros de lista8 validos: {len(df_lista8_clean.index)}\n")

    df_lista8_mfc = df_publicacion_mfc_hoy[df_publicacion_mfc_hoy['ref_id'].isin(valores_unicos_productos)]
    df_lista8_mfc = df_lista8_mfc[df_lista8_mfc['id_tienda'].isin(tiendas_activas)]
    df_lista8_mfc = df_lista8_mfc[~df_lista8_mfc['ref_id'].isin(lista_skus_excluidos)]
    print(f"\nregistros de mfc validos: {len(df_lista8_mfc.index)}\n")

    df_lista8_clean = pd.concat([df_lista8_clean, df_lista8_mfc], axis=0)
    df_lista8_clean = df_lista8_clean.drop_duplicates()
    df_lista8_clean = df_lista8_clean.reset_index(drop=True)

    print(f"\nRegistros de (Lista8+mfc): {len(df_lista8_clean.index)}\n")

    #acá sacamos el archivo listo de skus activos
    df_activos_skus = df_lista8_clean[df_lista8_clean['ref_id'].isin(valores_unicos_skus)]
    df_activos_skus = df_activos_skus[["ref_id"]]
    df_final_skus_activos = df_activos_skus.drop_duplicates(subset=['ref_id']).reset_index(drop=True)
    df_final_skus_activos.columns = ["refId"]
    df_final_skus_activos["publish"] = 1
    df_final_skus_activos["updatePending"] = 1
    df_final_skus_activos["active"] = 1

    #acá sacamos el archivo listo de productos activos
    df_activos_productos = df_lista8_clean.merge(df_excluidos_x_tiendas_mfc, how = 'left', on= ["ref_id","id_tienda"])
    df_activos_productos = df_activos_productos[df_activos_productos["is_mfc"]!= 1] 
    df_activos_productos = df_activos_productos[df_activos_productos['ref_id'].isin(valores_unicos_productos)]
    df_activos_productos = df_activos_productos[df_activos_productos['id_tienda'].isin(tiendas_activas)]
    df_activos_productos = df_activos_productos[["ref_id","id_tienda"]]
    df_activos_productos = df_activos_productos.drop_duplicates()
    df_activos_productos = df_activos_productos.reset_index(drop=True)
    print(f"\nRegistros validos para productos activos desde lista8+mfc: {len(df_activos_productos.index)}\n")
    df_final_productos_activos = df_activos_productos.groupby('ref_id')['id_tienda'].apply(list).reset_index()
    df_final_productos_activos['id_tienda'] = df_final_productos_activos['id_tienda'].apply(lambda x: ', '.join(map(str, x)))
    df_final_productos_activos.columns = ["refId","stores"]
    df_final_productos_activos["visible"] = 1
    df_final_productos_activos["publish"] = 1
    df_final_productos_activos["updatePending"] = 1
    df_final_productos_activos["active"] = 1
    df_final_productos_activos = df_final_productos_activos.drop_duplicates()
    df_final_productos_activos = df_final_productos_activos.reset_index(drop=True)

    df_lista8_desactivar = pd.concat([df_lista8, df_publicacion_mfc_hoy], axis=0)

    df_desactivados = (df_producto_tienda_janis.merge(df_lista8_desactivar, on=["ref_id","id_tienda"], how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge',axis= 1))

    print(f"\nRegistros a desactivar {len(df_desactivados.index)}\n")

    df_desactivados = df_desactivados[df_desactivados['id_tienda'].isin(tiendas_activas)]
    print(f"\nfiltro por tienda inactivas: {len(df_desactivados.index)}\n")

    lista_skus_activos = df_final_skus_activos['refId'].unique()
    df_desactivados = df_desactivados[~df_desactivados['ref_id'].isin(lista_skus_activos)]
    print(f"\nfiltro por skus activos: {len(df_desactivados.index)}\n")

    valores_unicos_skus = df_desactivados['ref_id'].unique()
    print(f"\nSkus unicos: {len(valores_unicos_skus)}")

    df_excluidos = df_producto_tienda_janis.merge(df_excluidos_x_tiendas_clean, on=["ref_id"], how='inner')
    df_excluidos = df_excluidos[df_excluidos["id_tienda_x"]!= '9212']
    df_excluidos = df_excluidos[df_excluidos['id_tienda_x'].isin(tiendas_activas)]
    df_excluidos = df_excluidos[~df_excluidos['ref_id'].isin(lista_skus_activos)]
    df_excluidos = df_excluidos.drop_duplicates(subset="ref_id")
    df_excluidos = df_excluidos.reset_index(drop=True)
    df_excluidos = df_excluidos[["ref_id"]]
    df_excluidos.columns = ["refId"]
    print("\ndf_excluidos: ",len(df_excluidos.index))


    df_desactivados_sku = df_desactivados[["ref_id"]]
    df_desactivados_sku.columns = ["refId"]
    df_desactivados_sku = pd.concat([df_desactivados_sku, df_excluidos], axis=0)
    df_desactivados_sku = df_desactivados_sku.drop_duplicates(subset=['refId']).reset_index(drop=True)
    df_desactivados_sku["publish"] = 1
    df_desactivados_sku["updatePending"] = 1
    df_desactivados_sku["active"] = 0


    df_desactivados_productos = df_desactivados[["ref_id"]]
    df_desactivados_productos.columns = ["refId"]
    df_desactivados_productos = pd.concat([df_desactivados_productos, df_excluidos], axis=0)
    df_desactivados_productos = df_desactivados_productos.drop_duplicates(subset=['refId']).reset_index(drop=True)
    df_desactivados_productos["stores"] = "9212"
    df_desactivados_productos["publish"] = 1
    df_desactivados_productos["updatePending"] = 1
    df_desactivados_productos["visible"] = 0
    df_desactivados_productos["active"] = 0

    df_final_productos = pd.concat([df_desactivados_productos, df_final_productos_activos], axis=0)
    df_final_skus = pd.concat([df_desactivados_sku, df_final_skus_activos], axis=0)


    buffer_1 = io.StringIO()
    df_final_productos.to_csv(buffer_1, header=True, index=False, encoding="utf-8")
    buffer_1.seek(0)
    
    buffer_2 = io.StringIO()
    df_final_skus.to_csv(buffer_2, header=True, index=False, encoding="utf-8")
    buffer_2.seek(0)

    filename_productos = f"carga_tiendas/{exec_date}/productos_{date_aux}.csv"
    filename_skus = f"carga_tiendas/{exec_date}/skus_{date_aux}.csv"

    print(f"con fecha {ds} y nombre de filename como {filename_productos}")
    s3_hook.load_string(buffer_1.getvalue(),
                key=filename_productos,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"con fecha {ds} y nombre de filename como {filename_skus}")
    s3_hook.load_string(buffer_2.getvalue(),
                key=filename_skus,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)


    print("se logro transformar los dataframes a archivos .csv")
    print(f"File load on S3: {prefix}")

    return filename_productos,filename_skus


def load_tables_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename_productos,filename_skus = ti.xcom_pull(key="return_value", task_ids=["load_tables_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #productos
    print("Searching file: "+filename_productos)
    if not s3_hook.check_for_key(filename_productos, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename_productos)

    s_stock_object = s3_hook.get_key(filename_productos, bucket_name=s3_bucket)

    df_productos = pd.read_csv(s_stock_object.get()["Body"])
    if len(df_productos.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    #skus
    print("Searching file: "+filename_skus)
    if not s3_hook.check_for_key(filename_skus, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename_skus)

    s_stock_object = s3_hook.get_key(filename_skus, bucket_name=s3_bucket)

    df_skus = pd.read_csv(s_stock_object.get()["Body"])
    if len(df_skus.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df_skus.index)}")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    df_lista = [df_productos,df_skus]
    names = ["carga_productos","carga_skus"]

    for i in [0,1]:
        with engine.begin() as conn:
            conn.execute(f"TRUNCATE ecommdata.{names[i]}")
            df_lista[i].to_sql(name=names[i],
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
    'etl_carga_tiendas_metabase',
    default_args=default_args,
    description="cargar tabla de productos y skus de carga tiendas",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "ecommdata", "metabase", "unimarc"],
) as dag:
    

    dag.doc_md = """
    Carga tabla productos y skus tiendas\n
    guardar en S3.
    """ 

    t0 = ExternalTaskSensor(
        task_id="wait_for_publicacion_catalogo",
        external_dag_id='etl_publicacion_catalogo',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PostgresOperator(
        task_id = "truncate_and_load_table_producto_tienda_excluidos",
        postgres_conn_id="postgresql_conn",
        sql="sql/truncate_load_table_producto_tienda_excluidos.sql",
    )

    t2 = PythonOperator(
        task_id = 'load_tables_to_s3',
        python_callable=load_tables_to_s3,
    )

    t3 = PythonOperator(
        task_id = "load_tables_to_postgres",
        python_callable = load_tables_to_postgres,
    )
    

    t0 >> t1 >> t2 >> t3