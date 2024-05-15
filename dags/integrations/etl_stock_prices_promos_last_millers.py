from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


import pendulum

from utils.netezza_utils import load_custom_query_to_s3

from datetime import datetime, timedelta

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results
    
def _get_last_millers_stores():
    last_millers_stores_query = """
        SELECT id
        FROM integraciones.tiendas_last_millers;
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(last_millers_stores_query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    return results

def extract_stock_from_dw(ti,ds,ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    ids_tiendas = [id[0] for id in ids_tiendas]
    ids_tiendas_str = str(tuple(ids_tiendas))
    print(ids_tiendas_str)

    query = f"""SELECT S.NBR_ITM 
                , S.SKU_KEY
                , SA.SKU_PRODUCT 
                , OU.OU_ID 
            FROM DWC_SMU.SMU.VW_FACT_STOCK S
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION O ON O.ORGANIZATION_KEY = OU.ORG_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
            WHERE OU.OU_ID in {ids_tiendas_str}
            AND O.PRIM_CMRCL_NM IN ('Unimarc')
            AND S.DATE_VALUE = '{ds}'::date-1
            AND S.APLICA_STOCK = 'S'
            AND A.ALMACEN_COD = '0001'
            AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)
            AND PART.PARTICULARIDAD_COD = 'A'
            AND S.NBR_ITM > 0
            ;"""
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"stock_sap_query")
        print("Searching file: "+filename)
        return "stock_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_stock"
    
def extract_product_from_dw(ts):
    import os
    import pandas as pd
    import io
    from io import StringIO
    from utils.netezza_utils import load_custom_query_to_s3

    query = f"""SELECT P.SKU_KEY
                    , P.EAN 
                    , P.CONT_CONV_UMB
                    , P.NM
                    , P.BRAND_DESC
                    , P.UNIDAD_DE_MEDIDA
                FROM DWC_SMU.SMU.VW_DIM_PRODUCT P
                WHERE p.indic_ean_ppal = 'X';
            """
    print(query)

    try:
        filename = load_custom_query_to_s3(ts,query,"product_dw")
        print("Searching file: "+filename)
        return "product_to_postgresql"
    except Exception as err:
        print(f"error: {err}")
        return "fallo_dw_producto"

def stock_to_postgresql(ts):
    print('\n carga de stock sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    BASE_S3_PATH = "data_warehouse/"
    query_name = "stock_sap_query"
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"

    filename = prefix+query_name+".csv"  

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
    df.columns = ["nbr_itm","sku_key","sku_product","ou_id"]
    df = df[["sku_key","sku_product","ou_id","nbr_itm"]]
    df['sku_product'] = df['sku_product'].apply(lambda x: str(x).zfill(18))
    df['ou_id'] = df['ou_id'].apply(lambda x: str(x).zfill(4))
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.stock_2") 
        df.to_sql(name="stock_2",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.stock_2")
    return

def product_to_postgresql(ts):
    print('\n carga de productos sap a postgresql')
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    BASE_S3_PATH = "data_warehouse/"
    query_name = "product_dw"
    curr_datetime = ts[:16].replace("-", "/").replace("T", "/").replace(":", "")
    prefix = BASE_S3_PATH+query_name+"/"+curr_datetime+"_"

    filename = prefix+query_name+".csv"  

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
    df.columns = map(str.lower, df.columns)
    df = df.dropna(subset=df.columns[:3])
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.productos_2") 
        df.to_sql(name="productos_2",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: integraciones.productos_2")
    return

def prices_to_integrations(ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    try:
        query = f"""select t.id as id_tienda
                    , p.ref_id
                    , split_part(p.ref_id, '-', 1) as material 
                    , split_part(p.ref_id, '-', 2) as umv
                    , p.precio
                from ecommdata.precios p 
                join ecommdata.tiendas t 
                    on p.id_tienda_janis = t.id_janis 
                    and t.status = 1
                join ecommdata.lista8 l 
                    on l.material || '-' || l.umv = p.ref_id 
                    and l.id_tienda = t.id 
                where p.fecha_carga = '{ds}'::date-1"""
        df = query_to_df(query)
        print(f"informacion obtenida de la Query: {df.info()}")

        host = Variable.get("POSTGRESQL_HOST")
        database = Variable.get("POSTGRESQL_DB")
        username = Variable.get("POSTGRESQL_USER")
        password = Variable.get("POSTGRESQL_PASSWORD")
        
        conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
        engine = sqlalchemy.create_engine(conn_url)

        # Save to PostgreSQL:
        with engine.begin() as conn:
            conn.execute("TRUNCATE integraciones.precios") 
            df.to_sql(name="precios",
                        con=conn,         
                        schema="integraciones",         
                        if_exists='append',         
                        index=False,         
                        chunksize=20000,         
                        method='multi')

        print("Data loaded to Postgres: integraciones.precios")
        return "precios_postgres"
    
    except Exception as err:
        print(f"error: {err}")
        return "fallo_postgres_precios"

def promos_to_integrations(ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    try:
        query = f"""select ean,
                    case 
                        when wp.umv = 'ST' then 'UN'
                        else wp.umv
                    end as umv ,
                    wp.material,
                    min(precio_promocional) AS precio_promocional
                    from ecommdata.workflow_promociones wp 
                    where wp.fecha_inicio_de_promocion <= '{ds}'::date
                    and wp.fecha_fin_de_promocion >= '{ds}'::date
                    and wp.tipo_promocion IN (1,4)
                    and wp.registro_valido = True
                    and wp.organizacion_ventas = '1000'
                    and wp.canal_distribucion = '10'
                    and wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123,124)
                    AND wp.nombre_promocion::text !~~ '%MFC%'::text
                    AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
                    AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
                    AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
                    AND wp.nombre_promocion::text !~~ '%917%'::text
                    AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
                    and wp.nombre_promocion::text !~~ '% LOC%'::text
                    and wp.nombre_promocion::text !~~ '%LIQ%'::text
                    group by wp.ean, wp.umv, wp.material
                    ;"""
        df = query_to_df(query)
        print(f"informacion obbtenida de la Query: {df.info()}")

        host = Variable.get("POSTGRESQL_HOST")
        database = Variable.get("POSTGRESQL_DB")
        username = Variable.get("POSTGRESQL_USER")
        password = Variable.get("POSTGRESQL_PASSWORD")
        
        conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
        engine = sqlalchemy.create_engine(conn_url)

        # Save to PostgreSQL:
        with engine.begin() as conn:
            conn.execute("TRUNCATE integraciones.promociones") 
            df.to_sql(name="promociones",
                        con=conn,         
                        schema="integraciones",         
                        if_exists='append',         
                        index=False,         
                        chunksize=20000,         
                        method='multi')

        print("Data loaded to Postgres: integraciones.promociones")
        return "promos_postgres"
        
    except Exception as err:
        print(f"error: {err}")
        return "fallo_postgres_promos"

def stock_prices_promos_lss_to_s3(ti,ds,ts):
    import os
    import pandas as pd
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"re_factor_last_millers/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #traemos tiendas lss_millers
    ids_tiendas = ti.xcom_pull(key="return_value", task_ids=["get_last_millers_stores"])[0]
    ids_tiendas_lss_millers = [id[0] for id in ids_tiendas]
    print(ids_tiendas_lss_millers)

    #traemos tiendas activas
    query_tiendas_activas = """select id as id_tienda
                        from ecommdata.tiendas t
                        where t.status = 1
                        and id <> '9051'"""
    
    df_tiendas_activas_ecommerce = query_to_df(query_tiendas_activas)
    lista_tiendas_activas_ecommerce = df_tiendas_activas_ecommerce['id_tienda'].unique()
    print(lista_tiendas_activas_ecommerce)

    #traemos stock general
    query_stock = """select *
                    from integraciones.stock_2"""
    
    df_stock = query_to_df(query_stock)
    df_stock.columns = ["sku_key","material","id_tienda","stock"]

    #traemos productos general
    query_productos = """select *
                    from integraciones.productos_2"""
    
    df_productos = query_to_df(query_productos)
    df_productos.columns = ["sku_key","ean","umb","descripcion_producto","marca","umv"]
    df_productos['umv'] = df_productos['umv'].replace('ST', 'UN')

    #tiendas ecommerce en lss_millers

    #Convertimos las listas a conjuntos
    set_ids_tiendas_lss_millers = set(ids_tiendas_lss_millers)
    set_lista_tiendas_activas_ecommerce = set(lista_tiendas_activas_ecommerce)

    # Encontramos la intersección
    tiendas_ecom_lss_millers = set_lista_tiendas_activas_ecommerce.intersection(set_ids_tiendas_lss_millers)

    # Convertimos el resultado en lista si es necesario
    lista_tiendas_ecom_lss_millers = list(tiendas_ecom_lss_millers)
    print(lista_tiendas_ecom_lss_millers)

    #primera tramo - Ecommerce
    print("Primer Tramo Tiendas Ecommerce")
    query_stock_ecommerce = f"""with tiendas as(
                            select t.id 
                            from integraciones.tiendas_last_millers tlm 
                            left join ecommdata.tiendas t 
                            on tlm.id = t.id 
                            where t.status = 1
                        )
                        select distinct s.id_tienda, 
                        s2.ean_primario as "ean", 
                        s.material, 
                        split_part(s.ref_id, '-',2) as "unidad_de_medida",
                        s2.multiplicador_unidad_medida as "multiplicador_unidad",
                        s.descripcion as "nombre",
                        m.nombre as "marca",
                        s.stock_vtex as "stock_unitario"
                        from ecommdata.stock s 
                        left join ecommdata.skus s2 
                        on s2.ref_id = s.ref_id
                        left join ecommdata.productos p 
                        on p.ref_id = s.ref_id 
                        left join ecommdata.marcas m 
                        on p.id_marca = m.id
                        left join tiendas t
                        on s.id_tienda = t.id
                        where s.stock_janis > 0
                        and s.surtido_ecommerce is true 
                        and m.nombre is not null
                        and s2.ean_primario is not null
                        and s.id_tienda is not null
                        and s.material is not null
                        and s.descripcion is not null 
                        and s.ultima_actualizacion = (select max(ultima_actualizacion) from ecommdata.stock s3)
                        and s.c1 not in ('No Trabajar','Inactivos','Integración')
                        and t.id is not null"""
    df_stock_ecom = query_to_df(query_stock_ecommerce)
    print("\nMuestra stock ecommerce\n",df_stock_ecom.head())
    df_stock_ecom["ref_id"] = df_stock_ecom["material"]+"-"+df_stock_ecom["unidad_de_medida"]

    #Extramos precios
    query_precios = "select * from integraciones.precios"
    df_precios = query_to_df(query_precios)

    #Extramos promociones
    query_promociones = "select * from integraciones.promociones"
    df_promociones = query_to_df(query_promociones)
    df_promociones["ref_id"] = df_promociones["material"]+"-"+df_promociones["umv"]

    df_promos_min = df_promociones.groupby('ref_id')['precio_promocional'].min().reset_index()
    print(df_promos_min.head())

    #Merge con Precios a nivel tienda-sku
    df_lss_millers_ecom = df_stock_ecom.merge(df_precios, how = "left", on = ["id_tienda","ref_id"])

    #Merge con Promociones a nivel sku
    df_lss_millers_ecom = df_lss_millers_ecom.merge(df_promos_min, how = "left", on = ["ref_id"])

    #Eliminamos registros sin precio
    df_lss_millers_ecom = df_lss_millers_ecom.dropna(subset=["precio"])
    df_lss_millers_ecom.info()

    df_lss_millers_ecom = df_lss_millers_ecom[["id_tienda",
                                               "ean",
                                               "material_x",
                                               "unidad_de_medida",
                                               "multiplicador_unidad",
                                               "nombre",
                                               "marca",
                                               "stock_unitario",
                                               "precio",
                                               "precio_promocional"]]
    
    df_lss_millers_ecom.columns = ["id_tienda",
                                    "ean",
                                    "material",
                                    "unidad_de_medida",
                                    "multiplicador_unidad",
                                    "nombre",
                                    "marca",
                                    "stock_unitario",
                                    "precio",
                                    "precio_promocional"]

    print("\nInformación data tiendas Ecommerce: \n")
    df_lss_millers_ecom.info()

    #Data No Ecommerce
    #traemos ref_id's de lista8
    query_lista8 = """select distinct concat(material,'-',umv) as ref_id
                        from ecommdata.lista8"""
    
    df_lista8 = query_to_df(query_lista8)
    lista_ref_ids_lista8 = df_lista8['ref_id'].unique()

    #Convertimos las listas a conjuntos
    lista_tiendas_no_ecommerce = set_ids_tiendas_lss_millers-set_lista_tiendas_activas_ecommerce

    #filtro tiendas no Ecommerce
    df_stock = df_stock[df_stock['id_tienda'].isin(lista_tiendas_no_ecommerce)]

    #merge stock x productos
    df_lss_millers_no_ecom = df_stock.merge(df_productos, how = "left", on = ["sku_key"])

    #eliminar registros sin umv
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["umv"])

    #Crear ref_id
    df_lss_millers_no_ecom["ref_id"] = df_lss_millers_no_ecom["material"]+"-"+df_lss_millers_no_ecom["umv"]

    #preparamamos data precios, solo maximos por ref_id
    df_precios_max = df_precios.groupby('ref_id')['precio'].max().reset_index()
    print(df_precios_max.head())

    #merge con precios
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.merge(df_precios_max, how = "left", on = ["ref_id"])

    #merge con promociones
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.merge(df_promos_min, how = "left", on = ["ref_id"])

    #merge skus
    query_skus = """select ref_id, multiplicador_unidad_medida as multiplicador_unidad
                    from ecommdata.skus s """
    
    df_skus = query_to_df(query_skus)
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.merge(df_skus,how = "left", on = ["ref_id"])

    #limpieza de datos
    df_lss_millers_no_ecom = df_lss_millers_no_ecom[df_lss_millers_no_ecom["ref_id"].isin(lista_ref_ids_lista8)]

    df_lss_millers_no_ecom = df_lss_millers_no_ecom[["id_tienda",
                                                    "ean",
                                                    "material",
                                                    "umv",
                                                    "multiplicador_unidad",
                                                    "descripcion_producto",
                                                    "marca",
                                                    "stock",
                                                    "precio",
                                                    "precio_promocional"]]
    
    df_lss_millers_no_ecom.columns = ["id_tienda",
                                    "ean",
                                    "material",
                                    "unidad_de_medida",
                                    "multiplicador_unidad",
                                    "nombre",
                                    "marca",
                                    "stock_unitario",
                                    "precio",
                                    "precio_promocional"]

    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["ean"])
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["marca"])
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["precio"])
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["stock_unitario"])
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["nombre"])
    df_lss_millers_no_ecom = df_lss_millers_no_ecom.dropna(subset=["multiplicador_unidad"])
    
    #imprimir informacion del df no ecommerce
    print("\nInformacion DF No Ecommerce\n")
    print(df_lss_millers_no_ecom.head())
    df_lss_millers_no_ecom.info()

    #Traer registros de integraciones hijos
    query_dw_ph = """select split_part(s2.ref_id, '-', 1) as material
		, s.ou_id as id_tienda 
		, s.nbr_itm as stock_unitario
		, s2.ean_primario as ean
		, p.cont_conv_umb as multiplicador_unidad
		, s2.nombre_sku as nombre
		, p.brand_desc as trademark 
		, case when p.unidad_de_medida = 'ST' then 'UN' else p.unidad_de_medida end as unidad_de_medida
	from integraciones.stock_2 s 
	left join integraciones.productos_2 p 
		on p.sku_key = s.sku_key
	join ecommdata.skus s2 
		on s2.erp_id = s.sku_product
		and s2.erp_id::int8 <> split_part(s2.ref_id, '-', 1)::int8 
	where p.ean is not null 
		and p.cont_conv_umb is not null 
		and p.nm is not null 
		and p.brand_desc is not null 
		and p.unidad_de_medida is not null"""
    
    df_dw_ph = query_to_df(query_dw_ph)
    df_dw_ph["ref_id"] = df_dw_ph["material"]+"-"+df_dw_ph["unidad_de_medida"]

    #merge con precios
    df_lss_millers_no_ecom_ph = df_dw_ph.merge(df_precios_max, how = "left", on = ["ref_id"])

    #merge con promociones
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.merge(df_promos_min, how = "left", on = ["ref_id"])

    #limpieza de datos
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph[df_lss_millers_no_ecom_ph["ref_id"].isin(lista_ref_ids_lista8)]

    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph[["id_tienda",
                                                    "ean",
                                                    "material",
                                                    "unidad_de_medida",
                                                    "multiplicador_unidad",
                                                    "nombre",
                                                    "trademark",
                                                    "stock_unitario",
                                                    "precio",
                                                    "precio_promocional"]]
    
    df_lss_millers_no_ecom_ph.columns = ["id_tienda",
                                    "ean",
                                    "material",
                                    "unidad_de_medida",
                                    "multiplicador_unidad",
                                    "nombre",
                                    "marca",
                                    "stock_unitario",
                                    "precio",
                                    "precio_promocional"]
    
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["ean"])
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["marca"])
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["precio"])
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["stock_unitario"])
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["nombre"])
    df_lss_millers_no_ecom_ph = df_lss_millers_no_ecom_ph.dropna(subset=["multiplicador_unidad"])
    
    #imprimir informacion del df no ecommerce ph
    print("\nInformacion DF No Ecommerce ph\n")
    print(df_lss_millers_no_ecom_ph.head())
    df_lss_millers_no_ecom_ph.info()

    #quitar productos con categorias invalidas
    query_cat_invalidas= """select distinct p.ref_id 
                        from ecommdata.productos p 
                        left join ecommdata.categorias c 
                        on p.id_categoria = c.id
                        left join ecommdata.lista8 l 
                        on concat(l.material,'-',l.umv ) = p.ref_id 
                        where c.n1 in ('No Trabajar','Inactivos','Integración')
                        and l.material is not null;
                                                """
    df_categorias_invalidas = query_to_df(query_cat_invalidas)
    lista_ref_id_categorias_invalidas = df_categorias_invalidas['ref_id'].unique()
    print(lista_ref_id_categorias_invalidas)

    
    
    #concatemos los 3 dataframe de ecom, no ecom y no ecom ph
    df_final = pd.concat([df_lss_millers_no_ecom, df_lss_millers_ecom, df_lss_millers_no_ecom_ph], ignore_index=True)
    df_final = df_final[df_final["stock_unitario"]>0]
    df_final["ref_id"] = df_final["material"]+"-"+["unidad_de_medida"]
    df_final = df_final[~df_final["ref_id"].isin(lista_ref_id_categorias_invalidas)]
    df_final = df_final[["id_tienda",
                            "ean",
                            "material",
                            "unidad_de_medida",
                            "multiplicador_unidad",
                            "nombre",
                            "trademark",
                            "stock_unitario",
                            "precio",
                            "precio_promocional"]]
    df_final.drop_duplicates()


    #imprimir informacion del df final
    print("\nInformacion DF final\n")
    df_final.drop_duplicates(inplace=True)
    print(df_final.head())
    df_final.info()

    #envio a S3
    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"re_factor_last_millers/{exec_date}/re_factor_last_millers_{date_aux}.csv"
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

def stock_prices_promos_lss_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["stock_prices_promos_lss_to_s3"])[0]

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

    df['id_tienda'] = df['id_tienda'].apply(lambda x: str(x).zfill(4))
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))
    df['stock_unitario'] = df['stock_unitario'].astype(float).round(3)
    df['precio'] = df['precio'].astype(np.int32)
    df['precio_promocional'] = df['precio_promocional'].astype('Int32')
    print(df.head(20))
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE integraciones.refactor_lss_millers")
        df.to_sql(name="refactor_lss_millers",
                    con=conn,         
                    schema="integraciones",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def promos_postgres():
    print("todo bien con las promos")
    return

def precios_postgres():
    print("todo bien con los precios")
    return

def check_promos():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    query = "select * from integraciones.promociones"
    df = query_to_df(query)

    print(f"informacion obtenida de la Query: {df.info()}")

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        
        registros = len(df.index)

        try:
            response = client.files_upload(
                channels="last-millers-avisos",
                file=buffer,
                filename="integraciones_promociones.csv",
                title="Promociones LastMillers",
                initial_comment=f"se registrar {registros} en la tabla de promociones"
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return

def check_prices():

    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    query = "select * from integraciones.precios"
    df = query_to_df(query)

    print(f"informacion obtenida de la Query: {df.info()}")

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        
        registros = len(df.index)

        try:
            response = client.files_upload(
                channels="last-millers-avisos",
                file=buffer,
                filename="integraciones_precios.csv",
                title="Precios LastMillers",
                initial_comment=f"se registrar {registros} en la tabla de precios"
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return

def check_stock():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    query = "select * from integraciones.stock_2"
    df = query_to_df(query)

    print(f"informacion obtenida de la Query: {df.info()}")

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        
        registros = len(df.index)

        try:
            response = client.files_upload(
                channels="last-millers-avisos",
                file=buffer,
                filename="integraciones_stock.csv",
                title="Stock LastMillers",
                initial_comment=f"se registrar {registros} en la tabla de stock"
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return

def check_product():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    query = "select * from integraciones.productos_2"
    df = query_to_df(query)

    print(f"informacion obtenida de la Query: {df.info()}")

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        registros = len(df.index)
        
        try:
            response = client.files_upload(
                channels="last-millers-avisos",
                file=buffer,
                filename="integraciones_productos.csv",
                title="Productos LastMillers",
                initial_comment=f"se registrar {registros} en la tabla de productos"
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_prices_promos_last_millers',
    default_args=default_args,
    description="cargar stock,precios y promos a la tabla lss_millers_promos",
    schedule_interval="30 8,12,16,20 * * *",
    start_date=pendulum.datetime(2023, 6, 12, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "last_millers", "integraciones", "stock", "prices", "promos","PATRICIO"],
) as dag:
    

    dag.doc_md = """
    cargar stock,precios y promos a la tabla lss_millers_promos\n
    guardar en S3 y postgresql.
    """ 

    t_dummy_p = DummyOperator(
            task_id='fallo_dw_producto',
        )
    
    t_dummy_s = DummyOperator(
            task_id='fallo_dw_stock',
        )
    t_dummy_prom = DummyOperator(
            task_id='fallo_postgres_promos',
        )
    
    t_dummy_price = DummyOperator(
            task_id='fallo_postgres_precios',
        )
    
    t0  = PythonOperator(
        task_id = "get_last_millers_stores",
        python_callable = _get_last_millers_stores
    )

    t1 = BranchPythonOperator(
        task_id = "extract_stock_from_dw",
        python_callable = extract_stock_from_dw,
    )

    t2 = BranchPythonOperator(
        task_id = "extract_product_from_dw",
        python_callable = extract_product_from_dw,
    )

    t3 = PythonOperator(
        task_id = "stock_to_postgresql",
        python_callable = stock_to_postgresql,
    )
    t4 = PythonOperator(
        task_id = "product_to_postgresql",
        python_callable = product_to_postgresql,
    )
    t5 = BranchPythonOperator(
        task_id="prices_to_integrations_s3",
        python_callable=prices_to_integrations,
    )
    t6 = BranchPythonOperator(
        task_id="promos_to_integrations_s3",
        python_callable=promos_to_integrations,
    )
    t7 = PythonOperator(
        task_id="check_stock",
        python_callable=check_stock,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t8 = PythonOperator(
        task_id="check_product",
        python_callable=check_product,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t9 = PythonOperator(
        task_id="stock_prices_promos_lss_to_s3",
        python_callable=stock_prices_promos_lss_to_s3,
    )
    t10 = PythonOperator(
        task_id="stock_prices_promos_lss_to_postgres",
        python_callable=stock_prices_promos_lss_to_postgres,
    )
    t11 = PythonOperator(
        task_id="precios_postgres",
        python_callable=precios_postgres,
    )

    t12 = PythonOperator(
        task_id="check_prices",
        python_callable=check_prices,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )
    t13 = PythonOperator(
        task_id="promos_postgres",
        python_callable=promos_postgres,
    )
    t14 = PythonOperator(
        task_id="check_promos",
        python_callable=check_promos,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    t1 >>  t_dummy_s 
    t2 >>  t_dummy_p
    t0 >> [t1,t2,t5,t6]
    t1 >> t3
    t2 >> t4
    t3 >> t7 
    t_dummy_s >> t7
    t4 >> t8 
    t_dummy_p >> t8
    t5 >> t_dummy_price
    t5 >> t11
    t_dummy_price >> t12
    t11 >> t12
    t6 >> t_dummy_prom
    t6 >> t13
    t_dummy_prom >> t14
    t13 >> t14
    [t7,t8,t12,t14] >> t9
    t9 >> t10

    
