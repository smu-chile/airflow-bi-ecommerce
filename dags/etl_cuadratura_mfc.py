from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def stock_x_l8(ds):
    #esta funcion se consulta las tablas stock y lista8
    #stock filtrado por lista8 con fecha actual y tienda 1917
    # de stock se extrar Janis y de lista8 se extrar SAP
    #si el UMV del sku es KG o KGV se divide por multiplicador_unidad_medida para transformar el dato a unidades
    import pandas as pd
    print("se está extrayendo la info de stock y lista 8\n")
    stock_l8_query = f"""select s.fecha, pt.ref_id, pt.id_tienda,l.stock_x_umv ,s.stock_janis,
                    case
                        when (l.umv in ('DIS','CJ')) then trunc(l.stock_x_umv,0) 
                        when (l.umv in ('KG','KGV')) then round(l.stock_x_umv/s.multiplicador_unidad_medida,0)
                        else l.stock_x_umv 
                    end stock_sap,
                    s.multiplicador_unidad_medida
                    from ecommdata.productos_tienda pt
                    left join ecommdata.stock s 
                    on s.ref_id = pt.ref_id and s.id_tienda = pt.id_tienda
                    left join ecommdata.lista8 l 
                    on concat(l.material, '-', l.umv) = pt.ref_id and l.id_tienda  = pt.id_tienda 
                    where pt.id_tienda = '1917'
                    and pt.activo is true
                    and s.fecha = '{ds}'::date+1"""
    print(stock_l8_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_l8_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["fecha","ref_id","id_tienda","stock_l8","stock_janis","stock_calculado","multiplicador_medida"]
    cursor.close()
    pg_connection.close()
    return results

def sku_erp_padre():
    #esta funcion se consulta las tablas Skus, productos, categoria y productos tienda
    #condiciones pueden ser:
    #   -UMV sea KG o KGV
    #   -Categoria c1 sea 'Carnes'
    #   -erp_id sea distinto de material
    import pandas as pd
    sku_erp_query = """select s.erp_id,s.ref_id,s.nombre_sku,c.n1, pt.id_tienda
                    from ecommdata.skus as s
                    left join ecommdata.productos as p
                    on s.ref_id = p.ref_id
                    left join ecommdata.categorias as c
                    on p.id_categoria = c.id
                    left join ecommdata.productos_tienda as pt
                    on s.ref_id = pt.ref_id
                    where s.ref_id LIKE '%-KG' 
                    or s.ref_id LIKE '%-KGV'
                    or c.n1 = 'Carnes'
                    or p.material <> s.erp_id;"""
    print(sku_erp_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(sku_erp_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["material","ref_id","descripcion","categoria","id_tienda"]
    cursor.close()
    pg_connection.close()
    return results

def stock_mfc(ds):
    import pandas as pd
    stock_mfc_query = """SELECT id_tienda,
                    CONCAT(LPAD(material, 18, '0'), '-', unidad_venta) as ref_id,
                    stock as stock_janis,
                    fecha_carga
                    from ecommdata.stock_mfc
                    where fecha_carga = '"""+ds+"""'::date  +1
                    and id_tienda = '1917'"""
    print(stock_mfc_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["id_tienda","ref_id","stock_mfc","fecha_carga"]
    cursor.close()
    pg_connection.close()
    return results

def l8_0917(ds):
    import pandas as pd
    print("se está extrayendo información de lista8 para la 0917\n")
    l8_0917_query = """select material||'-'||umv as ref_id, stock_x_umv
                    from ecommdata.lista8
                    where fecha = '"""+ds+"""'::date +1
                    and id_tienda = '0917'"""
    print(l8_0917_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(l8_0917_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    print(results)
    results.columns = ["ref_id","stock_l8_0917"]
    cursor.close()
    pg_connection.close()
    return results


def ubicaciones_mfc(ds):
    import pandas as pd
    ubi_mfc_query = """select "_id",sap_code,ean_code,store,measurement_unit,mfc_is_item_side,created_date,update_date from ecommdata.ubicacion_mfc"""
    print(ubi_mfc_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ubi_mfc_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["_id","sap_code","ean_code","store","measurement_unit","mfc_is_item_side","created_date","update_date"]
    cursor.close()
    pg_connection.close()
    return results

def render_netezza_view(id_material,ds):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    import os

    # ----- 1) Cliente BigQuery -----
    sa_info = Variable.get("BIGQUERY_CREDENTIALS", deserialize_json=True)
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(project=sa_info["project_id"], credentials=creds)

    sql_str= """SELECT sa.SKU_PRODUCT AS material ,
                NBR_ITM AS stock ,
                ou.ou_id AS id_tienda ,
                SA.NM AS nombre ,
                DATE_VALUE as fecha
                FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_STOCK` S 
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR` SA
                ON SA.SKU_KEY = S.SKU_KEY 
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ORGANIZATION_UNIT` OU 
                ON OU.OU_KEY = S.OU_KEY 
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ALMACEN` A 
                ON A.ALMACEN_KEY =S.ALMACEN_KEY 
                LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PARTICULARIDAD` PART 
                ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY 
                WHERE A.ALMACEN_COD = '0001' 
                AND S.APLICA_STOCK = 'S' 
                AND DATE_VALUE = DATE(@ds) + INTERVAL 1 DAY
                AND OU.OU_ID IN ('1917','0917') 
                AND PART.PARTICULARIDAD_COD = 'A' 
                AND S.TIPO_STOCK_KEY = MD5('TIPOSTOCK^CL^SMC^')
                AND sa.SKU_PRODUCT IN UNNEST(@materiales);"""
    ####################################################################################################
    print(sql_str)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("ds", "DATE", ds),
            bigquery.ArrayQueryParameter("materiales", "STRING", id_material),
        ]
    )

    # ----- 4) Ejecuta y retorna DF -----
    job = client.query(sql_str, job_config=job_config)
    df = job.to_dataframe()

    # print opcionales de debug
    print("Filas retornadas:", len(df.index))
    print("Columnas:", list(df.columns))

    #######################################################################################################################################
    print(sql_str) 
    print(df.head())
    
    return df

def create_and_load_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"cuadratura_mfc/{exec_date}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_stock_l8_0917 = l8_0917(ds)
    print("se ha cargado stock janis y L8 de la tienda 0917\n")
    print(df_stock_l8_0917)
    df_stock_mfc = stock_mfc(ds)
    print("se ha cargado stock TOM de la tienda 1917\n")
    df_stock_l8_1917 = stock_x_l8(ds)
    df_stock_l8_1917.stock_janis = df_stock_l8_1917.stock_janis.fillna(0)
    print("se ha cargado stock janis y L8 de la tienda 1917\n")
    df_erp_padre = sku_erp_padre()
    print("se ha cargado erp padre hijo\n")
    df_orquestador = ubicaciones_mfc(ds)
    df_orquestador["sap_code"] = df_orquestador["sap_code"].astype("str", errors="ignore")
    df_orquestador["store"] = df_orquestador["store"].astype("str", errors="ignore")
    df_orquestador['sap_code'] = df_orquestador['sap_code'].apply(lambda x: str(x).zfill(18))
    df_orquestador["ref_id"] = df_orquestador["sap_code"]+"-"+df_orquestador["measurement_unit"]
    df_orquestador = df_orquestador[["ref_id","store","mfc_is_item_side"]]
    df_orquestador.columns = ["ref_id","id_tienda","mfc_is_item_side"]
    print("se ha cargado ubicaciones mfc\n")

    list_material = []
    list_tienda = []

    df_temp = df_erp_padre[df_erp_padre["categoria"] == 'Carnes']
    list_material = df_temp['material'].tolist()
    list_tienda = df_temp['id_tienda'].tolist()

    list_material = list(dict.fromkeys(list_material))
    list_tienda = list(dict.fromkeys(list_tienda))

    #list_tienda = ' '.join(list_tienda)
    #list_tienda = list_tienda.replace(" ", "','")
    #list_material = ' '.join(list_material)
    #list_material = list_material.replace(" ", "','")

    lista_dw = render_netezza_view(list_material,ds)
    df_aux = pd.DataFrame(lista_dw)
    df_aux.columns = ["material","stock","id_tienda","nombre","fecha"]

    print("se ha cargado stock DW\n")

    df_final = df_stock_l8_1917.merge(df_stock_mfc, how = "left", on = ["id_tienda","ref_id"])
    df_final = df_final[["fecha_carga","id_tienda","ref_id","stock_mfc","stock_l8","stock_calculado","stock_janis","multiplicador_medida"]]
    df_final = df_final.merge(df_orquestador, how = "left", on = ["id_tienda","ref_id"])

    df_final = df_final.merge(df_erp_padre, how = "left", on = ["id_tienda","ref_id"])
    df_final = df_final.merge(df_aux, how = "left", on = ["material","id_tienda"])
    df_final = df_final.drop_duplicates()
    df_final = df_final[["fecha_carga","id_tienda","ref_id","material","stock_mfc","stock_l8","stock_janis","stock_calculado","mfc_is_item_side","stock","multiplicador_medida"]]

    df_final['stock_calculado'] = pd.to_numeric(df_final['stock_calculado'],errors = 'coerce')
    df_final['multiplicador_medida'] = pd.to_numeric(df_final['multiplicador_medida'],errors = 'coerce')
    df_final['stock_calculado'] = pd.to_numeric(df_final['stock_calculado'],errors = 'coerce')

    condlist = [df_final["stock"].isnull() == False,
                df_final["stock"].isnull() == True]
    choicelist = [df_final["stock"]/df_final["multiplicador_medida"], df_final["stock_calculado"]]
    df_final["stock_calculado"] = np.select(condlist, choicelist)
    df_final["stock_calculado"] = round(df_final["stock_calculado"],0)

    df_final = df_final.merge(df_stock_l8_0917, how = "left", on = "ref_id")

    print(df_final.columns)

    df_final["stock_l8_0917"] = pd.to_numeric(df_final['stock_l8_0917'],errors = 'coerce')

    df_final["stock_l8_0917_calculado"] = round(df_final["stock_l8_0917"]/df_final["multiplicador_medida"],0)

    df_final = df_final[["ref_id","material","id_tienda","stock_mfc","stock_l8","stock_janis","stock_calculado","stock_l8_0917","stock_l8_0917_calculado","mfc_is_item_side","fecha_carga"]]

    df_final.columns = ["ref_id","erp_id","id_tienda","stock_mfc","stock_l8","stock_janis","stock_calculado","stock_l8_0917","stock_l8_0917_calculado","mfc_is_item_side","fecha"]

    df_final["ref_id"]= df_final["ref_id"].astype(str)
    df_final["id_tienda"]= df_final["id_tienda"].astype("str", errors="ignore")
    df_final["mfc_is_item_side"]= df_final["mfc_is_item_side"].astype("str", errors="ignore")
    df_final["stock_calculado"]= pd.to_numeric(df_final['stock_calculado'], errors='coerce')
    df_final["stock_calculado"].fillna(0, inplace=True)
    df_final["stock_calculado"] = df_final["stock_calculado"].astype(int)
    df_final["fecha"] = ds
    print(df_final)
    print(df_final.info())

    print("todo bien hasta acá")

    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"cuadratura_mfc/{exec_date}/cuadratura_{date_aux}.csv"
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


def truncate_and_load_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["create_and_load_s3"])[0]

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
    print(df.info())
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE catalogo.cuadratura_stock_mfc") 
        df.to_sql(name="cuadratura_stock_mfc",
                    con=conn,         
                    schema="catalogo",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: catalogo.cuadratura_stock_mfc")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_cuadratura_mfc',
    default_args=default_args,
    description="crear y cargar cuadratura del dia para MFC",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["catalogo", "cuadratura", "MFC", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    
    dag.doc_md = """
    construir y cargar cuadratura mfc. \n
    Upsert en tabla catalogo.cuadratura_mfc.
    """ 

    t0 = PythonOperator(
        task_id = "create_and_load_s3",
        python_callable = create_and_load_s3,
    )

    t1 = PythonOperator(
        task_id = "truncate_and_load_postgres",
        python_callable = truncate_and_load_postgres,
    )
    
    t0 >> t1