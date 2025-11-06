from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from utils.bigquery_utils import load_custom_bq_query_to_s3

from datetime import datetime, timedelta

import pendulum

def lista8():
    import pandas as pd
    lista8 = """select concat(l.material, '-', l.umv) as ref_id, id_tienda from ecommdata.lista8 l;"""
    print(lista8)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(lista8)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda"]
    print(results.head())
    cursor.close()
    pg_connection.close()
    return results

def _stopper_lista8(ts):
    import pandas as pd
    import re

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'CSV'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    query = """
       select id 
    from ecommdata.tiendas t
    where t.status = 1
    and t.id <> '1917';
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df = pd.DataFrame(results)
    df.columns = ["id_tienda"]
    active_stores = df["id_tienda"].unique()
    stores_found = s3_file_list
    stores_found = [i.split('.CSV', 1)[0] for i in stores_found]
    stores_found = [i.split('-')[3] for i in stores_found]
    print(f"active stores: {active_stores}")
    print(f"stores found: {stores_found}")
    tiendas_faltantes = set(active_stores)-set(stores_found)
    tiendas_faltantes_lista = list(tiendas_faltantes)
    
    if len(tiendas_faltantes_lista) == 0:
        return
    else:
        raise Exception(f"No se encontraron las siguientes tiendas: {tiendas_faltantes_lista}")

def _load_lista8(ts):
    import pandas as pd
    import sqlalchemy

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Files detected: {s3_file_list}")
    column_types = {
        "CENTRO": "str",
        "MATERIAL":	"str",
        "UM VTA":	"str",
        "NEGOCIO": "str",
        "SECCION": "str",
        "LINEA": "str",
        "CATEGORIA": "str",
        "GRUPO ARTICULO": "str",
        "PRECIO REGULAR": "int",
        "PRECIO PROMOCIONAL": "int",
        "DESCRIPCION": "str",
        "STOCK X UMV": "float",
        "SUSTITUTO": "bool",
        "BLOQ.CENTRO": "Int64",
        "BLOQ.FORMATO": "Int64",
        "CATALOGADO": "bool"
    }

    column_names = {
        "CENTRO": "id_tienda",
        "MATERIAL":	"material",
        "UM VTA":	"umv",
        "NEGOCIO": "negocio",
        "SECCION": "seccion",
        "LINEA": "linea",
        "CATEGORIA": "categoria",
        "GRUPO ARTICULO": "grupo_articulo",
        "PRECIO REGULAR": "precio_regular",
        "PRECIO PROMOCIONAL": "precio_promocional",
        "DESCRIPCION": "descripcion",
        "STOCK X UMV": "stock_x_umv",
        "SUSTITUTO": "sustituto",
        "BLOQ.CENTRO": "bloq_centro",
        "BLOQ.FORMATO": "bloq_formato",
        "CATALOGADO": "catalogado" 
    }

    dataframe_list = []
    for s3_file in s3_file_list:
        if not s3_file.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        print(f"Loading file: {s3_file}")
        lista8_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        df = pd.read_csv(lista8_object.get()["Body"], sep=";")
        df["STOCK X UMV"] = df["STOCK X UMV"].str.replace(',','.')
        df['SUSTITUTO'] = df['SUSTITUTO'].fillna('Y')
        df['SUSTITUTO'] = df['SUSTITUTO'].map({'X': True, 'Y': False})
        df["BLOQ.CENTRO"]  = pd.to_numeric(df["BLOQ.CENTRO"],  errors="coerce").astype("Int64")
        df["BLOQ.FORMATO"] = pd.to_numeric(df["BLOQ.FORMATO"], errors="coerce").astype("Int64")
        for col in ["CATALOGADO"]: # Asegura que las nuevas columnas sean booleanas y existan
            if col not in df.columns:
                df[col] = False 
            # Asegura que todo sea booleano (maneja posibles combinatorias o strings)
            df[col] = df[col].map({'X': True, 'Y': False, 
                                   1: True, 0: False, 
                                   '1': True, '0': False, 
                                   True: True, False: False, 
                                   'True': True, 'False': False,
                                   'SI': True, 'NO': False,
                                   'S': True, 'N': False})
            
            # Si quedaron NaN transformar (por si acaso)
            df[col] = df[col].fillna(False) # Asigna False a las otras columnas si es NaN

        df = df.astype(column_types)
        dataframe_list.append(df)
    df_full = pd.concat(dataframe_list, ignore_index=True)
    df_full = df_full.rename(columns=column_names)
    df_full["fecha"] = exec_date
    df_full["id_tienda"] = df_full["id_tienda"].str.zfill(4)
    df_full["material"] = df_full["material"].str.zfill(18)
    df_full["excluido"] = False

    # Drop duplicates
    df_full = df_full.drop_duplicates()
    print("Number of records to be loaded: "+str(len(df_full.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.lista8") 
        df_full.to_sql(name="lista8",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')
        conn.execute("""
            UPDATE ecommdata.lista8 l
            SET excluido = True
            FROM catalogo.productos_excluidos pe
            WHERE l.material = pe.material and l.umv = pe.umv
        """)
        conn.execute("""
            DELETE FROM ecommdata.lista8 l
            WHERE l.material in ('000000000000655232','000000000000671384','000000000000671581','000000000000671582','000000000000671583','000000000000671584','000000000000671585','000000000000671586','000000000000671587','000000000000671588','000000000000671589','000000000000671590','000000000000671591','000000000000671592','000000000000671593','000000000000671594','000000000000671595','000000000000671596','000000000000671646','000000000000671649','000000000000671650','000000000000671671','000000000000671672','000000000000671673','000000000000671674','000000000000671675','000000000000671676','000000000000671677','000000000000671678','000000000000671679','000000000000671680','000000000000671683','000000000000671753','000000000000671754','000000000000671755','000000000000671756','000000000000671757','000000000000671765','000000000000672059','000000000000672089','000000000000673021','000000000000673649','000000000000673650','000000000000673711','000000000000673712','000000000000674028','000000000000674029','000000000000674030','000000000000674031','000000000000674032','000000000000675333','000000000000675334','000000000000675353','000000000000675354','000000000000675355','000000000000675356','000000000000675357','000000000000675421','000000000000675738','000000000000675739','000000000000675740','000000000000675751','000000000000675752','000000000000676042','000000000000676043','000000000000676044','000000000000676045','000000000000676046','000000000673517002','000000000673517004')
                     """)
    print("Data saved to PostgreSQL. Table: ecommdata.lista8")

    return

def _load_lista9_filtered(ti):
    import pandas as pd
    import sqlalchemy

    #obtener archivo con xcom desde S3
    file_name = ti.xcom_pull(key="return_value", task_ids=["extract_data_from_dw"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    if  not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    s3_object = s3_hook.get_key(file_name, bucket_name=s3_bucket)
    df_dw = pd.read_csv(s3_object.get()["Body"])

    # Debe traer: ref_id, id_tienda
    df_dw.columns = [c.strip().lower() for c in df_dw.columns]
    if "ref_id" not in df_dw.columns or "id_tienda" not in df_dw.columns:
        raise Exception(f"CSV inválido; columnas: {list(df_dw.columns)} (se esperan 'ref_id' e 'id_tienda')")

    ventas = df_dw[["ref_id", "id_tienda"]].copy()
    ventas["ref_id"] = ventas["ref_id"].astype(str).str.strip()
    ventas["id_tienda"] = ventas["id_tienda"].astype(str).str.zfill(4)
    ventas = ventas.drop_duplicates()
    print(f"[DW] filas ventas únicas: {len(ventas)}")

    df_l8 = lista8()  
    df_l8["ref_id"] = df_l8["ref_id"].astype(str).str.strip()
    df_l8["id_tienda"] = df_l8["id_tienda"].astype(str).str.zfill(4)

    # Inner join en pandas SOLO para obtener el set de llaves válidas en lista8 que tienen venta
    df_full = df_l8.merge(ventas, on=["ref_id", "id_tienda"], how="inner").drop_duplicates()
    print(f"[JOIN-keys] llaves a traer completas desde lista8: {len(df_full)}")
    if df_full.empty:
        raise Exception("No hay llaves (ref_id, id_tienda) con venta presentes en lista8.")

    # Conexión y carga de datos a PostgreSQL
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.lista9") 
        df_full.to_sql(name="lista9",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.lista9")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_lista8_datastage_truncate_and_load',
    default_args=default_args,
    description="Carga de datos de lista8 desde bucket de S3 al workspace de Postgresql.",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 7, 3, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata", "lista8", "FRANCISCO"],
) as dag:

    dag.doc_md = """
    Extracción de archivos csv de lista8 desde bucket de S3, transformación y carga de datos en tabla ecommdata_unimarc.lista8. \n
    Un sensor espera por 1 hora la presencia de un archivo bandera (.TRG) que indique que la carga de los csv de datos está completa. \n
    Se realiza previamente un truncado de todos los datos y posteriormente se realiza la carga del día. \n
    Lista8 contiene todos los datos del surtido, por lo que se está filtrando para obtener solo los productos con ventas y cargarlos en [temporal_name]lista9.
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_flag_file",
        bucket_key = "datastage/L8/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/LISTA_8.TRG",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    )

    t1 = PythonOperator(
        task_id = "stopper_lista8",
        python_callable = _stopper_lista8
    )

    t2 = PythonOperator(
        task_id = "load_lista8",
        python_callable = _load_lista8
    )

    t3 = PythonOperator(
        task_id = "extract_data_from_dw",
        python_callable = load_custom_bq_query_to_s3,
        op_kwargs = {
            "query": """
                WITH venta_skus AS (
                SELECT (S.SKU_PRODUCT || '-' || CASE 
                        WHEN S.UMB = 'ST' THEN 'UN'
                        ELSE S.UMB
                    END) AS ref_id,
                    STORE_H.STORE_ID AS id_tienda,
                    SUM(COALESCE (CAST(VENTAC.VENTA_BRUTA as FLOAT64))) AS total_venta_bruta,
                    SUM(COALESCE (CAST(VENTAC.VENTA_NETA AS FLOAT64))) AS total_venta_neta,
                    SUM(COALESCE (CAST(VENTAC.VENTA_UMV AS FLOAT64))) AS total_unidades_vendidas
                    FROM cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_REGISTRO_VENTA_CONTABLE VENTAC
                        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY STORE_H 
                            ON STORE_H.STORE_KEY = VENTAC.STORE_KEY
                        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR S 
                            ON VENTAC.SKU_KEY = S.SKU_KEY
                        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT P 
                            ON VENTAC.PRODUCT_KEY = P.PRODUCT_KEY
                        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_UOM U 
                            ON P.UOM_VTA_KEY = U.UOM_KEY
                        LEFT JOIN cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_HIERARCHY SH 
                            ON VENTAC.SKU_KEY = SH.SKU_KEY
                    WHERE VENTAC.DATE_VALUE >= date_sub(current_date, INTERVAL 15 day)
                        AND STORE_H.ORG_IP_ID IN ('01')
                        AND VENTAC.CANAL_DISTRIB IN ('10')
                    GROUP BY 1,2
                    HAVING
                    COALESCE(SUM(CAST(VENTAC.VENTA_UMV AS FLOAT64))) > 0
                    OR COALESCE(SUM(CAST(VENTAC.VENTA_NETA AS FLOAT64))) > 0
                    OR COALESCE(SUM(CAST(VENTAC.VENTA_BRUTA AS FLOAT64))) > 0
                  )
                  SELECT DISTINCT ref_id, id_tienda 
                  FROM venta_skus;
            """,
            "query_name": "ecommdata/lista8_productos_con_ventas"
        },
        retries = 1,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60),
        pool = "backfill_pool"
    )

    t4 = PythonOperator(
        task_id = "filter_and_load_lista9",
        python_callable = _load_lista9_filtered
    )

    t0 >> t1 >> t2 >> t3 >> t4
