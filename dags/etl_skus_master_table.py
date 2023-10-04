from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def render_netezza_view(query):
    import jaydebeapi
    import os
    import pandas as pd

    sql_str= query
    
    print(sql_str)

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
    
    conn = jaydebeapi.connect(jdbc_driver_name, 
                                connection_string, {'user': dsn_uid, 'password': dsn_pwd},
                                jars=jdbc_driver_loc)

    cur = conn.cursor()
    cur.execute(sql_str)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    print(df)
    cur.close()
    conn.close()

    return df

def materiales_lista8():
    import pandas as pd
    stock_carnes_padre_hijo = """select distinct material
                    from ecommdata.lista8 l;"""
    print(stock_carnes_padre_hijo)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(stock_carnes_padre_hijo)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    print(results)
    results.columns = ["material"] 
    cursor.close()
    pg_connection.close()
    return results

def master_sku_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"master_skus_/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("\niniciando script\n")
   
    query_supp = "SELECT * FROM DWC_SMU.SMU.VW_DIM_SUPPLIER;"
    df_supp = render_netezza_view(query_supp)
    query_sku_hierarchy = " SELECT * FROM DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY;"
    df_sku_hierarchy = render_netezza_view(query_sku_hierarchy)
    df_lista8 = materiales_lista8()

    print("\nDescarga de datos lista\n")
    df_sku_hierarchy = df_sku_hierarchy[['PROVEEDOR_PPAL_KEY','SKU_PRODUCT','SKU_KEY','GRUPO_DSC','CAT_DSC','LIN_DESC','SEC_DSC','SKU_NM','NEG_DSC','BRAND_DESC','DESC_TIPO_MATERIAL','DESC_CATEGORIA_MATERIAL','PESO_NETO','PESO_BRUTO','UMP', 'MARCA_PROPIA']]
    df_aux = pd.merge(df_lista8, df_sku_hierarchy, left_on='material', right_on='SKU_PRODUCT', how = 'left').drop('SKU_PRODUCT', axis=1)
    df_aux = pd.merge(df_aux, df_supp, left_on='PROVEEDOR_PPAL_KEY', right_on='SUPPLIER_KEY', how = 'left').drop('SUPPLIER_KEY', axis=1)

    columns_varchar = [
    'material', 'GRUPO_DSC', 'CAT_DSC', 'LIN_DESC', 'SEC_DSC', 'SKU_NM', 
    'NEG_DSC', 'BRAND_DESC', 'DESC_TIPO_MATERIAL', 'DESC_CATEGORIA_MATERIAL', 
    'UMP', 'SUPPLIER_ID', 'SUPPLIER_NM', 'SUPPLIER_TYPE', 'SUPPLIER_CHANGE_DT', 'SUPPLIER_NK',
    'PROVEEDOR_PPAL_KEY', 'SUPPLIER_CHANGE_KEY'
    ]
    for col in columns_varchar:
        df_aux[col] = df_aux[col].astype(str)

    # Asegurándonos de que las cadenas no superen los 200 caracteres
    df_aux[columns_varchar] = df_aux[columns_varchar].applymap(lambda x: x[:200])

    # Cambiando a tipo float8 equivalente en pandas
    columns_float8 = [
        'PESO_NETO', 'PESO_BRUTO','AGIP', 'DUNHUMBY', 'FACTORING', 'NUESTRO_100',
        'SUPPLIER_NO_RETAIL','SUPPLIER_RETAIL','MARCA_PROPIA'
    ]
    for col in columns_float8:
        df_aux[col] = df_aux[col].astype(float)

    # Cambiando a tipo int8 equivalente en pandas
    df_aux.columns = df_aux.columns.str.lower()
    
    df_final = df_aux

    df_final = df_final[["material",
    "grupo_dsc",
    "cat_dsc",
    "lin_desc",
    "sec_dsc",
    "sku_nm",
    "neg_dsc",
    "brand_desc",
    "desc_tipo_material",
    "desc_categoria_material",
    "peso_neto",
    "peso_bruto",
    "ump",
    "supplier_id",
    "supplier_nm",
    "supplier_type",
    "supplier_retail",
    "nuestro_100",
    "marca_propia"]]

    df_final.columns = ["material",
    "grupo_sap",
    "categoria_sap",
    "linea_sap",
    "seccion_sap",
    "descripcion_sap",
    "negocio_sap",
    "marca",
    "tipo_material",
    "categoria_material",
    "peso_neto",
    "peso_bruto",
    "ump",
    "id_proveedor",
    "nombre_proveedor",
    "tipo_proveedor",
    "proveedor_retail",
    "nuestro_100",
    "marca_propia"]

    print(df_final.info())


    buffer = io.StringIO()
    df_final.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"master_skus_/{exec_date}/master_skus_{date_aux}.csv"
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


def master_sku_to_postgresq(ti):
    print("todo bien por acá")
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["master_sku_to_s3"])[0]

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
    print(df.info())
    df['proveedor_retail'] = pd.to_numeric(df['proveedor_retail'], errors='coerce').astype('Int64')
    df['nuestro_100'] = pd.to_numeric(df['nuestro_100'], errors='coerce').astype('Int64')
    df['marca_propia'] = df['marca_propia'].astype(int)
    df['material'] = df['material'].apply(lambda x: str(x).zfill(18))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.maestra_sku_proveedor")
        df.to_sql(name="maestra_sku_proveedor",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return
    return 

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_skus_master_table',
    default_args=default_args,
    description="cargar maestra skus",
    schedule_interval= "0 9 * * *",
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata", "maestra_skus","proveedores"],
) as dag:
    

    dag.doc_md = """
    generar dataframe a partir de vistas en DW, guardarlos en s3 y cargar la vista del dia en postgesql. \n
    Truncate and load diario.
    """ 

    t0 = PythonOperator(
        task_id = "master_sku_to_s3",
        python_callable = master_sku_to_s3,
    )

    t1 = PythonOperator(
        task_id = "master_sku_to_postgresq",
        python_callable = master_sku_to_postgresq,
    )

    t0 >> t1