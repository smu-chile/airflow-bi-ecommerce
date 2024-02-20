from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

def render_netezza_view():
    from io import StringIO
    import os
    import jaydebeapi
    import pandas as pd

    sql_str = "SELECT * FROM DWC_SMU.SMU.VW_DIM_SKU_ATTR"
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
    result = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(result, columns=column_names)
    df = df[['SKU_KEY','ALTURA','ANCHO','BRND_ID','CATEGORIA_MATERIAL_DESC',
             'CONDICION_DE_ALMACENAJE','CONTENIDO_BRUTO','CONTENIDO_NETO',
             'GDS_PD_TP_DSC','GRADO_ACLOHOLICO','LONGITUD','MARCA_PROPIA',
             'NUMERADOR_UMP','PAIS_ORIGEN_ID','PESO_NETO',
             'SKU_PRODUCT','UM_CONTENIDO','UMB','UNIDAD','UNIDAD_DE_MEDIDA_PEDIDO',
             'UNIDAD_DE_VOLUMEN','UNIDAD_LAA','UNIDAD_PESO','VOLUMEN','VIDA_UTIL']]
    cur.close()
    conn.close()

    return df

def render_netezza_view_2():
    from io import StringIO
    import os
    import jaydebeapi
    import pandas as pd

    sql_str = """SELECT
        J.SPL_RQS_DOC NroDocumento,
        CAST(SKU_PRODUCT AS NUMERIC(18,0)) PLU_SAP60,
        j.fecha_pedido FechaDocumento,
        Z.DATE_VALUE FechaEntrega,
        cast(I.OU_ID as varchar(4)) CD,
        cast(D.OU_ID as varchar(4)) Tienda,
        Posicion,
        Z.DATE_VALUE,
        sum(J.Pedido_umb) CanpedUMB,
        sum(J.Pedido_ump) Canped,
        Sum(J.Recibido_umb) CanrecUMB,
        Sum(J.Recibido_ump) Canrec,
        sum(RECIBIDO_A_TIEMPO_UMB) CanRecTiempoUMB,
        sum(RECIBIDO_A_TIEMPO_UMP) CanRecTiempo
        FROM DWC_SMU.SMU.VW_FACT_COMPRAS AS J
        INNER JOIN (
            select SPL_RQS_DOC,SKU_KEY,max(DATE_VALUE)DATE_VALUE
            from DWC_SMU.SMU.VW_FACT_COMPRAS_ESPERADO
            where cast(DATE_VALUE as date) >= current_date -120
            AND SKU_KEY NOT IN (4719571)
            group by SPL_RQS_DOC,SKU_KEY
            )Z 
        on J.SPL_RQS_DOC=Z.SPL_RQS_DOC AND J.SKU_KEY=Z.SKU_KEY
        LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT D ON J.OU_RECEP_KEY=D.OU_KEY --DIM_ORGANIZATION_UNIT
        LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_HIERARCHY E ON J.SKU_KEY=E.SKU_KEY --DIM_SKU_HIERARCHY
        LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT I ON J.OU_PROV_KEY=I.OU_KEY --DIM_ORGANIZATION_UNIT
        where D.OU_ID = '1917'
        AND Z.DATE_VALUE <= CURRENT_DATE 
        group by J.SPL_RQS_DOC,
        CAST(SKU_PRODUCT AS NUMERIC(18,0)),
        j.fecha_pedido,
        Z.DATE_VALUE,
        cast(I.OU_ID as varchar(4)) ,
        cast(D.OU_ID as varchar(4)) ,
        POSICION
        HAVING sum(J.Pedido_ump)>0"""
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
    result = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    df = pd.DataFrame(result, columns=column_names)
    print(df.columns)
    df = df[['PLU_SAP60','CD','DATE_VALUE','CANPEDUMB','CANRECUMB']]
    df.columns = ['material','CD','ultimo_recibido','cant_pedida','cant_recibida']
    cur.close()
    conn.close()

    return df

def productos_mfc(ds):
    import pandas as pd
    productos_mfc_query = f"""select pt.ref_id,
                p.material,
                msp.descripcion_sap,
                c.n1 as categoria_1,
                c.n2 as categoria_2,
                c.n3 as categoria_3,
                m.nombre as nombre_marca,
                msp.nombre_proveedor,
                msp.id_proveedor,
                split_part(pt.ref_id,'-',2) as umv,
                msp.ump,
                msp.peso_bruto,
                venta_dia.venta_dia
                from ecommdata.productos_tienda pt 
                left join ecommdata.productos p 
                on pt.ref_id = p.ref_id 
                left join ecommdata.categorias c 
                on p.id_categoria = c.id
                left join ecommdata.marcas m 
                on p.id_marca = m.id 
                left join ecommdata.maestra_sku_proveedor msp 
                on p.material = msp.material
                left join (select ref_id_sku as ref_id,
                        round(avg(venta_umv),2) as venta_dia
                        from ecommdata.ventas_ecommerce_datawarehouse ved
                        where id_tienda = '1917'
                        and fecha_facturacion >= '2023-12-01 00:00:00.000'::date --cambio poligonos MFC
                        and fecha_facturacion >= '{ds}'::date - 90
                        group by ref_id_sku
                    ) as venta_dia
                on venta_dia.ref_id = pt.ref_id
                where pt.id_tienda = '1917'
                and pt.activo is true"""
    print(productos_mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_mfc_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["ref_id","material","descripcion","categoria_1","categoria_2","categoria_3",
                       "marca","proveedor","id_proveer","umv","ump","peso_bruto","venta_diaria_90d"]
    cursor.close()
    pg_connection.close()
    return results

def sku_atributos_mfc():
    import pandas as pd
    productos_mfc_query = f"""select split_part(tom_id,'-',1) as material, *
from ecommdata.sku_atributos_mfc sam """
    print(productos_mfc_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_mfc_query)
    results = cursor.fetchall()
    results = pd.DataFrame(results)
    results.columns = ["material","ref_id","food_safety","temperature_zone","is_hazardous"]
    cursor.close()
    pg_connection.close()
    return results


def load_slotting_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"slotting/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    print("Empezando carga de fill_rate\n")
    df_fill_rate = render_netezza_view_2()
    print("Terminada carga de fillrate\n")
    print("Empezando carga de productos MFC\n")
    df_productos_mfc = productos_mfc(ds)
    print("Terminada carga de productos MFC\n")
    print("Empezando carga de atributos skus\n")
    df_atributos_skus = render_netezza_view()
    print("Terminada carga de atributos skus\n")
    df_sku_atributos_mfc = sku_atributos_mfc()
    df_atributos_skus.columns = ['sku_key', 'altura', 'ancho', 'brnd_id', 'categoria_material_desc',
                'condicion_de_almacenaje', 'contenido_bruto', 'contenido_neto',
                'gds_pd_tp_dsc', 'grado_alcoholico', 'longitud', 'marca_propia',
                'numerador_ump', 'pais_origen_id', 'peso_neto',
                'material', 'um_contenido', 'umb', 'unidad', 'unidad_de_medida_pedido',
                'unidad_de_volumen', 'unidad_laa', 'unidad_peso', 'volumen', 'vida_util']
    
    print(df_atributos_skus.head())
    print(df_productos_mfc.head())
    print(df_fill_rate.head())

    df_fill_rate_acum = df_fill_rate.groupby(['material','ultimo_recibido'])['cant_pedida','cant_recibida'].sum().reset_index()
    df_fill_rate_acum["fill_rate"] = df_fill_rate_acum["cant_recibida"]/df_fill_rate_acum["cant_pedida"]
    df_fill_rate_acum = df_fill_rate_acum[['material','ultimo_recibido','fill_rate']]
    df_fill_rate_acum['material'] = df_fill_rate_acum['material'].apply(lambda x: str(int(x)) if pd.to_numeric(x, errors='coerce') == x else np.nan)
    df_fill_rate_acum['material'] = df_fill_rate_acum['material'].apply(lambda x: x.zfill(18) if pd.notnull(x) else x)
    df_fill_rate_acum.info()
    
    df_slotting = df_productos_mfc.merge(df_atributos_skus, how='left', on="material")
    df_slotting = df_slotting.drop_duplicates(subset=['ref_id'])
    df_slotting = df_slotting.merge(df_fill_rate_acum, how='left', on="material")
    df_slotting = df_slotting.merge(df_sku_atributos_mfc, how='left', on="material")

    print(df_slotting.info())

    buffer = io.StringIO()
    df_slotting.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"slotting/{exec_date}/slotting_{date_aux}.csv"
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


def load_slotting_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["load_slotting_to_s3"])[0]

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

    df['material'] = df['material'].apply(lambda x: str(int(x)) if pd.to_numeric(x, errors='coerce') == x else np.nan)
    df['material'] = df['material'].apply(lambda x: x.zfill(18) if pd.notnull(x) else x)
    df['peso_bruto'] = df['peso_bruto'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['venta_diaria_90d'] = df['venta_diaria_90d'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['altura'] = df['altura'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['ancho'] = df['ancho'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['contenido_bruto'] = df['contenido_bruto'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['contenido_neto'] = df['contenido_neto'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['longitud'] = df['longitud'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['numerador_ump'] = df['numerador_ump'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['peso_neto'] = df['peso_neto'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['volumen'] = df['volumen'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df['vida_util'] = df['vida_util'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)
    df = df[['ref_id_x','material','descripcion','categoria_1','categoria_2','categoria_3','marca','proveedor','id_proveer','umv','ump','peso_bruto','venta_diaria_90d','sku_key','altura','ancho','brnd_id','categoria_material_desc','condicion_de_almacenaje','contenido_bruto','contenido_neto','gds_pd_tp_dsc','grado_alcoholico','longitud','marca_propia','numerador_ump','pais_origen_id','peso_neto','um_contenido','umb','unidad','unidad_de_medida_pedido','unidad_de_volumen','unidad_laa','unidad_peso','volumen','vida_util','ultimo_recibido','fill_rate','food_safety','temperature_zone','is_hazardous']]
    df.columns = ['ref_id','material','descripcion','categoria_1','categoria_2','categoria_3','marca','proveedor','id_proveer','umv','ump','peso_bruto','venta_diaria_90d','sku_key','altura','ancho','brnd_id','categoria_material_desc','condicion_de_almacenaje','contenido_bruto','contenido_neto','gds_pd_tp_dsc','grado_alcoholico','longitud','marca_propia','numerador_ump','pais_origen_id','peso_neto','um_contenido','umb','unidad','unidad_de_medida_pedido','unidad_de_volumen','unidad_laa','unidad_peso','volumen','vida_util','ultimo_recibido','fill_rate','food_safety','temperature_zone','is_hazardous']

    df['ultimo_recibido'] = pd.to_datetime(df['ultimo_recibido'])
    df_sorted = df.sort_values(by='ultimo_recibido', ascending=False)
    df_final = df_sorted.drop_duplicates(subset=df.columns.difference(['ultimo_recibido']), keep='first')

    print(df_final.info())
    print(df_final.head())

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.maestra_slotting")
        df_final.to_sql(name="maestra_slotting",
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
    'etl_maestra_informacion_slotting',
    default_args=default_args,
    description="cargar tabla slotting",
    schedule_interval= "30 10 * * *",
    start_date=pendulum.datetime(2023, 6, 14, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "ecommdata_unimarc", "slotting", "MFC", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Desde postgres carga la base de la tabla de productos mfc con venta promedio y carga desde DW atributos de los skus \n
    Insert diario.
    """ 

    t0 = PythonOperator(
        task_id = "load_slotting_to_s3",
        python_callable = load_slotting_to_s3,
    )

    t1 = PythonOperator(
        task_id = "load_slotting_to_postgres",
        python_callable = load_slotting_to_postgres,
    )


    t0 >> t1