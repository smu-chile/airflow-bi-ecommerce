from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

import pendulum

from utils.postgres_utils import query_to_df
from utils.slack_utils import upload_bytes_to_slack, dag_success_slack, dag_failure_slack

def branch_8am(ts):
    exec_date = pendulum.parse(ts, tz="America/Santiago")
    hora = exec_date.hour
    print(f"Hora de ejecución: {hora}")
    if hora == 8:
        return "get_and_send_cargas_csv"
    else:
        return "skip_send"

def lista8(ds):
    promociones_query = f"""select distinct concat(material,'-',umv) as ref_id, id_tienda, fecha
                from ecommdata_alvi.lista8 l
                union
                SELECT 
                    distinct 
                    p.ref_id, 
                    t.id as id_tienda,
                    '{ds}'::date+1 as fecha
                FROM 
                    ecommdata_alvi.productos p
                CROSS JOIN 
                    (SELECT id 
                    FROM ecommdata_alvi.tiendas 
                    WHERE status = 1
                    and id <> '1') t
                WHERE 
                    p.ref_id IN (
                        '000000000099999999-UN', '000000000099999998-UN', '000000000099999997-UN',
                        '000000000099999996-UN', '000000000099999995-UN', '000000000000651290-UN',
                        '000000000000203455-UN', '000000000000198339-UN', '000000000000006552-UN',
                        '000000000000006558-UN', '000000000000141193-UN', '000000000000650419-UN',
                        '000000000000182334-UN', '000000000000209774-UN', '000000000000010526-UN',
                        '000000000000662165-UN', '000000000000817523-BOL', '000000000668910001-UN',
                        '000000000000010571-UN', '000000000000609686-UN', '000000000000651342-UN',
                        '000000000000005054-UN', '000000000000344299-UN', '000000000000192563-UN',
                        '000000000000941355-UN', '000000000000009228-UN', '000000000400308001-UN',
                        '000000000000023002-UN', '000000000000007518-UN', '000000000000006126-UN',
                        '000000000000006531-UN', '000000000000006519-UN', '000000000000132462-UN',
                        '000000000000141205-UN', '000000000000010517-UN', '000000000000010584-UN',
                        '000000000000647706-UN', '000000000666270001-UN', '000000000000024003-UN',
                        '000000000000652477-UN', '000000000000662077-UN', '000000000000671669-UN',
                        '000000000000211077-UN', '000000000000136746-UN', '000000000000956561-UN',
                        '000000000000807141-UN', '000000000000807140-UN', '000000000000009229-UN',
                        '000000000000008511-UN', '000000000000999999-UN', '000000000000999998-UN',
                        '000000000000999997-UN', '000000000000999996-UN', '000000000000999995-UN'
                    );"""
    results = query_to_df(promociones_query)
    results.columns = ["ref_id","id_tienda","fecha"]
    return results

def productos():
    productos_query = """select ref_id, nombre 
                    from ecommdata_alvi.productos"""
    results = query_to_df(productos_query)
    results.columns = ["ref_id","nombre_producto"]
    return results

def tiendas():
    tiendas_query = """select id, status, nombre_tienda_janis
                    from ecommdata_alvi.tiendas t 
                    where status = 1"""
    results = query_to_df(tiendas_query)
    results.columns = ["id_tienda","status","nombre_tienda_janis"]

    return results

def skus():
    skus_query = """select ref_id, nombre_sku
                    from ecommdata_alvi.skus"""
    results = query_to_df(skus_query)
    results.columns = ["ref_id","nombre_sku"]

    return results

def producto_tienda_janis():
    productos_tienda_query = """select ref_id, id_tienda, activo
                        from ecommdata_alvi.productos_tienda
                        where activo is true"""
    results = query_to_df(productos_tienda_query)
    results.columns = ["ref_id","id_tienda","activo"]
    results = results[["ref_id","id_tienda"]]

    return results

def excluidos_x_tiendas():
    excluidos_query = """select concat(material,'-',umv) as ref_id
                    from catalogo.productos_excluidos_alvi"""
    results = query_to_df(excluidos_query)
    results.columns = ["ref_id"]
    results = results[["ref_id"]]

    return results


def load_tables_to_s3(ts,ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"carga_tiendas_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_producto_tienda_janis = producto_tienda_janis()
    print(f"Ready productos por tienda en janis de hoy\n")
    df_lista_8 = lista8(ds)
    print(f"Ready lista8 de hoy\n")
    df_productos = productos()
    print("Ready productos\n")
    df_skus = skus()
    print("Ready skus\n")
    df_tiendas = tiendas()
    print("Ready tiendas activas\n")
    df_excluidos_x_tiendas = excluidos_x_tiendas()
    print("Ready excluidos_x_tiendas activas\n")

    df_productos_sin_skus = df_productos.merge(df_lista_8, on = ["ref_id"], how = 'left')
    df_skus_sin_producto = df_productos_sin_skus.merge(df_skus, on = ["ref_id"], how = 'left')
    df_skus_sin_producto = df_skus_sin_producto[(df_skus_sin_producto["id_tienda"].notna()) &
                                                (df_skus_sin_producto["nombre_sku"].isna())
                                                ].drop_duplicates(subset=['ref_id']).reset_index(drop=True)

    df_skus_sin_producto = df_skus_sin_producto[["ref_id"]]
    lista_skus_sin_producto = df_skus_sin_producto["ref_id"].to_list()

    #Activos
    #generamos los insumos de datos
    #productos activos por tiendas en janis
    print(f"\ncantidad de registros de productos por tiendas en janis: {len(df_producto_tienda_janis.index)}\n")
    df_productos_janis_tienda = df_producto_tienda_janis
    #lista8 con productos validos
    lista_productos = df_productos['ref_id'].unique()
    df_not_in_janis = df_lista_8[~df_lista_8['ref_id'].isin(lista_productos)]
    df_not_in_janis = df_not_in_janis[["ref_id"]]
    print(f"\ncantidad de registros en lista8 con productos no validos: {len(df_not_in_janis.index)}\n")
    #lista8
    df_lista8 = df_lista_8
    excluidos_x_tiendas_tiendas = df_excluidos_x_tiendas
    lista_excluidos = excluidos_x_tiendas_tiendas['ref_id'].unique()
    df_lista8 = df_lista8[~df_lista8['ref_id'].isin(lista_excluidos)]
    df_lista8 = df_lista8[["ref_id","id_tienda"]]
    print(f"\ncantidad de registros en lista8 con MFC: {len(df_lista8.index)}\n")
    #exclusiones con skus validos
    lista_skus = df_skus['ref_id'].unique()
    df_exclusions = excluidos_x_tiendas_tiendas[excluidos_x_tiendas_tiendas['ref_id'].isin(lista_skus)]
    df_exclusions = df_exclusions[["ref_id"]]
    print(f"\ncantidad de registros en excluidos con skus validos: {len(df_lista8.index)}\n")
    ##tiendas activcas
    df_tiendas = df_tiendas[["id_tienda"]]
    series_active_stores = df_tiendas['id_tienda'].unique()

    #transformacion de datos
    df_lista8 = df_lista8[df_lista8['id_tienda'].isin(series_active_stores)]

    df_deact = df_productos_janis_tienda.merge(df_lista8,how='left',on='ref_id')
    df_deact = df_deact[df_deact['id_tienda_y'].isna()]
    df_deact = pd.concat([df_exclusions, df_deact])

    series_deact = pd.Series(df_deact.loc[:,'ref_id'].unique())

    df = pd.concat([df_productos_janis_tienda, df_lista8])

    df = df.merge(df_not_in_janis,how='left',on='ref_id',indicator=True)
    df = df[df['_merge']!='both'][['ref_id','id_tienda']].reset_index(drop=True)
    
    df_gpby = df.groupby(list(df.columns))

    idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
    df_changes = df.reindex(idx)

    df_changes = df_changes.loc[~df_changes['ref_id'].isin(series_deact)]
    series_changes = pd.Series(df_changes['ref_id'].unique())

    df_lista8_changes = df_lista8.loc[df_lista8['ref_id'].isin(series_changes)]

    df_lista8_changes.loc[:,'idx'] = df_lista8_changes.groupby(['ref_id']).cumcount()
    df_changes_final = df_lista8_changes.pivot_table(index=['ref_id'], columns='idx', 
                        values=['id_tienda'], aggfunc='first')

    df_changes_final = df_changes_final.sort_index(axis=1, level=1)
    df_changes_final.columns = [f'{x}_{y}' for x,y in df_changes_final.columns]
    df_changes_final = df_changes_final.reset_index()

    cols = df_changes_final.filter(like='id_tienda_').columns

    df_changes_final['tiendas'] = df_changes_final[cols].agg(lambda s: s.dropna().str.cat(sep=','), axis=1)
    df_changes_final.drop(columns=cols, inplace=True)

    df_changes_final["publish"] = 1
    df_changes_final["visible"] = 1
    df_changes_final["updatePending"] = 1
    df_changes_final["active"] = 1
    df_changes_final.rename(columns={"ref_id":"refId","tiendas":"stores"}, inplace=True)
    df_changes_final["date"] = pd.to_datetime('today')

    #desactivados
    df_lista8_desactivar = df_lista8

    df_desactivados = (df_producto_tienda_janis.merge(df_lista8_desactivar, on=["ref_id","id_tienda"], how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge',axis= 1))

    print(f"\nRegistros a desactivar {len(df_desactivados.index)}\n")

    df_desactivados = df_desactivados[df_desactivados['id_tienda'].isin(series_active_stores)]
    print(f"\nfiltro por tienda inactivas: {len(df_desactivados.index)}\n")

    lista_skus_activos = df_changes_final['refId'].unique()
    df_desactivados = df_desactivados[~df_desactivados['ref_id'].isin(lista_skus_activos)]
    print(f"\nfiltro por skus activos: {len(df_desactivados.index)}\n")

    valores_unicos_skus = df_desactivados['ref_id'].unique()
    print(f"\nSkus unicos: {len(valores_unicos_skus)}")

    print("\n\nTodo Bien HASTA ACÄAA\n\n")

    df_excluidos = df_producto_tienda_janis.merge(excluidos_x_tiendas_tiendas, on=["ref_id"], how='inner')
    df_excluidos = df_excluidos[df_excluidos["id_tienda"]!= '3188']
    df_excluidos = df_excluidos[df_excluidos['id_tienda'].isin(series_active_stores)]
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
    df_desactivados_productos["stores"] = "3188"
    df_desactivados_productos["publish"] = 1
    df_desactivados_productos["updatePending"] = 1
    df_desactivados_productos["visible"] = 0
    df_desactivados_productos["active"] = 0

    df_changes_final = df_changes_final[["refId","stores","publish","updatePending","visible","active"]]
    df_final_skus = df_changes_final[["refId","publish","updatePending","active"]]
    df_final_productos = pd.concat([df_changes_final,df_desactivados_productos], axis=0)
    df_final_skus = pd.concat([df_final_skus,df_desactivados_sku], axis=0)
    df_final_skus = df_final_skus[~df_final_skus['refId'].isin(lista_skus_sin_producto)]

    buffer_1 = io.StringIO()
    df_final_productos.to_csv(buffer_1, header=True, index=False, encoding="utf-8")
    buffer_1.seek(0)
    
    buffer_2 = io.StringIO()
    df_final_skus.to_csv(buffer_2, header=True, index=False, encoding="utf-8")
    buffer_2.seek(0)

    filename_productos = f"carga_tiendas_alvi/{exec_date}/productos_{date_aux}.csv"
    filename_skus = f"carga_tiendas_alvi/{exec_date}/skus_{date_aux}.csv"

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
        raise Exception("Key %s products does not exist." % filename_productos)

    s_stock_object = s3_hook.get_key(filename_productos, bucket_name=s3_bucket)

    df_productos = pd.read_csv(s_stock_object.get()["Body"])
    if len(df_productos.index) == 0:
        print("There are no new nor updated products records to load. Task will exit as successfull.")
        return
    #skus
    print("Searching file: "+filename_skus)
    if not s3_hook.check_for_key(filename_skus, bucket_name=s3_bucket):
        raise Exception("Key %s skus does not exist." % filename_skus)

    s_stock_object = s3_hook.get_key(filename_skus, bucket_name=s3_bucket)

    df_skus = pd.read_csv(s_stock_object.get()["Body"])
    if len(df_skus.index) == 0:
        print("There are no new nor updated skus records to load. Task will exit as successfull.")
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
            conn.execute(f"TRUNCATE ecommdata_alvi.{names[i]}")
            df_lista[i].to_sql(name=names[i],
                        con=conn,         
                        schema="ecommdata_alvi",         
                        if_exists='append',         
                        index=False,         
                        chunksize=20000,         
                        method='multi')

        print("Data saved to PostgreSQL.")

    return

def get_and_send_cargas_csv():
    """
    Ejecuta 2 queries en Postgres (carga_productos y carga_skus)
    y sube 2 CSV separados a Slack.
    """
    import pandas as pd
    import io

    # conexiones / vars
    pg_hook   = PostgresHook(postgres_conn_id="postgresql_conn")
    engine    = pg_hook.get_sqlalchemy_engine()
    fecha_str = str(pendulum.now("America/Santiago").date())

    SQL_PRODUCTOS = """
        select CONCAT("refId",';',stores,';',publish,';',"updatePending",';',visible,';',active)
               as "refId;stores;publish;updatePending;visible;active"
        from ecommdata_alvi.carga_productos
    """
    SQL_SKUS = """
        select CONCAT("refId",';',publish,';',"updatePending",';',active)
               as "refId;publish;updatePending;active"
        from ecommdata_alvi.carga_skus
    """

    # ejecutar y exportar a CSV (separador coma; el contenido ya viene con ';' embebido)
    df_prod = pd.read_sql(SQL_PRODUCTOS, engine)
    df_skus = pd.read_sql(SQL_SKUS, engine)

    # si no hay filas, igual subimos un CSV con solo cabecera pa que quede trazabilidad
    buf_prod = io.StringIO()
    buf_skus = io.StringIO()
    df_prod.to_csv(buf_prod, index=False)  # header incluido
    df_skus.to_csv(buf_skus, index=False)

    # a bytes
    bytes_prod = buf_prod.getvalue().encode("utf-8")
    bytes_skus = buf_skus.getvalue().encode("utf-8")

    # nombres bonitos
    file_prod = f"carga_productos_{fecha_str}.csv"
    file_skus = f"carga_skus_{fecha_str}.csv"
    
    comment = "📎<!channel> [Unimarc] Ya se puede cargar {name}! :cat0:"

    upload_bytes_to_slack(
        file_name=file_prod,
        data_bytes=bytes_prod,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_prod),
    )

    upload_bytes_to_slack(
        file_name=file_skus,
        data_bytes=bytes_skus,
        channel_var_name="token_slack_carga_tiendas",
        initial_comment=comment.format(name=file_skus),
    )

    print(f"✅ CSVs enviados: {file_prod}, {file_skus}")


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_carga_tiendas_metabase_alvi',
    default_args=default_args,
    description="cargar tabla de productos y skus de carga tiendas",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "ecommdata", "metabase", "alvi", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    

    dag.doc_md = """
    Carga tabla productos y skus tiendas alvi\n
    guardar en S3.
    """ 

    t0 = ExternalTaskSensor(
        task_id="wait_lista8_alvi",
        external_dag_id='etl_lista8_alvi_datastage_truncate_and_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id = 'load_tables_to_s3',
        python_callable=load_tables_to_s3,
    )

    t2 = PythonOperator(
        task_id = "load_tables_to_postgres",
        python_callable = load_tables_to_postgres,
    )

    t3 = PythonOperator(
        task_id = "get_and_send_cargas_csv",
        python_callable = get_and_send_cargas_csv,
    )    

    t0 >> t1 >> t2 >> t3