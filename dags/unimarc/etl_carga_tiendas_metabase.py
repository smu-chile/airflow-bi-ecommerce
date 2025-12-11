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
from airflow.operators.python import get_current_context

from utils.postgres_utils import query_to_df

import pendulum


def branch_8am():
    ctx = get_current_context()

    # el "slot" que está corriendo
    end = ctx["data_interval_end"]  
    end_cl = end.in_timezone("America/Santiago")

    print(f"[BRANCH] start={ctx['data_interval_start']} end={end} | CL end={end_cl} | hour={end_cl.hour}")

    # si el slot es el de las 08:00 CL → manda alerta
    return "get_and_send_cargas_csv" if end_cl.hour == 8 else "skip_send"

    
def lista8():
    promociones_query = """select concat(l.material,'-',l.umv) as ref_id, l.id_tienda
                        from ecommdata.lista8 l
                        left join (select concat(sap_code,'-',measurement_unit) as ref_id, store as id_tienda 
                            from ecommdata.ubicacion_mfc um 
                            where mfc_is_item_side = 'REG') as ubi
                            on concat(l.material,'-',l.umv) = ubi.ref_id and l.id_tienda = ubi.id_tienda
                        where ubi.ref_id is null 
                        and not (
                            coalesce(l.bloq_centro,0) = 2
                            OR coalesce(l.bloq_formato,0) = 2
                            )
                        union
                        select distinct concat(l.material,'-',l.umv) as ref_id, '0053' as id_tienda
                        from ecommdata.lista8 l 
                        where not (
                            coalesce(l.bloq_centro,0) = 2
                            OR coalesce(l.bloq_formato,0) = 2
                            )
                        union
                        select distinct pc.ref_id, '0053' as id_tienda
                        from ecommdata.publicacion_catalogo pc
                        where pc.mfc is true
                        and pc.fecha_hora = (select max(fecha_hora) from ecommdata.publicacion_catalogo)
                        and pc.stock_janis > 0
                        union
                        select distinct pc.ref_id, '0398' as id_tienda
                        from ecommdata.publicacion_catalogo pc
                        where pc.mfc is true
                        and pc.fecha_hora = (select max(fecha_hora) from ecommdata.publicacion_catalogo)
                        and pc.stock_janis > 0
                        union 
                        select distinct concat(l.material,'-',l.umv) as ref_id, '0054' as id_tienda
                        from ecommdata.lista8 l where l.id_tienda in ('0469','0917','0581','0347','0336','0034')
                        AND NOT (
                            coalesce(l.bloq_centro,0) = 2
                            OR coalesce(l.bloq_formato,0) = 2
                        )
                        """
    results = query_to_df(promociones_query)
    results.columns = ["ref_id","id_tienda"]
    print(results.head())
    return results

def productos():
    productos_query = """select ref_id, nombre 
                    from ecommdata.productos"""
    results = query_to_df(productos_query)
    results.columns = ["ref_id","nombre_producto"]
    print(results.head())
    return results

def tiendas():
    import pandas as pd
    tiendas_query = """select id, status, nombre_tienda_janis
                    from ecommdata.tiendas t 
                    where status = 1"""
    results = query_to_df(tiendas_query)
    results.columns = ["id_tienda","status","nombre_tienda_janis"]
    return results

def skus():
    skus_query = """select ref_id, nombre_sku
                    from ecommdata.skus"""
    results = query_to_df(skus_query)
    results.columns = ["ref_id","nombre_sku"]
    return results

def producto_tienda_janis():
    productos_tienda_query = """select ref_id, id_tienda, activo
                        from ecommdata.productos_tienda"""
    results = query_to_df(productos_tienda_query)
    results.columns = ["ref_id","id_tienda","activo"]
    results = results[["ref_id","id_tienda"]]
    print(results.head())
    return results

def excluidos_x_tiendas():
    excluidos_query = """select ref_id,id_tienda,is_mfc,all_stores,fecha_carga
                    from ecommdata.producto_tienda_excluidos"""
    results = query_to_df(excluidos_query)
    results.columns = ["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]
    results = results[["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]]
    print(results.head())
    return results

def publicacion_1917_today(ts):
    import pandas as pd
    mfc_query = f"""select pc.ref_id, pc.id_tienda,
                    TO_CHAR(DATE_TRUNC('DAY', fecha_hora),'YYYY-MM-DD') AS fecha
                    from ecommdata.publicacion_catalogo pc
                    where pc.mfc is true
                    and pc.fecha_hora = (select max(fecha_hora) from ecommdata.publicacion_catalogo)
                    and pc.stock_janis > 0
                    ;"""
    results = query_to_df(mfc_query)
    if results.empty:
        print("There are no new nor updated records to load from MFC. Task will return an empty df.")
        return pd.DataFrame(columns=["ref_id", "id_tienda", "fecha"])
    results = pd.DataFrame(results)
    results.columns = ["ref_id","id_tienda","fecha",]
    results = results[["ref_id","id_tienda","fecha"]]
    print(results.head())
    return results

def aplicar_exclusiones_mfc(df_final_productos):
    import pandas as pd

    excl_query = """
        select ref_id as refId, id_tienda
        from catalogo.excluidos_carga_por_tienda
    """
    df_excl = query_to_df(excl_query)
    df_excl.columns = ["refId", "id_tienda"]

    if df_excl.empty:
        print("⚠️ aplicar_exclusiones_mfc: no hay filas en excluidos_carga_por_tienda")
        return df_final_productos

    df = df_final_productos.copy()

    # Solo nos preocupamos de los activos = 1
    df_activos = df[df["active"] == 1].copy()
    df_otros   = df[df["active"] != 1].copy()

    if df_activos.empty:
        print("⚠️ aplicar_exclusiones_mfc: no hay productos activos, no se aplica nada")
        return df

    df_activos["stores"] = df_activos["stores"].fillna("").astype(str)

    # separar stores en filas
    df_exp = df_activos.assign(store=df_activos["stores"].str.split(",")).explode("store")
    df_exp["store"] = df_exp["store"].str.strip()

    # cruzar con exclusiones
    df_exp = df_exp.merge(
        df_excl,
        how="left",
        left_on=["refId", "store"],
        right_on=["refId", "id_tienda"]
    )

    # nos quedamos SOLO con las combinaciones que NO están en la tabla de exclusión
    df_exp = df_exp[df_exp["id_tienda"].isna()]

    # rearmar la lista de tiendas
    df_group = (
        df_exp.groupby("refId")["store"]
        .apply(lambda x: ",".join([s for s in x if s]))  # sacar vacíos
        .reset_index()
    )

    # unir de vuelta con el resto de columnas de activos
    df_activos = df_activos.drop(columns=["stores"]).merge(df_group, on="refId", how="left")

    # si algún refId quedó sin tiendas → lo sacamos
    df_activos = df_activos[df_activos["store"].notna() & (df_activos["store"] != "")]
    df_activos = df_activos.rename(columns={"store": "stores"})

    # recomponer todo (activos filtrados + otros)
    df_final = pd.concat([df_activos, df_otros], axis=0).reset_index(drop=True)

    cols_order = ["refId", "stores", "publish", "updatePending", "visible", "active"]
    df_final = df_final[cols_order]

    print(f"aplicar_exclusiones_mfc: df_final_productos quedó con {len(df_final.index)} filas")
    return df_final

def load_tables_to_s3(ts,ds):
    import pandas as pd
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ts.replace("-", "_")
    prefix = f"carga_tiendas/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df_producto_tienda_janis = producto_tienda_janis()
    print(f"Ready productos por tienda en janis de hoy\n")
    df_lista_8 = lista8()
    print(f"Ready lista8 de hoy\n")
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
    #lista8+mfc
    df_lista8 = pd.concat([df_lista_8, df_publicacion_mfc_hoy], axis=0)
    excluidos_x_tiendas_tiendas = df_excluidos_x_tiendas[df_excluidos_x_tiendas["all_stores"]==1]
    lista_excluidos = excluidos_x_tiendas_tiendas['ref_id'].unique()
    df_lista8 = df_lista8[~df_lista8['ref_id'].isin(lista_excluidos)]
    df_lista8 = df_lista8[["ref_id","id_tienda"]]
    print(f"\ncantidad de registros en lista8 con MFC: {len(df_lista8.index)}\n")
    #exclusiones con skus validos
    lista_skus = df_skus['ref_id'].unique()
    df_exclusions = excluidos_x_tiendas_tiendas[excluidos_x_tiendas_tiendas['ref_id'].isin(lista_skus)]
    df_exclusions = df_exclusions[["ref_id"]]
    print(f"\ncantidad de registros en excluidos con skus validos: {len(df_exclusions.index)}\n")
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

    df_excluidos = df_producto_tienda_janis.merge(excluidos_x_tiendas_tiendas, on=["ref_id"], how='inner')
    df_excluidos = df_excluidos[df_excluidos["id_tienda_x"]!= '0486']
    df_excluidos = df_excluidos[df_excluidos['id_tienda_x'].isin(series_active_stores)]
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
    df_desactivados_productos["stores"] = "0486"
    df_desactivados_productos["publish"] = 1
    df_desactivados_productos["updatePending"] = 1
    df_desactivados_productos["visible"] = 0
    df_desactivados_productos["active"] = 0

    df_changes_final = df_changes_final[["refId","stores","publish","updatePending","visible","active"]]
    df_final_skus = df_changes_final[["refId","publish","updatePending","active"]]
    df_final_productos = pd.concat([df_changes_final,df_desactivados_productos], axis=0)
    df_final_skus = pd.concat([df_final_skus,df_desactivados_sku], axis=0)
    df_final_skus = df_final_skus[~df_final_skus['refId'].isin(lista_skus_sin_producto)]

    #lógica de excluir por tienda en carga_productos
    df_final_productos = aplicar_exclusiones_mfc(df_final_productos)


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

def _upload_to_slack(file_name: str, data_bytes: bytes, channel_id: str, token: str):
    import requests, json
    # 1) pedir URL de subida
    upload_url_resp = requests.post(
        "https://slack.com/api/files.getUploadURLExternal",
        data={
            "filename": file_name,
            "length": str(len(data_bytes)),
            "token": token,
        }
    ).json()
    upload_url = upload_url_resp.get("upload_url")
    file_id    = upload_url_resp.get("file_id")
    if not upload_url:
        raise RuntimeError(f"Error getUploadURLExternal: {upload_url_resp}")

    # 2) subir bytes
    up_resp = requests.post(
        upload_url,
        data=data_bytes,
        headers={"Content-Type": "application/octet-stream"}
    )
    if up_resp.status_code != 200:
        raise RuntimeError(f"Error subiendo {file_name}: {up_resp.text}")

    # 3) completar subida
    comp = requests.post(
        "https://slack.com/api/files.completeUploadExternal",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "files": [{"id": file_id}],
            "channel_id": channel_id,
            "initial_comment": f"📎<!channel> [Unimarc] Ya se puede cargar {file_name}! :cat0:"
        })
    ).json()
    if not comp.get("ok"):
        raise RuntimeError(f"Error completeUploadExternal {file_name}: {comp}")


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
    token     = Variable.get("token_slack_bot")
    channel_id = Variable.get("token_slack_carga_tiendas")  # mismo var que ya usas
    fecha_str = str(pendulum.now("America/Santiago").date())

    # queries tal cual las pediste
    SQL_PRODUCTOS = """
        select CONCAT("refId",';',stores,';',publish,';',"updatePending",';',visible,';',active)
               as "refId;stores;publish;updatePending;visible;active"
        from ecommdata.carga_productos
    """
    SQL_SKUS = """
        select CONCAT("refId",';',publish,';',"updatePending",';',active)
               as "refId;publish;updatePending;active"
        from ecommdata.carga_skus
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

    # subir a Slack
    _upload_to_slack(file_prod, bytes_prod, channel_id, token)
    _upload_to_slack(file_skus, bytes_skus, channel_id, token)

    print(f"✅ CSVs enviados: {file_prod}, {file_skus}")


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
    schedule_interval="0 1,4/4 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "ecommdata", "metabase", "unimarc", "PATRICIO"],
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
    
    t4 = PythonOperator(
        task_id = "get_and_send_cargas_csv",
        python_callable = get_and_send_cargas_csv,
    )
    
    t_b = BranchPythonOperator(
        task_id="branch_check_8am",
        python_callable=branch_8am,
    )
    
    t_end = DummyOperator(
        task_id="skip_send"
    )
    
    t0 >> t1 >> t2 >> t3 >> t_b
    t_b >> t4
    t_b >> t_end