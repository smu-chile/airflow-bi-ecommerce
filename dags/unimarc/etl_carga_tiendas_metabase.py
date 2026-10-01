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
from utils.slack_utils import upload_bytes_to_slack, dag_failure_slack, dag_success_slack

import pendulum

def branch_8am():
    ctx = get_current_context()

    # el "slot" que está corriendo
    end = ctx["data_interval_end"]  
    end_cl = end.in_timezone("America/Santiago")

    # logs (se quedan para verificar en el servidor)
    # Convertimos a pendulum para evitar el AttributeError
    logical_date = pendulum.instance(ctx["dag_run"].logical_date)
    logical_date_cl = logical_date.in_timezone("America/Santiago")
    is_manual = ctx['dag_run'].external_trigger
    
    print(f"[DEBUG_BRANCH_V3] end_cl={end_cl.hour} | logic_cl={logical_date_cl.hour} | logic_utc={logical_date.hour} | manual={is_manual}")

    # 1. Caso programado automático: Siempre a las 08:00 AM Chile (vía data_interval_end)
    if not is_manual and end_cl.hour == 8:
        return "get_and_send_cargas_csv"
    
    # 2. Caso manual (forzado): Si la fecha elegida (logical_date) es las 07:00 AM
    if is_manual and (logical_date_cl.hour == 7 or logical_date.hour == 7):
        return "get_and_send_cargas_csv"

    return "skip_send"
    
def lista8():
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS catalogo.productos_desbloqueados (
                    material VARCHAR(18) NOT NULL,
                    umv VARCHAR(10) DEFAULT 'UN',
                    id_tienda VARCHAR(4),
                    fecha_carga TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    motivo TEXT
                );
            """)
        conn.commit()

    # Obtenemos tiendas activas para el filtro estricto en SQL (excluyendo 0486)
    promociones_query = """
    WITH active_stores AS (
        SELECT id FROM ecommdata.tiendas WHERE status = 1 AND id != '0486'
    ),
    exceptions AS (
        SELECT material, umv, id_tienda FROM catalogo.productos_excluidos_excepciones
        UNION ALL
        SELECT material, umv, id_tienda FROM catalogo.productos_desbloqueados 
        WHERE id_tienda IS NOT NULL AND id_tienda NOT IN ('', 'ALL')
    ),
    global_desbloqueados AS (
        SELECT DISTINCT material, umv FROM catalogo.productos_desbloqueados 
        WHERE id_tienda IS NULL OR id_tienda IN ('', 'ALL')
    )
    SELECT ref_id, id_tienda FROM (
        -- 1. TIENDAS FISICAS (BASE ORIGINAL)
        select concat(l.material,'-',l.umv) as ref_id, l.id_tienda
        from ecommdata.lista8 l
        left join (select concat(sap_code,'-',measurement_unit) as ref_id, store as id_tienda 
                    from ecommdata.ubicacion_mfc um 
                    where mfc_is_item_side = 'REG') as ubi
                    on concat(l.material,'-',l.umv) = ubi.ref_id and l.id_tienda = ubi.id_tienda
        where (l.id_tienda = '1917' OR ubi.ref_id is null) 
        and l.id_tienda != '0486'
        -- Misión Original: Bypass de Excepciones ESTRICTO POR TIENDA
        and (
            l.excluido is not true 
            OR EXISTS (SELECT 1 FROM exceptions ex WHERE ex.material = l.material AND ex.umv = l.umv AND ex.id_tienda = l.id_tienda)
            OR EXISTS (SELECT 1 FROM global_desbloqueados gd WHERE gd.material = l.material AND gd.umv = l.umv)
        )
        and not (
            ((coalesce(l.bloq_centro,0) in (1,2,6,9) and l.linea not in ('ELECTRO'))
            OR (coalesce(l.bloq_formato,0) in (1,2,6,9) and l.linea not in ('ELECTRO')))
            AND concat(l.material, '-', l.umv) not in ('000000000000661989-UN', '000000000000661988-UN', '000000000000638773-UN')
            AND NOT EXISTS (SELECT 1 FROM exceptions ex WHERE ex.material = l.material AND ex.umv = l.umv AND ex.id_tienda = l.id_tienda)
            AND NOT EXISTS (SELECT 1 FROM global_desbloqueados gd WHERE gd.material = l.material AND gd.umv = l.umv)
            )
        
        union
        
        -- 2. TIENDA 0053 (MASTER)
        select distinct concat(l.material,'-',l.umv) as ref_id, '0053' as id_tienda
        from ecommdata.lista8 l 
        -- Misión Original: Bypass de Excepciones GENERAL (0053 siempre las tiene)
        where (
            l.excluido is not true 
            OR EXISTS (SELECT 1 FROM exceptions ex WHERE ex.material = l.material AND ex.umv = l.umv)
            OR EXISTS (SELECT 1 FROM global_desbloqueados gd WHERE gd.material = l.material AND gd.umv = l.umv)
        )
        and not (
            ((coalesce(l.bloq_centro,0) in (1,2,6,9) and l.linea not in ('ELECTRO'))
            OR (coalesce(l.bloq_formato,0) in (1,2,6,9) and l.linea not in ('ELECTRO')))
            AND concat(l.material, '-', l.umv) not in ('000000000000661989-UN', '000000000000661988-UN', '000000000000638773-UN')
            AND NOT EXISTS (SELECT 1 FROM exceptions ex WHERE ex.material = l.material AND ex.umv = l.umv)
            AND NOT EXISTS (SELECT 1 FROM global_desbloqueados gd WHERE gd.material = l.material AND gd.umv = l.umv)
            )
        
        union
        
        -- 3. TIENDAS MFC (0053 Y 0398)
        select distinct pc.ref_id, pc.id_tienda
        from ecommdata.publicacion_catalogo pc
        where pc.mfc is true
        and pc.id_tienda in ('0053', '0398')
        and pc.fecha_hora = (select max(fecha_hora) from ecommdata.publicacion_catalogo)
        and pc.stock_janis > 0
        
        union
        
        -- 4. TIENDA WEB 0054 (STRICT EXCLUSION, NO EXCEPTIONS)
        select distinct concat(l.material,'-',l.umv) as ref_id, '0054' as id_tienda
        from ecommdata.lista8 l 
        where l.id_tienda in ('0469','0917','0581','0347','0336','0034')
        AND l.excluido is not true
        -- Las excepciones manuales no deben ir a la tienda 0054
        AND NOT EXISTS (SELECT 1 FROM exceptions ex WHERE ex.material = l.material AND ex.umv = l.umv)
        AND NOT EXISTS (SELECT 1 FROM global_desbloqueados gd WHERE gd.material = l.material AND gd.umv = l.umv)
        AND NOT (
            ((coalesce(l.bloq_centro,0) in (1,2,6,9) and l.linea not in ('ELECTRO'))
            OR (coalesce(l.bloq_formato,0) in (1,2,6,9) and l.linea not in ('ELECTRO')))
            AND concat(l.material, '-', l.umv) not in ('000000000000661989-UN', '000000000000661988-UN', '000000000000638773-UN')
        )
    ) candidates
    -- Solo cargamos si la tienda existe en ecommdata.tiendas (status=1) y NO es la tienda basurero 0486
    WHERE candidates.id_tienda IN (SELECT id FROM active_stores)
      AND candidates.id_tienda != '0486'
    """
    results = query_to_df(promociones_query)
    import pandas as pd

    # --- INICIO REPLICA DE BUNDLES ESTATICOS ---
    # STATIC_BUNDLES = {
    #     '000000000000760476-UN': '000000000099999001-UN',
    #     '000000000663002001-UN': '000000000099999002-UN',
    #     '000000000000604342-UN': '000000000099999003-UN',
    #     '000000000000663001-UN': '000000000099999004-UN',
    #     '000000000663002002-UN': '000000000099999005-UN',
    #     '000000000000661863-UN': '000000000099999006-UN',
    #     '000000000000652451-UN': '000000000099999008-UN',
    #     '000000000653931001-UN': '000000000099999009-UN',
    #     '000000000653931002-UN': '000000000099999010-UN',
    #     '000000000000677826-UN': '000000000099999011-UN',
    #     '000000000000711482-UN': '000000000099999012-UN'
    # }
    # 
    # bundles_a_concatenar = []
    # 
    # # 1. Bundles simples 1 a 1
    # for ref_original, ref_bundle in STATIC_BUNDLES.items():
    #     df_bundle = results[results['ref_id'] == ref_original].copy()
    #     if not df_bundle.empty:
    #         df_bundle['ref_id'] = ref_bundle
    #         bundles_a_concatenar.append(df_bundle)
    #         
    # # 2. Bundle Surtido Costa (requiere que la tienda tenga TODOS los 6 componentes)
    # surtido_components = [
    #     '000000000000760476-UN',
    #     '000000000663002001-UN',
    #     '000000000000604342-UN',
    #     '000000000000663001-UN',
    #     '000000000663002002-UN',
    #     '000000000000661863-UN'
    # ]
    # df_surtido = results[results['ref_id'].isin(surtido_components)]
    # tiendas_con_todos = df_surtido.groupby('id_tienda').size()
    # # Filtramos solo las tiendas que tienen exactamente los 6 SKUs
    # tiendas_validas = tiendas_con_todos[tiendas_con_todos == len(surtido_components)].index.tolist()
    # 
    # if tiendas_validas:
    #     df_bundle_surtido = pd.DataFrame({
    #         'ref_id': '000000000099999007-UN',
    #         'id_tienda': tiendas_validas
    #     })
    #     bundles_a_concatenar.append(df_bundle_surtido)
    #     print(f"📦 BUNDLE SURTIDO COSTA: Se habilitaron {len(tiendas_validas)} tiendas que contienen los 6 componentes.")
    #
    # if bundles_a_concatenar:
    #     results = pd.concat([results] + bundles_a_concatenar, ignore_index=True)
    #     results = results.drop_duplicates()
    # --- FIN REPLICA DE BUNDLES ESTATICOS ---

    return results

def productos():
    productos_query = """
        SELECT DISTINCT ref_id 
        FROM ecommdata.productos_janis_api
        WHERE categoria_valida IS TRUE
    """
    results = query_to_df(productos_query)
    results.columns = ["ref_id"]
    print(f"✅ Productos válidos (categoría activa en Janis API): {len(results.index)}")
    return results

def get_skus_invalidos_a_apagar():
    """
    Retorna SKUs con categorías inválidas o inactivas (ej: 'No Trabajar', 'Inactivos', 'Fizzmod')
    NOTA: 'Integración' fue removida de la lista negra — se trata como categoría activa.
    que figuran activos o con tiendas operativas asignadas directamente en Janis API.
    Si ya están desactivados en Janis (activo=False y tiendas='0486'), NO se vuelven a enviar (delta=0).
    Los bundles se excluyen porque se gestionan por su propia lógica de componentes.
    """
    query = """
        SELECT DISTINCT p.ref_id 
        FROM ecommdata.productos_janis_api p
        WHERE COALESCE(p.categoria_valida, FALSE) IS FALSE
          AND (p.activo IS TRUE OR (p.tiendas IS NOT NULL AND p.tiendas != '' AND p.tiendas != '0486'))
          AND p.ref_id NOT IN (SELECT sku_bundle FROM ecommdata.sku_bundles_retornables WHERE active = true)
          AND p.ref_id NOT IN (SELECT ref_id_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true)
          AND p.ref_id NOT IN (SELECT skuid_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true);
    """
    try:
        results = query_to_df(query)
        results.columns = ["ref_id"]
        return results
    except Exception as e:
        print(f"Error querying get_skus_invalidos_a_apagar: {e}")
        import pandas as pd
        return pd.DataFrame(columns=["ref_id"])



def tiendas():
    import pandas as pd
    tiendas_query = """select id, status, nombre_tienda_janis
                    from ecommdata.tiendas t 
                    where status = 1 and id != '0486'"""
    results = query_to_df(tiendas_query)
    results.columns = ["id_tienda","status","nombre_tienda_janis"]
    return results

def skus():
    skus_query = """
        SELECT ref_id, nombre_producto AS nombre_sku
        FROM ecommdata.productos_janis_api
        WHERE COALESCE(es_huerfano, FALSE) IS FALSE
    """
    results = query_to_df(skus_query)
    results.columns = ["ref_id", "nombre_sku"]
    return results

def producto_tienda_janis():
    import gc
    query = """
        SELECT DISTINCT 
            p.ref_id, 
            lpad(trim(store_id), 4, '0') AS id_tienda
        FROM ecommdata.productos_janis_api p,
        LATERAL unnest(string_to_array(p.tiendas, ',')) AS store_id
        WHERE p.activo IS TRUE 
          AND p.tiendas IS NOT NULL 
          AND p.tiendas != '' 
          AND trim(store_id) != '0486'
          AND trim(store_id) != ''
          AND COALESCE(p.es_huerfano, FALSE) IS FALSE
    """
    results = query_to_df(query)
    gc.collect()
    print(f"✅ Productos por tienda reales cargados desde Janis API: {len(results.index)} combinaciones.")
    print(results.head())
    return results

def get_productos_huerfanos():
    """
    Retorna los ref_id de productos huérfanos (productos que no tienen SKU relacionado) directamente desde Janis API.
    Estos productos NUNCA pueden incluirse en carga_skus (tabla ni CSV) y DEBEN ser apagados
    obligatoriamente en carga_productos (active=0, visible=0, stores='0486') independiente de lista8.
    """
    query = """
        SELECT DISTINCT ref_id FROM ecommdata.productos_janis_api WHERE es_huerfano IS TRUE
    """
    try:
        results = query_to_df(query)
        results.columns = ["ref_id"]
        return results
    except Exception as e:
        print(f"Error querying get_productos_huerfanos: {e}")
        import pandas as pd
        return pd.DataFrame(columns=["ref_id"])





def excluidos_x_tiendas():
    excluidos_query = """select ref_id,id_tienda,is_mfc,all_stores,fecha_carga
                    from ecommdata.producto_tienda_excluidos"""
    results = query_to_df(excluidos_query)
    results.columns = ["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]
    results = results[["ref_id","id_tienda","is_mfc","all_stores","fecha_carga"]]
    print(results.head())
    return results

def get_bundles_retornables():
    query = """
        SELECT sku_original, sku_bundle 
        FROM ecommdata.sku_bundles_retornables 
        WHERE active = true
    """
    results = query_to_df(query)
    results.columns = ["sku_original", "sku_bundle"]
    return results

def get_bundles_dinamicos():
    query = """
        SELECT skuid_bundle, ref_id_bundle, skus_componentes 
        FROM ecommdata.sku_bundles_dinamicos 
        WHERE active = true
    """
    try:
        results = query_to_df(query)
        results.columns = ["skuid_bundle", "ref_id_bundle", "skus_componentes"]
        return results
    except Exception as e:
        print(f"Error querying sku_bundles_dinamicos: {e}")
        import pandas as pd
        return pd.DataFrame(columns=["skuid_bundle", "ref_id_bundle", "skus_componentes"])

def get_productos_janis_api_activos():
    query = """
        SELECT ref_id, tiendas 
        FROM ecommdata.productos_janis_api 
        WHERE activo IS TRUE
          AND tiendas IS NOT NULL 
          AND tiendas != '' 
          AND tiendas != '0486'
    """
    try:
        results = query_to_df(query)
        results.columns = ["ref_id", "tiendas"]
        # Filtrar en python para asegurar que no quede ninguna fila que solo tenga '0486'
        results = results[results["tiendas"].astype(str).str.strip() != "0486"].reset_index(drop=True)
        return results
    except Exception as e:
        print(f"Error querying ecommdata.productos_janis_api: {e}")
        import pandas as pd
        return pd.DataFrame(columns=["ref_id", "tiendas"])

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
    df_bundles_activos = get_bundles_retornables()
    print("Ready bundles retornables activos\n")

    # Identificar productos huérfanos (sin SKU asociado en Janis API o catálogo)
    df_huerfanos = get_productos_huerfanos()
    set_huerfanos = set(df_huerfanos['ref_id'].dropna().astype(str).unique()) if not df_huerfanos.empty else set()
    print(f"🛑 Total productos huérfanos (sin SKU) detectados: {len(set_huerfanos)}\n")

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
    lista_productos = set(df_productos['ref_id'].unique())
    df_not_in_janis = df_lista_8[~df_lista_8['ref_id'].isin(lista_productos)]
    df_not_in_janis = df_not_in_janis[["ref_id"]].drop_duplicates()
    print(f"\ncantidad de registros en lista8 con productos no validos (cat inactiva/no trabajar): {len(df_not_in_janis.index)}\n")
    #lista8+mfc
    df_lista8 = pd.concat([df_lista_8, df_publicacion_mfc_hoy], axis=0)
    # Filtro estricto: Solo productos válidos (con categoría activa y permitida) pueden estar en df_lista8
    df_lista8 = df_lista8[df_lista8['ref_id'].isin(lista_productos)].copy()
    
    # FILTRO TOTAL DE HUÉRFANOS DE LISTA8: Ningún producto huérfano puede ingresar como candidato de carga activa
    if set_huerfanos:
        df_lista8 = df_lista8[~df_lista8['ref_id'].isin(set_huerfanos)].copy()
        print(f"  -> Filtrados productos huérfanos de df_lista8. Quedan {len(df_lista8)} registros.")

    # Restauramos la definición para evitar el NameError
    excluidos_x_tiendas_tiendas = df_excluidos_x_tiendas[df_excluidos_x_tiendas["all_stores"]==1]
    # REMOVIDO: El filtro global por lista_excluidos ya no es necesario aquí 
    # porque la función lista8() ya filtra individualmente por tienda usando l.excluido.
    df_lista8 = df_lista8[["ref_id","id_tienda"]]
    print(f"\ncantidad de registros en lista8 con MFC (solo válidos): {len(df_lista8.index)}\n")
    #exclusiones con skus validos
    lista_skus = df_skus['ref_id'].unique()
    # Cambiamos a vacío para que no interfiera con excepciones en df_deact
    df_exclusions = pd.DataFrame(columns=['ref_id'])
    print(f"\ncantidad de registros en excluidos con skus validos: {len(df_exclusions.index)}\n")
    ##tiendas activcas
    df_tiendas = df_tiendas[["id_tienda"]]
    series_active_stores = [str(s) for s in df_tiendas['id_tienda'].unique() if str(s) != '0486']

    # transformacion de datos: Solo procesamos tiendas con status=1 en ecommdata.tiendas (excluyendo estrictamente la 0486)
    # Esto evita que tiendas que se "adelantaron" (status=0) generen deltas de carga
    # y evita que la tienda basurero 0486 sea cargada como tienda activa.
    df_lista8 = df_lista8[df_lista8['id_tienda'].isin(series_active_stores) & (df_lista8['id_tienda'] != '0486')]
    df_productos_janis_tienda = df_productos_janis_tienda[df_productos_janis_tienda['id_tienda'].isin(series_active_stores) & (df_productos_janis_tienda['id_tienda'] != '0486')]

    # OPTIMIZACION OOM: Hacemos el merge solo con valores unicos para no generar producto cartesiano infinito
    df_janis_unique = df_productos_janis_tienda[['ref_id']].drop_duplicates()
    df_lista8_unique = df_lista8[['ref_id']].drop_duplicates()
    df_lista8_unique['in_lista8'] = 1

    df_deact = df_janis_unique.merge(df_lista8_unique, how='left', on='ref_id')
    df_deact = df_deact[df_deact['in_lista8'].isna()][['ref_id']]
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

    # NOTA: Ya no se fuerzan bundle_originals ni componentes_dinamicos en series_changes
    # para evitar re-activaciones continuas innecesarias de bundles cuando sus tiendas no cambiaron.

    df_lista8_changes = df_lista8.loc[df_lista8['ref_id'].isin(series_changes)]

    df_lista8_changes.loc[:,'idx'] = df_lista8_changes.groupby(['ref_id']).cumcount()
    df_changes_final = df_lista8_changes.pivot_table(index=['ref_id'], columns='idx', 
                        values=['id_tienda'], aggfunc='first')

    df_changes_final = df_changes_final.sort_index(axis=1, level=1)
    df_changes_final.columns = [f'{x}_{y}' for x,y in df_changes_final.columns]
    df_changes_final = df_changes_final.reset_index()

    cols = df_changes_final.filter(like='id_tienda_').columns

    if df_changes_final.empty:
        df_changes_final['tiendas'] = pd.Series(dtype='str')
    else:
        df_changes_final['tiendas'] = df_changes_final[cols].agg(lambda s: s.dropna().str.cat(sep=','), axis=1)
        
    df_changes_final.drop(columns=cols, inplace=True, errors='ignore')

    df_changes_final["publish"] = 1
    df_changes_final["visible"] = 1
    df_changes_final["updatePending"] = 1
    df_changes_final["active"] = 1
    df_changes_final.rename(columns={"ref_id":"refId","tiendas":"stores"}, inplace=True)
    df_changes_final["date"] = pd.to_datetime('today')

    # --- FORZAR INVISIBILIDAD DE ENVASES ---
    envases_skus = ['000000000000167429-UN', '000000000000163603-UN']
    mask_envases = df_changes_final['refId'].isin(envases_skus)
    df_changes_final.loc[mask_envases, 'visible'] = 0

    # Estado actual en Janis API para comparar deltas de bundles de forma inteligente
    df_janis_api_activos = get_productos_janis_api_activos()
    janis_state_map = {}
    for _, r in df_janis_api_activos.iterrows():
        t_str = str(r['tiendas']) if pd.notna(r['tiendas']) else ''
        st_set = {s.strip() for s in t_str.split(',') if s.strip() and s.strip() != '0486'}
        janis_state_map[str(r['ref_id']).strip()] = st_set

    # Mapa de tiendas completas por SKU desde df_lista8 (que contiene todos los productos válidos y activos)
    stores_by_sku = df_lista8.groupby('ref_id')['id_tienda'].apply(set).to_dict()

    # --- LOGICA DELTA BUNDLES RETORNABLES ---
    retornables_a_cargar = []
    bundles_a_desactivar = []
    if not df_bundles_activos.empty:
        for _, row in df_bundles_activos.iterrows():
            sku_orig = str(row['sku_original']).strip()
            sku_bund = str(row['sku_bundle']).strip()
            
            # Tiendas objetivo del bundle según el producto original en lista8
            target_stores = stores_by_sku.get(sku_orig, set())
            janis_stores = janis_state_map.get(sku_bund)
            
            if target_stores:
                # Si Janis ya tiene el bundle activo con EXACTAMENTE las mismas tiendas, NO se envía
                if janis_stores is not None and janis_stores == target_stores:
                    pass
                else:
                    stores_str = ','.join(sorted(list(target_stores)))
                    retornables_a_cargar.append({
                        'refId': sku_bund,
                        'stores': stores_str,
                        'publish': 1,
                        'updatePending': 1,
                        'visible': 1,
                        'active': 1,
                        'date': pd.to_datetime('today')
                    })
            else:
                # Si el original no tiene tiendas y el bundle figura activo en Janis, se desactiva
                if janis_stores is not None and len(janis_stores) > 0:
                    bundles_a_desactivar.append({'ref_id': sku_bund})
            
            # El SKU original líquido siempre debe tener visible = 0 si está en df_changes_final
            mask_orig = df_changes_final['refId'] == sku_orig
            if mask_orig.any():
                df_changes_final.loc[mask_orig, 'visible'] = 0

        if retornables_a_cargar:
            df_new_ret = pd.DataFrame(retornables_a_cargar)
            print(f"📦 Bundles retornables con cambios reales a enviar: {len(df_new_ret)}")
            df_changes_final = pd.concat([df_changes_final, df_new_ret], ignore_index=True)
        else:
            print("📦 Bundles retornables: Todos en sincronía exacta con Janis. 0 enviados.")

    # --- LOGICA DELTA BUNDLES DINAMICOS ---
    df_bundles_din = get_bundles_dinamicos()
    dinamicos_a_cargar = []
    if not df_bundles_din.empty:
        import json
        for _, row in df_bundles_din.iterrows():
            skuid_bundle = str(row['skuid_bundle'])
            ref_id_bundle = row.get('ref_id_bundle')
            if pd.isna(ref_id_bundle) or not ref_id_bundle or str(ref_id_bundle).lower() in ['none', 'nan', 'null']:
                ref_id_bundle = skuid_bundle
            else:
                ref_id_bundle = str(ref_id_bundle).strip()
                
            if not ref_id_bundle or ref_id_bundle.lower() in ['none', 'nan', 'null']:
                continue
                
            componentes = row['skus_componentes']
            if isinstance(componentes, str):
                try:
                    componentes = json.loads(componentes)
                except:
                    componentes = []
            if not componentes or not isinstance(componentes, list):
                continue
                
            # Calculamos la intersección de tiendas usando el mapa completo de tiendas
            intersected_stores = None
            valid = True
            for comp in componentes:
                comp_str = str(comp).strip()
                comp_stores = stores_by_sku.get(comp_str, set())
                if not comp_stores:
                    valid = False
                    break
                if intersected_stores is None:
                    intersected_stores = set(comp_stores)
                else:
                    intersected_stores = intersected_stores.intersection(comp_stores)
                if not intersected_stores:
                    valid = False
                    break
                    
            janis_stores = janis_state_map.get(ref_id_bundle)
            
            if valid and intersected_stores:
                # Si Janis ya tiene el bundle activo con EXACTAMENTE las mismas tiendas, NO se envía
                if janis_stores is not None and janis_stores == intersected_stores:
                    pass
                else:
                    stores_str = ','.join(sorted(list(intersected_stores)))
                    dinamicos_a_cargar.append({
                        'refId': ref_id_bundle,
                        'stores': stores_str,
                        'publish': 1,
                        'updatePending': 1,
                        'visible': 1,
                        'active': 1,
                        'date': pd.to_datetime('today')
                    })
            else:
                # Si no tiene tiendas comunes y figura activo en Janis, se desactiva
                if janis_stores is not None and len(janis_stores) > 0:
                    bundles_a_desactivar.append({'ref_id': ref_id_bundle})
                    
        if dinamicos_a_cargar:
            df_nuevos_din = pd.DataFrame(dinamicos_a_cargar)
            print(f"📦 Bundles dinámicos con cambios reales a enviar: {len(df_nuevos_din)}")
            df_changes_final = pd.concat([df_changes_final, df_nuevos_din], ignore_index=True)
        else:
            print("📦 Bundles dinámicos: Todos en sincronía exacta con Janis. 0 enviados.")

    #desactivados
    df_lista8_desactivar = df_lista8

    df_desactivados = (df_producto_tienda_janis.merge(df_lista8_desactivar, on=["ref_id","id_tienda"], how='left', indicator=True)
        .query('_merge == "left_only"')
        .drop('_merge',axis= 1))

    print(f"\nRegistros a desactivar {len(df_desactivados.index)}\n")

    df_desactivados = df_desactivados[df_desactivados['id_tienda'].isin(series_active_stores)]
    print(f"\nfiltro por tienda inactivas: {len(df_desactivados.index)}\n")

    # APAGADO ESTRICTO DE CATEGORIAS INVALIDAS:
    # Todo producto en 'No Trabajar', 'Inactivos', 'Fizzmod', etc. que esté activo en Janis DEBE APAGARSE.
    # NOTA: 'Integración' ya NO está en la lista negra; esos productos se activan si están en lista8.
    df_skus_invalidos = get_skus_invalidos_a_apagar()
    if not df_skus_invalidos.empty:
        print(f"🛑 SKUs con categoría no trabajar/inválida a apagar: {len(df_skus_invalidos.index)}")
        df_desactivados = pd.concat([df_desactivados, df_skus_invalidos[['ref_id']]], ignore_index=True)

    # APAGADO ESTRICTO DE PRODUCTOS HUÉRFANOS (Sin SKUs relacionados en Janis API o catálogo):
    # Todo producto huérfano DEBE apagarse obligatoriamente en carga_productos (active=0, visible=0, stores='0486')
    if set_huerfanos:
        print(f"🛑 Añadiendo {len(set_huerfanos)} productos huérfanos a lista de desactivación de productos.")
        df_desactivados = pd.concat([df_desactivados, df_huerfanos[['ref_id']]], ignore_index=True)

    # EXCLUSIÓN DE BUNDLES DE TODA LA DESACTIVACIÓN (excepto si están en bundles_a_desactivar):
    # Los bundles son productos virtuales y NO existen en ecommdata.lista8 física.
    # Su desactivación se determina exclusivamente mediante 'bundles_a_desactivar' (cuando
    # sus componentes se quedan sin stock o sin tiendas válidas).
    bundle_skus_all = set()
    if not df_bundles_activos.empty:
        bundle_skus_all.update(set(df_bundles_activos['sku_bundle'].dropna().astype(str).unique()))
    if not df_bundles_din.empty:
        bundle_skus_all.update(set(df_bundles_din['ref_id_bundle'].dropna().astype(str).unique()))
        bundle_skus_all.update(set(df_bundles_din['skuid_bundle'].dropna().astype(str).unique()))

    if bundle_skus_all:
        df_desactivados = df_desactivados[~df_desactivados['ref_id'].isin(bundle_skus_all)].reset_index(drop=True)

    if bundles_a_desactivar:
        print(f"📦 Añadiendo {len(bundles_a_desactivar)} bundles a desactivar por falta de tiendas/stock.")
        df_desactivados = pd.concat([df_desactivados, pd.DataFrame(bundles_a_desactivar)], ignore_index=True)

    lista_skus_activos = df_changes_final['refId'].unique()
    df_desactivados = df_desactivados[~df_desactivados['ref_id'].isin(lista_skus_activos)]
    print(f"\nfiltro por skus activos: {len(df_desactivados.index)}\n")

    valores_unicos_skus = df_desactivados['ref_id'].unique()
    print(f"\nSkus unicos a desactivar: {len(valores_unicos_skus)}")

    # REMOVIDO: df_excluidos causaba deactivación global de productos con excepciones. 
    # El proceso de merge ya maneja las deactivaciones por tienda de forma individual.
    df_excluidos = pd.DataFrame(columns=["refId"])
    print("\ndf_excluidos: ",len(df_excluidos.index))

    # === RED DE SEGURIDAD / AUDITORÍA CON JANIS API ===
    # Detecta productos que en Janis figuran como activos (IsActive=True),
    # pero que NO existen en el universo COMPLETO de lista8 + MFC (no solo el delta diario).
    try:
        if not df_janis_api_activos.empty:
            # CORRECCIÓN CLAVE: Usar df_lista8 (que incluye tiendas físicas Y MFC 1917) más df_changes_final
            todos_ref_ids_lista8 = set(df_lista8['ref_id'].dropna().astype(str).unique())
            todos_ref_ids_lista8.update(set(lista_skus_activos))

            # Whitelist de bundles y envases
            envases_skus = {'000000000000167429-UN', '000000000000163603-UN'}
            whitelist_bundles = set(df_bundles_activos['sku_bundle'].dropna().astype(str)).union(
                set(df_bundles_activos['sku_original'].dropna().astype(str)),
                set(df_bundles_din['skuid_bundle'].dropna().astype(str)),
                set(df_bundles_din['ref_id_bundle'].dropna().astype(str)),
                envases_skus
            )

            # Bases normalizadas para proteger casos de ceros a la izquierda (ej: 17 vs 18 dígitos)
            bases_lista8 = {str(s).split('-')[0].lstrip('0') for s in todos_ref_ids_lista8 if '-' in str(s)}

            # Solo son huérfanos los que NO están en lista8 completa Y NO son bundles/envases
            candidatos_janis = df_janis_api_activos[
                (~df_janis_api_activos['ref_id'].isin(todos_ref_ids_lista8)) &
                (~df_janis_api_activos['ref_id'].isin(whitelist_bundles))
            ].copy()

            candidatos_janis = candidatos_janis[
                ~candidatos_janis['ref_id'].apply(lambda r: str(r).split('-')[0].lstrip('0') in bases_lista8 if '-' in str(r) else False)
            ]

            if not candidatos_janis.empty:
                print(f"\n[AUDITORÍA JANIS API] Se detectaron {len(candidatos_janis)} productos activos en Janis huérfanos/bloqueados (no existen en lista8 completa).")
                df_huerfanos_auditoria = pd.DataFrame({'ref_id': candidatos_janis['ref_id'].unique()})
                df_desactivados = pd.concat([df_desactivados, df_huerfanos_auditoria], ignore_index=True)
                df_desactivados = df_desactivados.drop_duplicates(subset=['ref_id']).reset_index(drop=True)
                # Candado estricto: Ningún producto que esté programado como activo en df_changes_final puede ser desactivado
                df_desactivados = df_desactivados[~df_desactivados['ref_id'].isin(lista_skus_activos)].reset_index(drop=True)
            else:
                print("\n[AUDITORÍA JANIS API] No se detectaron productos huérfanos en Janis.")
    except Exception as e:
        print(f"\n[AUDITORÍA JANIS API] Error al auditar productos huérfanos con Janis API: {e}")

    # Construcción de desactivados para SKUs:
    # REGLA ESTRICTA: Los productos huérfanos NO tienen SKU, jamás deben incluirse en carga_skus
    df_desactivados_sku = df_desactivados[["ref_id"]]
    df_desactivados_sku.columns = ["refId"]
    df_desactivados_sku = pd.concat([df_desactivados_sku, df_excluidos], axis=0)
    df_desactivados_sku = df_desactivados_sku.drop_duplicates(subset=['refId']).reset_index(drop=True)
    if set_huerfanos:
        df_desactivados_sku = df_desactivados_sku[~df_desactivados_sku['refId'].isin(set_huerfanos)].reset_index(drop=True)
    # Candado estricto: eliminar cualquier SKU activo
    df_desactivados_sku = df_desactivados_sku[~df_desactivados_sku['refId'].isin(lista_skus_activos)].reset_index(drop=True)
    df_desactivados_sku["publish"] = 1
    df_desactivados_sku["updatePending"] = 1
    df_desactivados_sku["active"] = 0

    # Construcción de desactivados para PRODUCTOS:
    # Aquí sí se incluyen los productos huérfanos para apagarlos a tienda 0486 en Janis
    df_desactivados_productos = df_desactivados[["ref_id"]]
    df_desactivados_productos.columns = ["refId"]
    df_desactivados_productos = pd.concat([df_desactivados_productos, df_excluidos], axis=0)
    # Solo incluir productos huérfanos si realmente existen en Janis API (para apagarlos)
    # Si no existen en Janis, no se deben enviar porque Janis daría error por entidad inexistente
    if set_huerfanos:
        set_huerfanos_en_janis = {ref for ref in set_huerfanos if ref in df_producto_tienda_janis['ref_id'].values}
        if set_huerfanos_en_janis:
            df_huerfanos_add = pd.DataFrame({'refId': list(set_huerfanos_en_janis)})
            df_desactivados_productos = pd.concat([df_desactivados_productos, df_huerfanos_add], ignore_index=True)
    df_desactivados_productos = df_desactivados_productos.drop_duplicates(subset=['refId']).reset_index(drop=True)
    # Candado estricto: eliminar cualquier producto activo
    df_desactivados_productos = df_desactivados_productos[~df_desactivados_productos['refId'].isin(lista_skus_activos)].reset_index(drop=True)
    df_desactivados_productos["stores"] = "0486"
    df_desactivados_productos["publish"] = 1
    df_desactivados_productos["updatePending"] = 1
    df_desactivados_productos["visible"] = 0
    df_desactivados_productos["active"] = 0

    # OPTIMIZACIÓN DELTA DESACTIVACIONES:
    # No volver a enviar a desactivar a 0486 productos que ya estén inactivos (activo=False)
    # y asignados exclusivamente a la tienda basurero '0486' (o sin tiendas) en Janis API.
    df_already_off_janis = query_to_df("""
        SELECT DISTINCT ref_id 
        FROM ecommdata.productos_janis_api 
        WHERE activo IS FALSE 
          AND (tiendas = '0486' OR tiendas IS NULL OR tiendas = '')
    """)
    if not df_already_off_janis.empty:
        set_already_off = set(df_already_off_janis['ref_id'].dropna().astype(str).unique())
        n_before_p = len(df_desactivados_productos)
        df_desactivados_productos = df_desactivados_productos[~df_desactivados_productos['refId'].isin(set_already_off)].reset_index(drop=True)
        df_desactivados_sku = df_desactivados_sku[~df_desactivados_sku['refId'].isin(set_already_off)].reset_index(drop=True)
        print(f"🛑 [DELTA DESACTIVACIONES] Se omitieron {n_before_p - len(df_desactivados_productos)} productos que YA están apagados en Janis (tienda 0486, activo=False).")

    df_changes_final = df_changes_final[["refId","stores","publish","updatePending","visible","active"]]
    df_final_skus = df_changes_final[["refId","publish","updatePending","active"]]
    df_final_productos = pd.concat([df_changes_final,df_desactivados_productos], axis=0)
    df_final_skus = pd.concat([df_final_skus,df_desactivados_sku], axis=0)
    df_final_skus = df_final_skus[~df_final_skus['refId'].isin(lista_skus_sin_producto)]
    if set_huerfanos:
        df_final_skus = df_final_skus[~df_final_skus['refId'].isin(set_huerfanos)].reset_index(drop=True)

    #lógica de excluir por tienda en carga_productos
    df_final_productos = aplicar_exclusiones_mfc(df_final_productos)

    # CANDADO DE SEGURIDAD 1: Asegurar que ningún producto activo (active = 1) tenga '0486' en su lista de tiendas
    mask_activos = df_final_productos['active'] == 1
    if mask_activos.any():
        df_final_productos.loc[mask_activos, 'stores'] = (
            df_final_productos.loc[mask_activos, 'stores']
            .fillna('')
            .astype(str)
            .apply(lambda s: ','.join([t.strip() for t in s.split(',') if t.strip() and t.strip() != '0486']))
        )
        mask_stores_vacios = mask_activos & (df_final_productos['stores'].fillna('').str.strip() == '')
        n_vacios = mask_stores_vacios.sum()
        if n_vacios > 0:
            print(f"[CANDADO 0486] ⚠️ Se eliminaron {n_vacios} filas activas que quedaron sin tiendas tras el strip de 0486.")
        df_final_productos = df_final_productos[~mask_stores_vacios].reset_index(drop=True)

    # CANDADO DE SEGURIDAD 2: Ningún producto con categoría inválida ('No Trabajar', etc.) puede quedar con active = 1
    if not df_skus_invalidos.empty:
        skus_invalidos_set = set(df_skus_invalidos['ref_id'].dropna().unique())
        mask_invalido_activo = (df_final_productos['active'] == 1) & (df_final_productos['refId'].isin(skus_invalidos_set))
        if mask_invalido_activo.any():
            n_inv = mask_invalido_activo.sum()
            print(f"[CANDADO NO TRABAJAR] 🚨 Corrigiendo {n_inv} productos en 'No Trabajar' que tenían active=1 -> apagando a 0486.")
            df_final_productos.loc[mask_invalido_activo, 'active'] = 0
            df_final_productos.loc[mask_invalido_activo, 'visible'] = 0
            df_final_productos.loc[mask_invalido_activo, 'stores'] = '0486'
        mask_skus_invalido_activo = (df_final_skus['active'] == 1) & (df_final_skus['refId'].isin(skus_invalidos_set))
        if mask_skus_invalido_activo.any():
            df_final_skus.loc[mask_skus_invalido_activo, 'active'] = 0

    # CANDADO DE SEGURIDAD 3: PRODUCTOS HUÉRFANOS (Sin SKUs relacionados)
    # 1. En df_final_productos: NINGÚN producto huérfano puede quedar activo ni con tiendas operativas
    if set_huerfanos:
        mask_huerfano_prod = df_final_productos['refId'].isin(set_huerfanos)
        if mask_huerfano_prod.any():
            df_final_productos.loc[mask_huerfano_prod, 'active'] = 0
            df_final_productos.loc[mask_huerfano_prod, 'visible'] = 0
            df_final_productos.loc[mask_huerfano_prod, 'stores'] = '0486'
        
        # 2. En df_final_skus: NINGÚN producto huérfano puede estar en df_final_skus (tabla ni CSV carga_skus)
        mask_huerfano_sku = df_final_skus['refId'].isin(set_huerfanos)
        if mask_huerfano_sku.any():
            n_del = mask_huerfano_sku.sum()
            print(f"[CANDADO HUÉRFANOS] 🛡️ Eliminando estrictamente {n_del} productos huérfanos de df_final_skus.")
            df_final_skus = df_final_skus[~mask_huerfano_sku].reset_index(drop=True)

    # CANDADO DE SEGURIDAD 4: VISIBILIDAD DE ENVASES Y BEBIDAS RETORNABLES ORIGINALES
    # NUNCA deben publicarse con visible = 1 (deben tener visible = 0 siempre para que el cliente solo vea el bundle).
    skus_invisibles = set(envases_skus)
    if not df_bundles_activos.empty:
        skus_invisibles.update(set(df_bundles_activos['sku_original'].dropna().astype(str).unique()))
    mask_invisibles = df_final_productos['refId'].isin(skus_invisibles)
    if mask_invisibles.any():
        df_final_productos.loc[mask_invisibles, 'visible'] = 0

    # CANDADO DE SEGURIDAD 5: VALIDACIÓN ESTRICTA DE EXISTENCIA EN JANIS API
    # Ningún refId puede entrar a carga_productos ni a carga_skus si NO existe en Janis API (o bundles activos).
    # Esto garantiza que el archivo exportado a Janis jamás falle por entidad inexistente.
    df_janis_api_existentes = query_to_df("SELECT DISTINCT ref_id FROM ecommdata.productos_janis_api")
    set_janis_validos = set(df_janis_api_existentes['ref_id'].dropna().astype(str).unique())
    if not df_bundles_activos.empty:
        set_janis_validos.update(set(df_bundles_activos['sku_bundle'].dropna().astype(str).unique()))
    if not df_bundles_din.empty:
        set_janis_validos.update(set(df_bundles_din['ref_id_bundle'].dropna().astype(str).unique()))
        set_janis_validos.update(set(df_bundles_din['skuid_bundle'].dropna().astype(str).unique()))

    # 1. En carga_productos: debe existir en Janis API
    mask_valido_producto = df_final_productos['refId'].isin(set_janis_validos)
    if (~mask_valido_producto).any():
        n_invalidos = (~mask_valido_producto).sum()
        refs_invalidos = df_final_productos.loc[~mask_valido_producto, 'refId'].tolist()
        print(f"[CANDADO PRODUCTOS] 🛡️ Eliminando {n_invalidos} registros de df_final_productos que NO existen en Janis API: {refs_invalidos}")
        df_final_productos = df_final_productos[mask_valido_producto].reset_index(drop=True)

    # 2. En carga_skus: debe existir en Janis API
    mask_valido_sku = df_final_skus['refId'].isin(set_janis_validos)
    if (~mask_valido_sku).any():
        n_invalidos_sku = (~mask_valido_sku).sum()
        refs_invalidos_sku = df_final_skus.loc[~mask_valido_sku, 'refId'].tolist()
        print(f"[CANDADO SKUS] 🛡️ Eliminando {n_invalidos_sku} registros de df_final_skus que NO existen en Janis API: {refs_invalidos_sku}")
        df_final_skus = df_final_skus[mask_valido_sku].reset_index(drop=True)

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
            conn.execute(f"""delete 
                            from ecommdata.{names[i]} 
                                where \"refId\" in (
                                    select ref_id 
                                    from catalogo.eliminados_carga_tiendas
                                    )
                            """)
            if names[i] == "carga_productos":
                conn.execute(text("""
                    DELETE FROM ecommdata.carga_productos
                    WHERE "refId" NOT IN (
                        SELECT ref_id FROM ecommdata.productos_janis_api
                        UNION
                        SELECT sku_bundle FROM ecommdata.sku_bundles_retornables WHERE active = true
                        UNION
                        SELECT ref_id_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true
                        UNION
                        SELECT skuid_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true
                    );
                """))
            if names[i] == "carga_skus":
                conn.execute(text("""
                    DELETE FROM ecommdata.carga_skus
                    WHERE "refId" NOT IN (
                        SELECT ref_id FROM ecommdata.productos_janis_api WHERE COALESCE(es_huerfano, FALSE) IS FALSE
                        UNION
                        SELECT sku_bundle FROM ecommdata.sku_bundles_retornables WHERE active = true
                        UNION
                        SELECT ref_id_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true
                        UNION
                        SELECT skuid_bundle FROM ecommdata.sku_bundles_dinamicos WHERE active = true
                    );
                """))

        print("Data saved to PostgreSQL.")

    return

def get_and_send_cargas_csv():
    """
    Ejecuta 2 queries en Postgres (carga_productos y carga_skus)
    y sube 2 CSV separados a Slack.
    """
    import pandas as pd
    import io

    pg_hook   = PostgresHook(postgres_conn_id="postgresql_conn")
    engine    = pg_hook.get_sqlalchemy_engine()
    fecha_str = str(pendulum.now("America/Santiago").date())

    SQL_PRODUCTOS = """
        SELECT "refId", stores, publish, "updatePending", visible, active
        FROM ecommdata.carga_productos
    """
    SQL_SKUS = """
        SELECT "refId", publish, "updatePending", active
        FROM ecommdata.carga_skus
    """

    # Ejecutar y exportar directamente a CSV con separador ';' nativo y limpio
    df_prod = pd.read_sql(SQL_PRODUCTOS, engine)
    df_skus = pd.read_sql(SQL_SKUS, engine)

    # Si no hay filas, igual subimos un CSV con solo cabecera para trazabilidad
    buf_prod = io.StringIO()
    buf_skus = io.StringIO()
    df_prod.to_csv(buf_prod, sep=";", index=False)
    df_skus.to_csv(buf_skus, sep=";", index=False)

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
    'etl_carga_tiendas_metabase',
    default_args=default_args,
    description="cargar tabla de productos y skus de carga tiendas",
    schedule_interval="0 1,4/4 * * *",
    start_date=pendulum.datetime(2023, 12, 6, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "tiendas", "ecommdata", "metabase", "unimarc", "PATRICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
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