from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
from airflow.operators.python import PythonOperator

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime
import pendulum

def check_update_attributes_products(ti):
    import time
    import pandas as pd
    from utils.postgres_utils import is_empty_table

    pg_hook = PostgresHook(conn_id="postgresql_conn")

    id_atributo_alergias = Variable.get(
        'JANIS_ATRIBUTOS_PRODUCTO_ID_ATT_ALERGIAS')
    id_atributo_sellos = Variable.get('JANIS_ATRIBUTOS_PRODUCTO_ID_ATT_SELLOS')

    query_alergias = """
        select s.ref_id as ref_id, concat_ws(',',
           CASE when ok.vegetarian = 1 then 'Vegetariano' ELSE NULL END,
            CASE when ok.vegan = 1 then 'Vegano' ELSE NULL END,
            CASE when ok.wheat_free = 1 then 'Libre de Trigo' ELSE NULL END,
            CASE when ok.sulphite_free = 1 then 'Libre de Sulfitos' ELSE NULL END,
            CASE when ok.soy_free = 1 then 'Libre de Soya' ELSE NULL END,
            CASE when ok.fish_free = 1 then 'Libre de Peces' ELSE NULL END,
            CASE when ok.walnuts_free = 1 then 'Libre de Nuez' ELSE NULL END,
            CASE when ok.seafood_free = 1 then 'Libre de Mariscos' ELSE NULL END,
            CASE when ok.peanut_free = 1 then 'Libre de Maní' ELSE NULL END,
            CASE when ok.lactose_free = 1 then 'Libre de Lactosa' ELSE NULL END,
            CASE when ok.egg_free = 1 then 'Libre de Huevo' ELSE NULL END,            
            CASE when ok.gluten_free = 1 then 'Libre de Gluten' ELSE NULL END, --CASE when ok.halal = 1 then 'Halal' ELSE NULL END, CASE when ok.kosher = 1 then 'Kosher' ELSE NULL END,
            CASE when ok.nuts_free = 1 then 'Libre de Frutos Secos' ELSE NULL END,
            CASE when ok.diabetes_suitable = 1 then 'Apto para Diabéticos' ELSE NULL END,
            CASE when ok.aplv_suitable = 1 then 'Apto para APLV' ELSE NULL END
            ) as Alergias 
        from catalogo.ok_to_shop_v2 oK
        left join ecommdata.sku_ean se on ok.product_ean::text = se.ean 
        left join ecommdata.skus s on s.ref_id = se.ref_id
        where (ok.aplv_suitable = 1
            or ok.gluten_free = 1 --or ok.halal = 1 or ok.kosher = 1
            or ok.lactose_free = 1
            or ok.vegan = 1
            or ok.vegetarian = 1
            or ok.diabetes_suitable = 1 
            or ok.soy_free = 1
            or ok.egg_free = 1
            or ok.fish_free = 1
            or ok.seafood_free = 1
            or ok.peanut_free = 1
            or ok.nuts_free = 1
            or ok.walnuts_free = 1
            or ok.sulphite_free = 1
            or ok.wheat_free = 1)
        and se.ean is not null;"""
    query_sellos = """
        select s.ref_id, concat_ws(',',
            CASE when ok.minsal_cl_high_sodium  = 1 then 'Alto en Sodio' ELSE NULL END,
            CASE when ok.minsal_cl_high_saturated_fat  = 1 then 'Alto en Grasas Saturadas' ELSE NULL END,
            CASE when ok.minsal_cl_high_calories = 1 then 'Alto en Calorías' ELSE NULL END,
            CASE when ok.minsal_cl_high_sugar = 1 then 'Alto en Azúcares' ELSE NULL END
            ) as sellos 
        from catalogo.ok_to_shop_v2 oK
        left join ecommdata.sku_ean se on ok.product_ean::text = se.ean 
        left join ecommdata.skus s on s.ref_id = se.ref_id
        where (ok.minsal_cl_high_sugar = 1 
            or ok.minsal_cl_high_sodium = 1 
            or ok.minsal_cl_high_saturated_fat = 1 
            or ok.minsal_cl_high_calories = 1 )
            AND se.ean is not null;"""
    query_alergias_atr_pro = F"""select ap.ref_id, 
        TRIM(TRAILING ',' FROM string_agg(ap.valor_atributo,',' ORDER BY ap.valor_atributo DESC)) as alergias
        from ecommdata.atributos_producto ap
        where ap.id_atributo = {id_atributo_alergias}
        group by ap.ref_id;"""
    query_sellos_atr_pro = f"""select ap.ref_id, 
        TRIM(TRAILING ',' FROM array_to_string(array_agg(ap.valor_atributo ORDER BY ap.valor_atributo DESC), ',')) AS sellos
        from ecommdata.atributos_producto ap
        where ap.id_atributo = {id_atributo_sellos}
        group by ap.ref_id;"""

    def get_atributos(query):  # atributos: alergias, sellos
        print(query)
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(query)
        pg_connection.commit()
        results = cursor.fetchall()
        columns_name = [i[0] for i in cursor.description]
        df = pd.DataFrame(results, columns=columns_name)
        print("Data obtenida")
        cursor.close()
        pg_connection.close()
        return df

    print("Iniciando obtencion de ok_to_shop_v2_alergias")
    df_alergias = get_atributos(query_alergias)
    print(df_alergias)
    print("Iniciando obtencion de ok_to_shop_v2_sellos")
    df_sellos = get_atributos(query_sellos)
    print(df_sellos)
    print("Iniciando obtencion de atributos_producto_alergias")
    i = 0
    while is_empty_table("ecommdata", "atributos_producto") == True:
        print("La tabla ecommdata.atributos_producto se encuentra vacia, se reintentará en 5 minumetos más")
        time.sleep(300)
        i += 1
        if i == 4:
            raise Exception(
                "No se encuentra disponible la tabla ecommdata.atributos_producto")
    df_alergias_atr = get_atributos(query_alergias_atr_pro)
    print(df_alergias_atr)
    print("Iniciando obtencion de atributos_producto_sellos")
    df_sellos_atr = get_atributos(query_sellos_atr_pro)
    print(df_sellos_atr)

    if df_alergias.equals(df_alergias_atr) and df_sellos.equals(df_sellos_atr):
        print("La data no presenta actualizaciones en cuanto a ALERGIAS")
        print("FINALIZADO")
        return []

    df_new_alergias = df_alergias[~df_alergias.isin(df_alergias_atr)].dropna()
    df_new_sellos = df_sellos[~df_sellos.isin(df_sellos_atr)].dropna()

    df_new_sellos['sellos'] = df_new_sellos['sellos'].apply(lambda x: 
        'Sin Sellos' if pd.isnull(x) else 
        x + ',Un Sello' if ',' not in x else 
        x + ',Dos Sellos' if x.count(',') == 1 else 
        x + ',Tres Sellos' if x.count(',') == 2 else 
        x + ',Cuatro Sellos')

    df_new_total = df_new_alergias.merge(
        df_new_sellos, on='ref_id', how='outer')
    print("Datos que se actualizarán")
    print(df_new_total)

    jst = []
    for index, row in df_new_total.iterrows():
        item = dict()
        item["item_id"] = row['ref_id']
        item["attributes"] = []
        if isinstance(row['alergias'], str):
            attributes = dict()
            attributes['id'] = Variable.get(
                "JANIS_API_REF_ID_ATTRIBUTE_ALERGIAS")
            attributes['values'] = row['alergias'].split(',')
            item["attributes"].append(attributes)
        if isinstance(row['sellos'], str):
            attributes = dict()
            attributes['id'] = Variable.get(
                "JANIS_API_REF_ID_ATTRIBUTE_SELLOS")
            attributes['values'] = row['sellos'].split(',')
            item["attributes"].append(attributes)
        jst.append(item)
    return jst


def set_janis_atributos(ti):
    import requests
    # Partición de big-json,
    API_JANIS = Variable.get("JANIS_API_URL")
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"}

    def set_attributes(jst):
        print("Iniciando carga a Janis")
        print(jst)
        lim_json = 500
        total_size = len(jst)
        if total_size > lim_json:
            jst = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
        else:
            jst = [jst]
        cargando = 0
        for arr_dic in jst:
            r = requests.post(f'{API_JANIS}attribute_value',
                              headers=headers, json=arr_dic)
            cargando += len(arr_dic)
            if r.status_code == 200:
                print(
                    f"Productos actualizados: {cargando} de {total_size} con EXITO")
            else:
                print(f"Carga sin éxito | Status_Code: {r.status_code} ")
                print(f"Response Print: {r.content}")

    json_data = ti.xcom_pull(task_ids=["check_update_attributes_products"])[0]
    if json_data == []:
        print("No hay atributos para cargar a JANIS, FINALIZADO")
        return
    json_clean = [{'item_id': x['item_id'], 'attributes': [
        {'id': atributo['id'], 'values': []} for atributo in x['attributes']]} for x in json_data]

    print("Inicia LIMPIADO de los valores del atributo en los productos a actualizar")
    set_attributes(json_clean)
    print("Inicia INSERTO de los valores del atributo en los productos a actualizar")
    set_attributes(json_data)
    print("La carga a FINALIZADO")


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_janis_attributes_product_ok_to_shop',
    default_args=default_args,
    description=""" With the extractions of .csv files from ftp connection, it's been made
    an update of catalogo.ok_to_shop_v2 table, and an insert of attributes of products that match EAN's 
    of sku_ean and skus using the API of Janis attribute_value. After this, we hope to observe
    atributos_producto table updated.""",

    schedule="0 10 * * *",
    start_date=pendulum.datetime(2023, 5, 21, tz="America/Santiago"),
    catchup=False,
    tags=["API", "Janis", "ok_to_shop_v2", 'atributos', 'atributos_producto', "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extraction and insert of attributes from ftp:ok_to_shop_v2 to Janis.
    """

    t0 = PythonOperator(
        task_id="check_update_attributes_products",
        python_callable=check_update_attributes_products,
    )
    t1 = PythonOperator(
        task_id="set_janis_atributos",
        python_callable=set_janis_atributos,
    )

t0 >> t1


