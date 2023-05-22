from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from datetime import datetime
import pendulum


def last_file_ok_to_shop(ti):
    import io
    import ftplib
    import pandas as pd
    import zipfile
    import numpy as np
    from sqlalchemy import text

    from datetime import datetime

    date_dir = datetime.now().strftime("/%Y/%m/")
    date_name = datetime.now().strftime("%Y%m%d")
    # file_dir = "/2023/05/20230520_ok_to_shop.zip"
    file_dir = date_dir+date_name+"_ok_to_shop.zip"
    print(f"Checking file: {file_dir}")
    df = pd.DataFrame()
    # Establecer la conexión FTP
    ip_ftp_ok_to_shop = Variable.get(
        "JANIS_OK_TO_SHOP_ATRIBUTOS_PRODUCTOS_IP_FTP")
    password_ftp_ok_to_shop = Variable.get(
        "JANIS_OK_TO_SHOP_ATRIBUTOS_PRODUCTOS_PASSWORD_FTP")
    with ftplib.FTP(ip_ftp_ok_to_shop) as ftp:
        ftp.login(user='unimarc_cl@okto.shop', passwd=password_ftp_ok_to_shop)
        print("Adentro de FTP")
        if file_dir in ftp.nlst(date_dir):
            with io.BytesIO() as zip_buffer:  # file_dir = '/2023/02/20230209_ok_to_shop.zip'
                ftp.retrbinary('RETR ' + file_dir, zip_buffer.write)
                zip_buffer.seek(0)
                print("en bytesIO")
                if zipfile.is_zipfile(zip_buffer):
                    print(f"Dentro del archivo {file_dir}")
                    with zipfile.ZipFile(zip_buffer, mode='r') as myzip:
                        if len(myzip.namelist()) != 0:  # la lista está vacía dentro de .zip
                            csv_file_name = myzip.namelist()[0]
                            with myzip.open(csv_file_name, 'r') as f:
                                df = pd.read_csv(
                                    f, encoding='utf-8', engine='python', sep=';', on_bad_lines='skip')  # warn
                        else:
                            print(file_dir, ": El archivo zip está vacio")
                else:
                    print(file_dir, ": No es archivo ZIP")
        else:
            print("Archivo no existe en el directorio")
        ftp.quit()
    print("Out of ftp server")
    if not df.empty:
        columns = ['product_ean', 'timestamp_in', 'date_in',
                   'last_update', 'date_last_update', 'brand_name', 'description',
                   'flavor', 'size_value', 'drained_size_value', 'size_unit',
                   'ingredients', 'allergens', 'traces', 'has_nutritional_table',
                   'portion_text', 'portion_value', 'portion_unit', 'num_portions',
                   'basic_unit', 'energy_value', 'energy_unit', 'protein_value',
                   'protein_unit', 'fat_total_value', 'fat_total_unit', 'fat_sat_value',
                   'fat_sat_unit', 'fat_mono_value', 'fat_mono_unit', 'fat_poli_value',
                   'fat_poli_unit', 'fat_trans_value', 'fat_trans_unit',
                   'fat_cholesterol_value', 'fat_cholesterol_unit', 'carb_value',
                   'carb_unit', 'sugars_value', 'sugars_unit', 'fiber_value', 'fiber_unit',
                   'sodium_value', 'sodium_unit', 'minsal_cl_high_sugar',
                   'minsal_cl_high_saturated_fat', 'minsal_cl_high_sodium',
                   'minsal_cl_high_calories', 'aplv_suitable', 'gluten_free',
                   'lactose_free', 'vegan', 'vegetarian', 'diabetes_suitable', 'soy_free',
                   'egg_free', 'fish_free', 'seafood_free', 'peanut_free', 'nuts_free',
                   'walnuts_free', 'sulphite_free', 'wheat_free']

        df = df[columns]
        df = df.fillna(value=np.nan)
        int_cols = ['product_ean', 'timestamp_in', 'last_update', 'size_value', 'drained_size_value',
                    'has_nutritional_table', 'portion_value', 'num_portions',
                    'energy_value', 'protein_value', 'fat_total_value', 'fat_sat_value',
                    'fat_mono_value', 'fat_poli_value', 'fat_trans_value', 'fat_cholesterol_value',
                    'carb_value', 'sugars_value', 'fiber_value', 'sodium_value',
                    'minsal_cl_high_sugar', 'minsal_cl_high_saturated_fat', 'minsal_cl_high_sodium',
                    'minsal_cl_high_calories', 'aplv_suitable', 'gluten_free', 'lactose_free', 'vegan',
                    'vegetarian', 'diabetes_suitable', 'soy_free', 'egg_free', 'fish_free', 'seafood_free',
                    'peanut_free', 'nuts_free', 'walnuts_free', 'sulphite_free', 'wheat_free']
        time_cols = ['date_in', 'date_last_update']
        df['date_in'] = pd.to_datetime(df['date_in'])
        df['date_l ast_update'] = pd.to_datetime(df['date_last_update'])
        types = {x: 'float' for x in int_cols}
        type_str = {x: 'str' for x in columns if x not in int_cols+time_cols}
        types.update(type_str)
        df = df.astype(types)
        df = df.replace("nan", None)

        columns.remove("product_ean")

        columns_query = ",".join(columns)
        excluded_query = ",".join(["EXCLUDED."+column for column in columns])
        values_query = "%s,"+",".join(["%s" for column in columns])
        records = list(df.to_records(index=False))

        # Change data types to native python types
        fixed_records = []
        print(records[:10])
        for record in records[:10]:
            fixed_record = []
            for value in record:
                if isinstance(value, np.generic):
                    fixed_record.append(value.item())
                else:
                    fixed_record.append(value)
            fixed_records.append(tuple(fixed_record))
        print(f"Number of records to load: {str(len(fixed_records))}")
        incremental_query = """
            INSERT INTO ecommdata.administradores (id,"""+columns_query+""") 
            VALUES ("""+values_query+""")
            ON CONFLICT (product_ean)
            DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
        """
        print(incremental_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.executemany(incremental_query, fixed_records)
        pg_connection.commit()
        cursor.close()
        pg_connection.close()
        print("Data loaded to Postgres")
    else:
        print("Finalizado sin obtener data")


def check_update_attributes_products(ti):
    from datetime import datetime, timedelta
    import pandas as pd
    import json
    import requests
    from typing import List
    import utils

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Data loaded to Postgres")
    query_alergias = """
        select s.ref_id as ref_id, concat_ws(',',
            CASE when ok.aplv_suitable = 1 then 'Apto para APLV' ELSE NULL END,
            CASE when ok.gluten_free = 1 then 'Libre de Gluten' ELSE NULL END, --CASE when ok.halal = 1 then 'Halal' ELSE NULL END, CASE when ok.kosher = 1 then 'Kosher' ELSE NULL END,
            CASE when ok.lactose_free = 1 then 'Libre de Lactosa' ELSE NULL END,
            CASE when ok.vegan = 1 then 'Vegano' ELSE NULL END,
            CASE when ok.vegetarian = 1 then 'Vegetariano' ELSE NULL END,
            CASE when ok.diabetes_suitable = 1 then 'Apto para Diabéticos' ELSE NULL END,
            CASE when ok.soy_free = 1 then 'Libre de Soya' ELSE NULL END,
            CASE when ok.egg_free = 1 then 'Libre de Huevo' ELSE NULL END,
            CASE when ok.fish_free = 1 then 'Libre de Peces' ELSE NULL END,
            CASE when ok.seafood_free = 1 then 'Libre de Mariscos' ELSE NULL END,
            CASE when ok.peanut_free = 1 then 'Libre de Maní' ELSE NULL END,
            CASE when ok.nuts_free = 1 then 'Libre de Frutos Secos' ELSE NULL END,
            CASE when ok.walnuts_free = 1 then 'Libre de Nuez' ELSE NULL END,
            CASE when ok.sulphite_free = 1 then 'Libre de Sulfitos' ELSE NULL END,
            CASE when ok.wheat_free = 1 then 'Libre de Trigo' ELSE NULL END
            ) as Alergias 
        from catalogo.ok_to_shop oK
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
        AND NOT EXISTS ( select distinct s.ean_primario from ecommdata.skus s where ok.product_ean::text = s.ean_primario  )
        and se.ean is not null;"""
    query_sellos = """
        select s.ref_id, concat_ws(',',
            CASE when ok.minsal_cl_high_sugar = 1 then 'Alto en Azúcares' ELSE NULL END,
            CASE when ok.minsal_cl_high_sodium  = 1 then 'Alto en Sodio' ELSE NULL END,
            CASE when ok.minsal_cl_high_saturated_fat  = 1 then 'Alto en Grasas Saturadas' ELSE NULL END,
            CASE when ok.minsal_cl_high_calories = 1 then 'Alto en Calorías' ELSE NULL END) as sellos 
        from catalogo.ok_to_shop oK
        left join ecommdata.sku_ean se on ok.product_ean::text = se.ean 
        left join ecommdata.skus s on s.ref_id = se.ref_id
        where (ok.minsal_cl_high_sugar = 1 
            or ok.minsal_cl_high_sodium = 1 
            or ok.minsal_cl_high_saturated_fat = 1 
            or ok.minsal_cl_high_calories = 1 )
            AND NOT EXISTS ( select distinct s.ean_primario from ecommdata.skus s where ok.product_ean::text = s.ean_primario  )
            AND se.ean is not null;"""
    query_alergias_atr_pro = """select ap.ref_id, concat_ws(',',ap.valor) as alergias
        from ecommdata.atributos_producto ap
        where ap.nombre_atributo = 'alergias';"""
    query_sellos_atr_pro = """select ap.ref_id, concat_ws(',',ap.valor) as sellos
        from ecommdata.atributos_producto ap
        where ap.nombre_atributo = 'sellos';"""

    cursor.close()
    pg_connection.close()

    def get_atributos(query):  # atributos: alergias, sellos
        print(f"Iniciando obtencion de atributos...")
        cursor.execute(query)
        pg_connection.commit()
        results = cursor.fetchall()
        columns_name = [i[0] for i in cursor.description]
        print(columns_name)
        df = pd.DataFrame(results, columns=columns_name)
        return df

    df_alergias = get_atributos(query_alergias)
    df_sellos = get_atributos(query_sellos)
    df_alergias_atr = get_atributos(query_alergias_atr_pro)
    df_sellos_atr = get_atributos(query_sellos_atr_pro)

    if df_alergias.equals(df_alergias_atr):
        print("La data no presenta actualizaciones en cuanto a ALERGIAS")
    elif df_sellos.equals(df_sellos_atr):
        print("La data no presenta actualizaciones en cuanto a SELLOS")
    elif ~df_alergias.equals(df_alergias_atr) and df_alergias.size >= df_alergias_atr.size:
        print("Se añaden atributos ALERGIAS nuevos")
        modified_rows_alergias = df_alergias.merge(
            df_alergias_atr, on='alergias', indicator=True, how='outer')
        modified_rows_alergias = modified_rows_alergias[modified_rows_alergias['_merge'] != 'left_only']
    elif ~df_alergias.equals(df_alergias_atr) and df_alergias.size <= df_alergias_atr.size:
        print("Checkear, hay menos datos en la tabla ok_to_shop que en atributos_producto de en cuanto ALERGIAS")
    elif ~df_sellos.equals(df_sellos_atr) and df_sellos.size >= df_sellos_atr.size:
        print("Se añaden atributos SELLOS nuevos")
        modified_rows_sellos = df_sellos.merge(
            df_sellos_atr, on='sellos',  indicator=True, how='outer')
        modified_rows_sellos = modified_rows_sellos[modified_rows_sellos['_merge'] != 'left_only']
    elif ~df_sellos.equals(df_sellos_atr) and df_sellos.size <= df_sellos_atr.size:
        print("Checkear, hay menos datos en la tabla ok_to_shop que en atributos_producto de en cuanto SELLOS")

    df_final = modified_rows_alergias.merge(modified_rows_sellos, how='outer')
    jst = []
    for index, row in df_final.iterrows():
        item = dict()
        item["item_id"] = row['ref_id']
        item["attributes"] = []
        if row['alergias'] is not None:
            attributes = dict()
            attributes['id'] = Variable.get(
                "JANIS_API_REF_ID_ATTRIBUTE_ALERGIAS")
            attributes['values'] = row['alergias'].split(',')
            item["attributes"].append(attributes)
        if row['sellos'] is not None:
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

    json_data = ti.xcom_pull(task_ids=[check_update_attributes_products])[0]
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
    an update of catalogo.ok_to_shop table, and an insert of attributes of products that match EAN's 
    of sku_ean and skus using the API of Janis attribute_value. After this, we hope to observe
    atributos_producto table updated.""",

    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 5, 21, tz="America/Santiago"),
    catchup=False,
    tags=["API", "Janis", "ok_to_shop", 'atributos', 'atributos_producto'],
) as dag:

    dag.doc_md = """
    Extraction and insert of attributes from ftp:ok_to_shop to Janis.
    """

    t0 = PythonOperator(
        task_id="last_file_ok_to_shop",
        python_callable=last_file_ok_to_shop,
    )
    t1 = PythonOperator(
        task_id="check_update_attributes_products",
        python_callable=check_update_attributes_products,
    )
    t2 = PythonOperator(
        task_id="set_janis_atributos",
        python_callable=set_janis_atributos,
    )

    # t0 >> t1 >> t2
