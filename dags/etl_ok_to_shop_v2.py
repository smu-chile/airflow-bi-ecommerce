from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime
import pendulum

def api_ok_to_shop(ds,last_time):
    import pandas as pd
    import requests
    import time
    import io
    import zipfile
    
    exec_date_unix = last_time
    
    api_url = 'https://api.okto.shop/v2/products/dump'
    headers = {
        'Content-Type': 'application/json',
        'x-auth-token': Variable.get("token_ok_to_shop")
    }

    body = {
        "lineBreak": "\n",
        "stringEncapsulator": "\"",
        "encapsulatorEscapeChar": "\"",
        "decimalSeparator": ",",
        "titles": "\"product_id\";\"product_ean\";\"timestamp_in\";\"date_in\";\"last_update\";\"date_last_update\";\"brand_name\";\"description\";\"flavor\";\"size_value\";\"drained_size_value\";\"size_unit\";\"ingredients\";\"allergens\";\"traces\";\"portion_text\";\"portion_value\";\"portion_unit\";\"num_portions\";\"basic_unit\";\"energy_value\";\"energy_unit\";\"protein_value\";\"protein_unit\";\"fat_total_value\";\"fat_total_unit\";\"fat_sat_value\";\"fat_sat_unit\";\"fat_mono_value\";\"fat_mono_unit\";\"fat_poli_value\";\"fat_poli_unit\";\"fat_trans_value\";\"fat_trans_unit\";\"fat_cholesterol_value\";\"fat_cholesterol_unit\";\"carb_value\";\"carb_unit\";\"sugars_value\";\"sugars_unit\";\"fiber_value\";\"fiber_unit\";\"sodium_value\";\"sodium_unit\";\"minsal_cl_high_sugar\";\"minsal_cl_high_saturated_fat\";\"minsal_cl_high_sodium\";\"minsal_cl_high_calories\";\"aplv_suitable\";\"gluten_free\";\"lactose_free\";\"kosher\";\"vegan\";\"vegetarian\";\"diabetes_suitable\";\"soy_free\";\"egg_free\";\"fish_free\";\"seafood_free\";\"peanut_free\";\"nuts_free\";\"walnuts_free\";\"sulphite_free\";\"wheat_free\";\"alcohol_by_volume\";\"alcohol_proof\"",
        "fields": [
            "id",
            "';'",
            "barcodesSeparatedByComma",
            "';'",
            "timestampIn",
            "';'",
            "dateIn",
            "';'",
            "lastUpdate",
            "';'",
            "dateLastUpdate",
            "';'",
            "brandsSeparatedByComma",
            "';'",
            "description",
            "';'",
            "variant",
            "';'",
            "sizeValue",
            "';'",
            "drainedSizeValue",
            "';'",
            "sizeUnit",
            "';'",
            "ingredientsSeparatedByComma",
            "';'",
            "containsSeparatedByComma",
            "';'",
            "nutritionalTracesSeparatedByComma",
            "';'",
            "nutritionfacts:portiontext",
            "';'",
            "nutritionfacts:portionsvalue",
            "';'",
            "nutritionfacts:portionsunit",
            "';'",
            "nutritionfacts:totalportionsvalue",
            "';'",
            "nutritionfacts:basicunit",
            "';'",
            "nutritionfacts:energyvalue",
            "';'",
            "nutritionfacts:energyunit",
            "';'",
            "nutritionfacts:proteinvalue",
            "';'",
            "nutritionfacts:proteinunit",
            "';'",
            "nutritionfacts:totalfatvalue",
            "';'",
            "nutritionfacts:totalfatunit",
            "';'",
            "nutritionfacts:saturatedfatvalue",
            "';'",
            "nutritionfacts:saturatedfatunit",
            "';'",
            "nutritionfacts:monounsaturatedfatvalue",
            "';'",
            "nutritionfacts:monounsaturatedfatunit",
            "';'",
            "nutritionfacts:polyunsaturatedfatvalue",
            "';'",
            "nutritionfacts:polyunsaturatedfatunit",
            "';'",
            "nutritionfacts:transfatvalue",
            "';'",
            "nutritionfacts:transfatunit",
            "';'",
            "nutritionfacts:cholesterolvalue",
            "';'",
            "nutritionfacts:cholesterolunit",
            "';'",
            "nutritionfacts:totalcarbohydratevalue",
            "';'",
            "nutritionfacts:totalcarbohydrateunit",
            "';'",
            "nutritionfacts:totalsugarsvalue",
            "';'",
            "nutritionfacts:totalsugarsunit",
            "';'",
            "nutritionfacts:dietaryfibervalue",
            "';'",
            "nutritionfacts:dietaryfiberunit",
            "';'",
            "nutritionfacts:sodiumvalue",
            "';'",
            "nutritionfacts:sodiumunit",
            "';'",
            "hasWarning:minsalCLHighSugar",
            "';'",
            "hasWarning:minsalCLHighSaturatedFat",
            "';'",
            "hasWarning:minsalCLHighSodium",
            "';'",
            "hasWarning:minsalCLHighCalories",
            "';'",
            "byCertificateV1:cmpaSuitable",
            "';'",
            "byCertificateV1:glutenFree",
            "';'",
            "byCertificateV1:lactoseFree",
            "';'",
            "byCertificateV1:kosher",
            "';'",
            "byCertificateV1:vegan",
            "';'",
            "byCertificateV1:vegetarian",
            "';'",
            "byCertificateV1:diabetesSuitable",
            "';'",
            "byCertificateV1:soyFree",
            "';'",
            "byCertificateV1:eggFree",
            "';'",
            "byCertificateV1:fishFree",
            "';'",
            "byCertificateV1:seafoodFree",
            "';'",
            "byCertificateV1:peanutFree",
            "';'",
            "byCertificateV1:nutsFree",
            "';'",
            "byCertificateV1:walnutsFree",
            "';'",
            "byCertificateV1:sulphiteFree",
            "';'",
            "byCertificateV1:wheatFree",
            "';'",
            "alcoholByVolume",
            "';'",
            "alcoholProof"
        ],
        "since": exec_date_unix
    }

    # Hacer la solicitud a la API
    response = requests.post(api_url, headers=headers, json=body)
    
    if response.status_code == 200:
        # Descargar y descomprimir el archivo ZIP en memoria
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Suponiendo que solo hay un archivo en el ZIP
            csv_filename = zip_file.namelist()[0]
            
            # Leer el CSV directamente desde el ZIP como un DataFrame
            with zip_file.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file, sep=';', encoding='utf-8')
                return df
    else:
        raise Exception(f"Error al descargar el archivo: {response.status_code}")

def ok_to_shop_api_to_s3(ds,ti):
    import pandas as pd
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"ok_to_shop_v2/{exec_date}/"

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    last_time = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_date"])[0]

    df = api_ok_to_shop(ds,last_time)

    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8", sep=';')
    filename = f"ok_to_shop_v2/{exec_date}/ok_to_shop_v2_{date_aux}.csv"
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

def ok_to_shop_api_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["ok_to_shop_api_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    object_csv = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(object_csv.get()["Body"], sep=';')
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    df['date_in'] = df['date_in'].str.replace(r' a\. m\.| p\. m\.', '', regex=True)
    df['date_last_update'] = df['date_last_update'].str.replace(r' a\. m\.| p\. m\.', '', regex=True)

    # Luego, convertimos al formato correcto
    df['date_in'] = pd.to_datetime(df['date_in'], format='%d-%m-%Y %I:%M:%S', errors='coerce')

    # Luego, convertimos al formato correcto
    df['date_last_update'] = pd.to_datetime(df['date_last_update'], format='%d-%m-%Y %I:%M:%S', errors='coerce')

    df['protein_value'] = df['protein_value'].str.replace(',', '.')
    df['fat_total_value'] = df['fat_total_value'].str.replace(',', '.')
    df['fat_sat_value'] = df['fat_sat_value'].str.replace(',', '.')
    df['fat_mono_value'] = df['fat_mono_value'].str.replace(',', '.')
    df['fat_poli_value'] = df['fat_poli_value'].str.replace(',', '.')
    df['fat_trans_value'] = df['fat_trans_value'].str.replace(',', '.')
    df['fat_cholesterol_value'] = df['fat_cholesterol_value'].str.replace(',', '.')
    df['sugars_value'] = df['sugars_value'].str.replace(',', '.')
    df['sodium_value'] = df['sodium_value'].str.replace(',', '.')
    df['energy_value'] = df['energy_value'].str.replace(',', '.')
    df['carb_value'] = df['carb_value'].str.replace(',', '.')

    df['protein_value'] = df['protein_value'].astype(float, errors = 'raise')
    df['fat_total_value'] = df['fat_total_value'].astype(float, errors = 'raise')
    df['fat_sat_value'] = df['fat_sat_value'].astype(float, errors = 'raise')
    df['fat_mono_value'] = df['fat_mono_value'].astype(float, errors = 'raise')
    df['fat_poli_value'] = df['fat_poli_value'].astype(float, errors = 'raise')
    df['fat_trans_value'] = df['fat_trans_value'].astype(float, errors = 'raise')
    df['fat_cholesterol_value'] = df['fat_cholesterol_value'].astype(float, errors = 'raise')
    df['sugars_value'] = df['sugars_value'].astype(float, errors = 'raise')
    df['sodium_value'] = df['sodium_value'].astype(float, errors = 'raise')
    df['energy_value'] = df['energy_value'].astype(float, errors = 'raise')
    df['carb_value'] = df['carb_value'].astype(float, errors = 'raise')

    columns = [
    "product_ean", "timestamp_in", "date_in", "last_update", "date_last_update", 
    "brand_name", "description", "flavor", "size_value", "drained_size_value", "size_unit", 
    "ingredients", "allergens", "traces", "portion_text", "portion_value", "portion_unit", 
    "num_portions", "basic_unit", "energy_value", "energy_unit", "protein_value", "protein_unit", 
    "fat_total_value", "fat_total_unit", "fat_sat_value", "fat_sat_unit", "fat_mono_value", 
    "fat_mono_unit", "fat_poli_value", "fat_poli_unit", "fat_trans_value", "fat_trans_unit", 
    "fat_cholesterol_value", "fat_cholesterol_unit", "carb_value", "carb_unit", "sugars_value", 
    "sugars_unit", "fiber_value", "fiber_unit", "sodium_value", "sodium_unit", 
    "minsal_cl_high_sugar", "minsal_cl_high_saturated_fat", "minsal_cl_high_sodium", 
    "minsal_cl_high_calories", "aplv_suitable", "gluten_free", "lactose_free", "kosher","vegan", 
    "vegetarian", "diabetes_suitable", "soy_free", "egg_free", "fish_free", "seafood_free", 
    "peanut_free", "nuts_free", "walnuts_free", "sulphite_free", "wheat_free", 
    "alcohol_by_volume", "alcohol_proof"
    ]

    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))

    df.info()
    
    # Change data types to native python types
    fixed_records = []
    for record in records:
        fixed_record = []
        for value in record:
            if isinstance(value, np.generic):
                fixed_record.append(value.item())
            elif value == "NULL":
                fixed_record.append(None)
            else:
                fixed_record.append(value)
        fixed_records.append(tuple(fixed_record))
    print(f"Number of records: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO catalogo.ok_to_shop_v2 (product_id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (product_id)
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

    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_ok_to_shop_v2',
    default_args=default_args,
    description="""Cargar datos de eans de productos al consumir API ok_to_shop""",
    schedule_interval="0 5 * * 6",
    start_date=pendulum.datetime(2023, 5, 21, tz="America/Santiago"),
    catchup=False,
    tags=["API", "ok_to_shop", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Cargar datos de eans de productos al consumir API ok_to_shop a postgres y S3
    upsert
    """

    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "catalogo",
            "table_name": "ok_to_shop_v2", 
            "updated_at_field": "last_update",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id="ok_to_shop_api_to_s3",
        python_callable=ok_to_shop_api_to_s3,
    )
    t2 = PythonOperator(
        task_id="ok_to_shop_api_to_postgres",
        python_callable=ok_to_shop_api_to_postgres,
    )

    t0 >> t1 >> t2