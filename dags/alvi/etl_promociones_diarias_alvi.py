from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

from utils.netezza_utils import load_custom_query_to_s3

import pendulum

def _delete_fixed_prices_from_vtex(ti, ds):
    import pandas as pd
    import sqlalchemy
    import requests
    
    price_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: " + price_file)
    if not s3_hook.check_for_key(price_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % price_file)

    price_object = s3_hook.get_key(price_file, bucket_name=s3_bucket)
    column_types = {
        "REF_ID": "str",
        "PRICE_TABLE": "str"
    }

    df_precios_fijos = pd.read_csv(price_object.get()["Body"], dtype=column_types)
    print(f"Number of records found: {len(df_precios_fijos.index)}")
    
    if len(df_precios_fijos.index) == 0:
        print("There are no new records to load. Task will exit as successfull.")
        return

    column_names = {
        "REF_ID": "ref_id",
        "PRICE_TABLE": "id_lista_precios"
    }

    df_precios_fijos = df_precios_fijos.rename(columns=column_names)

    list_ref_id = tuple(df_precios_fijos['ref_id'].tolist())

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    print("Getting vtex_ids of products in WP with Fixed Price")
    query = f"""SELECT DISTINCT s.ref_id, s.vtex_id
                FROM ecommdata_alvi.skus s
                WHERE s.ref_id IN {list_ref_id};"""
    cursor.execute(query)
    results = cursor.fetchall()
    df_vtex_id = pd.DataFrame(results, columns=['ref_id', 'vtex_id'])
    cursor.close()
    pg_connection.close()

    merged_df = pd.merge(df_precios_fijos, df_vtex_id, on='ref_id', how='inner')

    X_VTEX_API_AppKey = Variable.get("X_VTEX_ALVI_API_Appkey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_ALVI_API_Apptoken")
    accountName = Variable.get("VTEX_ALVI_ACCOUNT_NAME")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken": X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    for _, row in merged_df.iterrows():
        endpoint = f"https://api.vtex.com/{accountName}/pricing/prices/{row['vtex_id']}/fixed/{row['id_lista_precios']}"
        print(endpoint)
        response = requests.delete(endpoint, headers=headers)
        if response.status_code == 200:
            print(f"Deleted price for VTEX ID: {row['vtex_id']} and Price Table ID: {row['id_lista_precios']}")
        else:
            print(f"Failed to delete price for VTEX ID: {row['vtex_id']}. Status code: {response.status_code}")
            print(response.text)
    return

def create_and_load_s3(ds):
    import pandas as pd
    import numpy as np
    import os
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    prefix = f"promociones_vtex_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    curr_working_directory = os.getcwd()
    print(os.getcwd())

    with open(f"{curr_working_directory}/dags/alvi/sql/promociones_diarias.sql", "r") as query_file:
        promociones_query = query_file.read()
    
    promociones_query = promociones_query.replace("{ds}", ds)

    print("Base query:")
    print(promociones_query)

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    df_promotions = pd.read_sql_query(promociones_query, pg_connection)
    buffer = io.StringIO()
    df_promotions.to_csv(buffer, header=True, index=False, encoding="utf-8")

    filename = f"promociones_vtex_alvi/{exec_date}/promociones_diarias.csv"

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

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_promotion_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_promotion_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata_alvi.promociones_diarias") 
        df.to_sql(name="promociones_diarias",
                    con=conn,         
                    schema="ecommdata_alvi",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data loaded to Postgres: ecommdata_alvi.promociones_diarias")
    return

def create_list_price(ti):
    import json
    import pandas as pd
    import requests

    filename = ti.xcom_pull(key="return_value", task_ids=["create_and_load_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_promotion_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_promotion_object.get()["Body"])

    df['nombre_promocion'] = df['nombre_promocion'].apply(lambda x: x.strip(
    ).replace(' ', '').replace('.', '').replace('+', '').replace('-', '').replace(',', ''))

    df_vtex = df
    df_vtex = df_vtex.sort_values(by='precio_promocional_2')
    df_vtex = df_vtex.drop_duplicates(subset='ref_id', keep='first')

    if len(df_vtex.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    X_VTEX_API_AppKey = Variable.get("X_VTEX_ALVI_API_Appkey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_ALVI_API_Apptoken")
    accountName = Variable.get("VTEX_ALVI_ACCOUNT_NAME")
    environment = Variable.get("VTEX_ENV")
    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }

    column_mapping = {
        "nombre_promocion": 'promotionName',
        'ref_id': "refId",
        'precio_modal': "modalPrice",
        "precio_promocional": "PrecPro1",
        'precio_promocional_2': "PrecPro2",
        'factor': "Factor1",
        'factor_2': "Factor2",
        'fecha_inicio_de_promocion': 'startDate',
        'fecha_fin_de_promocion': 'endDate'
    }
    df_vtex = df_vtex.rename(columns=column_mapping)

    df_vtex = df_vtex.assign(Factor0=1)
    df_vtex['PrecPro0'] = df_vtex['modalPrice']
    df_vtex['Factor0'] = df_vtex.apply(
        lambda row: 0 if row['Factor1'] == 1 else row['Factor0'], axis=1)

    df_vtex_w2l = (
        pd.wide_to_long(df_vtex, stubnames=["PrecPro", "Factor"], i=["refId"], j="n")
        .droplevel(-1)
        .reset_index()
    )

    df_vtex_w2l = df_vtex_w2l[df_vtex_w2l["Factor"] != 0]

    df_vtex_w2l["startDate"] = pd.to_datetime(
        df_vtex_w2l["startDate"], dayfirst=True).dt.strftime("%Y-%m-%dT%H:%M:%S-03:00")
    df_vtex_w2l["endDate"] = (pd.to_datetime(df_vtex_w2l["endDate"], dayfirst=True) +
                              pd.Timedelta(days=1)).dt.strftime(("%Y-%m-%dT%H:%M:%S-03:00"))

    df_vtex_w2l = df_vtex_w2l.rename(
        columns={
            "vtex_id": "SKU ID",
            "promotionName": "Trade Policy",
            "PrecPro": "Price",
            "Factor": "Min Quantity",
            "modalPrice": "List Price",
            "startDate": "Date From",
            "endDate": "Date To"
        }
    )

    cols_vtex = ["SKU ID", "Trade Policy", "Price",
                 "List Price", "Min Quantity", "Date From", "Date To"]
    df_vtex_w2l = df_vtex_w2l[cols_vtex]

    df_vtex_w2l['List Price'] = df_vtex_w2l['List Price'].astype('int64')

    payload_dict={}
    price_table_dict = {}

    for index, row in df_vtex_w2l.iterrows():
        itemId = str(int(round(float(row['SKU ID']))))
        priceTableId = row['Trade Policy']

        current_payload = {
            "value": int(row['Price']),
            "minQuantity": int(row['Min Quantity']),
            "dateRange": {
                "from": row['Date From'],
                "to": row['Date To']
            }
        }

        price_table_dict[itemId] = priceTableId

        if itemId in payload_dict:
            payload_dict[itemId].append(current_payload)
        else:
            payload_dict[itemId] = [current_payload]

    for itemId, payload_list in payload_dict.items():
        priceTableId = price_table_dict.get(itemId, "")
        
        print(payload_list)
        POST_CREATE_UPDATE_FIXED_PRICES = f"https://api.vtex.com/{accountName}/pricing/prices/{itemId}/fixed/{priceTableId}"
        print(POST_CREATE_UPDATE_FIXED_PRICES)

        r = requests.post(POST_CREATE_UPDATE_FIXED_PRICES, json=payload_list, headers=headers)
        print("r.status_code: ", r.status_code)
        print("r.text: ", r.text)
    
    df['nombre_vtex'] = df['n_promocion'].astype(str) + ' ' + df['nombre_promocion']

    valid_n_promocion = df.groupby('n_promocion')['idcalculatorconfigurator'].apply(lambda x: x.isnull().all())
    valid_n_promocion = valid_n_promocion[valid_n_promocion].index

    filtered_df = df[df['n_promocion'].isin(valid_n_promocion)].copy()
    unique_df = filtered_df.drop_duplicates(subset='nombre_vtex').reset_index(drop=True)

    if len(unique_df.index) == 0:
        print("There are no new promotions to create. Task will exit as successfull.")
        return

    base = f"https://{accountName}.{environment}.com.br"
    url = base + "/api/rnb/pvt/calculatorconfiguration"
    
    for index, row in unique_df.iterrows():
        payload = {
                'idCalculatorConfiguration': "",
                'name': row['nombre_vtex'],
                'generalValues': {'WORKFLOWID': row['n_promocion']},
                'beginDateUtc': str(pendulum.parse(row['fecha_inicio_de_promocion'], tz='America/Santiago')),
                'endDateUtc': str(pendulum.parse(row['fecha_fin_de_promocion'], tz='America/Santiago').add(days=1)),
                'lastModified': "",
                'daysAgoOfPurchases': 0,
                'isActive': True,
                'isArchived': False,
                'isFeatured': True,
                'disableDeal': False,
                'activeDaysOfWeek': [],
                'offset': 0,
                'activateGiftsMultiplier': False,
                'maxPricesPerItems': [],
                'cumulative': False,
                'nominalShippingDiscountValue': 0.0,
                'absoluteShippingDiscountValue': 0.0,
                'nominalDiscountValue': 0.0,
                'nominalDiscountType': "cart",
                'maximumUnitPriceDiscount': 0,
                'percentualDiscountValue': 0.0,
                'rebatePercentualDiscountValue': 0.0,
                'percentualShippingDiscountValue': 0.0,
                'percentualTax': 0.0,
                'shippingPercentualTax': 0.0,
                'percentualDiscountValueList1': 0.0,
                'percentualDiscountValueList2': 0.0,
                'skusGift': {'quantitySelectable': 0},
                'nominalRewardValue': 0.0,
                'percentualRewardValue': 0.0,
                'orderStatusRewardValue': "invoiced",
                'maxNumberOfAffectedItems': 0,
                'maxNumberOfAffectedItemsGroupKey': "perCart",
                'applyToAllShippings': False,
                'priceTableName': row['nombre_promocion'].lower(),
                'nominalTax': 0.0,
                'origin': "Marketplace",
                'idSellerIsInclusive': False,
                'idsSalesChannel': [],
                'areSalesChannelIdsExclusive': False,
                'marketingTags': [],
                'marketingTagsAreNotInclusive': False,
                'paymentsMethods': [],
                'stores': [],
                'campaigns': [],
                'storesAreInclusive': True,
                'categories': [],
                'categoriesAreInclusive': True,
                'brands': [],
                'brandsAreInclusive': True,
                'products': [],
                'productsAreInclusive': True,
                'skus': [],
                'skusAreInclusive': True,
                'collections1BuyTogether': [],
                'collections2BuyTogether': [],
                'minimumQuantityBuyTogether': 0,
                'quantityToAffectBuyTogether': 0,
                'enableBuyTogetherPerSku': False,
                'listSku1BuyTogether': [],
                'listSku2BuyTogether': [],
                'coupon': [],
                'totalValueFloor': 0.0,
                'totalValueCeling': 0.0,
                'totalValueIncludeAllItems': False,
                'totalValueMode': "IncludeMatchedItems",
                'collections': [],
                'collectionsIsInclusive': True,
                'restrictionsBins': [],
                'cardIssuers': [],
                'totalValuePurchase': 0.0,
                'slasIds': [],
                'isSlaSelected': False,
                'isFirstBuy': False,
                'firstBuyIsProfileOptimistic': False,
                'compareListPriceAndPrice': False,
                'isDifferentListPriceAndPrice': False,
                'zipCodeRanges': [],
                'itemMaxPrice': 0.0,
                'itemMinPrice': 0.0,
                'installment': 0,
                'isMinMaxInstallments': False,
                'minInstallment': 0,
                'maxInstallment': 0,
                'merchants': [],
                'clusterExpressions': [],
                'piiClusterExpressions': [],
                'paymentsRules': [],
                'giftListTypes': [],
                'productsSpecifications': [],
                'affiliates': [],
                'maxUsage': 0,
                'maxUsagePerClient': 0,
                'shouldDistributeDiscountAmongMatchedItems': False,
                'multipleUsePerClient': False,
                'accumulateWithManualPrice': True,
                'type': "regular",
                'useNewProgressiveAlgorithm': False,
                'percentualDiscountValueList': []
        }
        print(payload)
        try:
            r = requests.post(url, headers=headers, json=payload)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error in API request: {e}")
            continue

        print("API request status code: ", r.status_code)
        print("API response content: ", r.content)
    
    return
    
def load_json_to_publisher(ti):
    import datetime
    from datetime import datetime
    import pandas as pd
    import requests
    import sqlalchemy

    filename = ti.xcom_pull(key="return_value", task_ids=["create_and_load_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_promotion_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_promotion_object.get()["Body"])

    df = df.sort_values(by='precio_promocional_2')
    df = df.drop_duplicates(subset='ref_id', keep='first')

    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    df['nombre_promocion'] = df['nombre_promocion'].apply(lambda x: x.strip(
    ).replace(' ', '').replace('.', '').replace('+', '').replace('-', '').replace(',', ''))

    column_mapping = {
        "nombre_promocion": 'promotionName',
        'ref_id': "refId",
        'precio_modal': "modalPrice",
        "precio_promocional": "firstPromotionalPrice",
        'precio_promocional_2': "secondPromotionalPrice",
        'factor': "firstMinQuantity",
        'factor_2': "secondMinQuantity",
        'fecha_inicio_de_promocion': 'startDate',
        'fecha_fin_de_promocion': 'endDate'
    }
    
    df.rename(columns=column_mapping, inplace=True)
    df = df.assign(
        promotionType="Escalas",
        local="3092",
        isRemoved=False
    )

    df['startDate'] = pd.to_datetime(df['startDate'], unit='ns').dt.strftime('%Y-%m-%d')
    df['endDate'] = pd.to_datetime(df['endDate'], unit='ns').dt.strftime('%Y-%m-%d')

    df = df.astype({
        'modalPrice': 'int64',
        'firstPromotionalPrice': 'int64',
        'secondPromotionalPrice': 'int64',
        "firstMinQuantity": 'int64',
        "secondMinQuantity": 'int64'
    })

    df['secondPromotionalPrice'] = df['secondPromotionalPrice'].apply(lambda x: None if x == 0 else x)
    df['secondMinQuantity'] = df['secondMinQuantity'].apply(lambda x: None if x == 0 else x)

    main_cols = ['promotionType'] + list(column_mapping.values()) + ['local', 'isRemoved']
    df = df[main_cols]
    result = df.to_json(orient="records")

    print(result)

    headers = {
        'Content-Type': 'application/json'
    }

    POST_PUBLISH_FIXED_PRICES = "https://ms-integrations-publisher.smu-service.cl/promotions"
    print(POST_PUBLISH_FIXED_PRICES)
    r = requests.request("POST", POST_PUBLISH_FIXED_PRICES, headers=headers, data=result)
    print("r.status_code: ", r.status_code)
    print("r.text: ", r.text)

    return

def load_prices_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    filename = ti.xcom_pull(key="return_value", task_ids=["create_and_load_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_promotion_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_promotion_object.get()["Body"])

    df['nombre_promocion'] = df['nombre_promocion'].apply(lambda x: x.strip(
    ).replace(' ', '').replace('.', '').replace('+', '').replace('-', '').replace(',', ''))

    df_vtex = df
    df_vtex = df_vtex.sort_values(by='precio_promocional_2')
    df_vtex = df_vtex.drop_duplicates(subset='ref_id', keep='first')

    if len(df_vtex.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    column_mapping = {
        "nombre_promocion": 'promotionName',
        'ref_id': "refId",
        'precio_modal': "modalPrice",
        "precio_promocional": "PrecPro1",
        'precio_promocional_2': "PrecPro2",
        'factor': "Factor1",
        'factor_2': "Factor2",
        'fecha_inicio_de_promocion': 'startDate',
        'fecha_fin_de_promocion': 'endDate'
    }
    df_vtex = df_vtex.rename(columns=column_mapping)

    df_vtex = df_vtex.assign(Factor0=1)
    df_vtex['PrecPro0'] = df_vtex['modalPrice']
    df_vtex['Factor0'] = df_vtex.apply(
        lambda row: 0 if row['Factor1'] == 1 else row['Factor0'], axis=1)

    df_vtex_w2l = (
        pd.wide_to_long(df_vtex, stubnames=["PrecPro", "Factor"], i=["refId"], j="n")
        .droplevel(-1)
        .reset_index()
    )

    df_vtex_w2l = df_vtex_w2l[df_vtex_w2l["Factor"] != 0]

    df_vtex_w2l["startDate"] = pd.to_datetime(
        df_vtex_w2l["startDate"], dayfirst=True).dt.strftime("%Y-%m-%dT%H:%M:%S-03:00")
    df_vtex_w2l["endDate"] = (pd.to_datetime(df_vtex_w2l["endDate"], dayfirst=True) +
                              pd.Timedelta(days=1)).dt.strftime(("%Y-%m-%dT%H:%M:%S-03:00"))

    df_vtex_w2l = df_vtex_w2l.rename(
        columns={
            "refId": "ref_id",
            "promotionName": "nombre_promocion",
            "PrecPro": "precio_promocional",
            "Factor": "cantidad",
            "startDate": "fecha_inicio_promocion",
            "endDate": "fecha_fin_promocion"
        }
    )

    cols_vtex = ["ref_id", "nombre_promocion", "precio_promocional", "cantidad", "fecha_inicio_promocion", "fecha_fin_promocion"]
    df_vtex_w2l = df_vtex_w2l[cols_vtex]

    df_vtex_w2l.info()

    print(df_vtex_w2l)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata_alvi.precios_promocionales"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df_vtex_w2l.to_sql(name="precios_promocionales",
                con=engine,         
                schema="ecommdata_alvi",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata_alvi.precios_promocionales")
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_promociones_diarias_alvi',
    default_args=default_args,
    description="crear y cargar promociones que estan activas en workflow y VTEX",
    schedule_interval="30 23 * * *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["ecommdata", "VTEX", "promociones", "unimarc", "workflow", "SERGIO"],
) as dag:
    
    dag.doc_md = """
    construir y cargar promociones diarias de VTEX. \n
    Upsert en tabla ecommdata.promociones_diarias.
    """
    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """SELECT DISTINCT LPAD(MATERIAL,18,'0') || '-' || CASE
                    WHEN UN_MEDIDA_VENTA = 'ST'::text THEN 'UN'
                    WHEN UN_MEDIDA_VENTA = 'CS'::text THEN 'CJ'
                    ELSE UN_MEDIDA_VENTA
            END AS REF_ID,
            TRANSLATE(NOMBRE_PROMOCION, ' ,.', '') AS PRICE_TABLE
        FROM NZ_BU.ECOMERCE.VW_WORKFLOW 
        WHERE ORGANIZACION_VENTAS = '7500'
        AND CANAL_DISTRIBUCION in ('10','70')
        AND ID_EVENTO <> '572'
        AND FECHA_INICIO_DE_PROMOCION <= TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD')
        AND FECHA_FIN_DE_PROMOCION >= TO_DATE('{{execution_date.strftime('%Y-%m-%d')}}', 'YYYY-MM-DD') 
        AND SKU_CANCEL = 'X'
        AND TIPO_PROMOCION IN (10,9,4);
            """,
            "query_name": "borrado_precios_fijos_alvi",
        }
    )

    t1 = PythonOperator(
        task_id = "_delete_fixed_prices_from_vtex",
        python_callable = _delete_fixed_prices_from_vtex,
    )

    t2 = PythonOperator(
        task_id = "create_and_load_s3",
        python_callable = create_and_load_s3,
    )

    t3 = PythonOperator(
        task_id = "truncate_and_load_postgres",
        python_callable = truncate_and_load_postgres,
    )

    t4 = PythonOperator(
        task_id = "create_list_price",
        python_callable = create_list_price,
    )

    t5 = PythonOperator(
        task_id = "load_json_to_publisher",
        python_callable = load_json_to_publisher,
    )

    t6 = PythonOperator(
        task_id = "load_prices_to_postgres",
        python_callable = load_prices_to_postgres,
    )
    
    t0 >> t1 >> t2 >> t3 >> t4 >> t5 >> t6