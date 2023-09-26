from airflow import DAG
from airflow import macros
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum

def payload_prom(row):
        import re
        max_skus_regular = 200
        max_skus_1BuyTogether = 100
        lista_precio = (row['precio_wp'] == 'lista-precio')
        regular = (row['mecanica_wp'] == 'regular')
        moreforless = (row['mecanica_wp'] == 'forThePriceOf')

        price_table_name = re.sub(
            r'[^a-zA-Z0-9]', '', row['nombre_promocion_wp']) if lista_precio else ''

        skus_regular = row['vtexid_name'] if (
            regular & ~lista_precio & (row['vtex_id_count'] <= max_skus_regular)) else []

        price = 0 if lista_precio else row['precio']

        skus_moreforless = row['vtexid_name'] if moreforless & (
            row['vtex_id_count'] <= max_skus_1BuyTogether) else []
        collections_regular = get_collection_name([item['id'] for item in row['vtexid_name']]) if (
            regular & ~lista_precio & (row['vtex_id_count'] > max_skus_regular)) else []
        collections_moreforless = get_collection_name(
            [item['id'] for item in row['vtexid_name']]) if moreforless & ~lista_precio & (row['vtex_id_count'] > max_skus_1BuyTogether) else []

        # PROMOCIONES MORE FOR LESS ( forThePriceOf )
        # % descuento 2da unidad
        combinacion_llevas_n = (row['tipo_promocion'] == 8)
        combinacion_nxm = (row['tipo_promocion'] == 2)  # es con %descuento
        descuento_regular = (row['tipo_promocion'] == 1)

        minimumQuantityBuyTogether = row['llevas_n'] if row['llevas_n'] > 0 else row['cantidad_n']
        quantityToAffectBuyTogether = row['llevas_n'] if row['llevas_n'] > 0 else row['cantidad_n']

        if combinacion_llevas_n:  # 'percentualDiscountValue'
            porcentaje_descuento = row['porcentaje_n']/2
        elif combinacion_nxm:
            porcentaje_descuento = 100 - \
                (row['cantidad_m']/row['cantidad_n'])*100
        elif descuento_regular:
            porcentaje_descuento = row['porcentaje_de_descuento_wp']
        else:
            porcentaje_descuento = 0.0

        payload = {
            'idCalculatorConfiguration': "",
            'name': row['nombre_carga_wp'],
            'generalValues': {'WORKFLOWID': row['n_promocion_wp']},
            'beginDateUtc': row['fecha_inicio_wp'],
            'endDateUtc': row['fecha_fin_wp'],
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
            'maximumUnitPriceDiscount': int(price),
            'percentualDiscountValue': porcentaje_descuento,
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
            'priceTableName': price_table_name,
            'nominalTax': 0.0,
            'origin': "Marketplace",
            'idSellerIsInclusive': False,
            'idsSalesChannel': [40],
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
            'skus': skus_regular,
            'skusAreInclusive': True,
            'collections1BuyTogether': collections_moreforless,
            'collections2BuyTogether': [],
            'minimumQuantityBuyTogether': minimumQuantityBuyTogether,
            'quantityToAffectBuyTogether': quantityToAffectBuyTogether,
            'enableBuyTogetherPerSku': False,
            'listSku1BuyTogether': skus_moreforless,
            'listSku2BuyTogether': [],
            'coupon': [],
            'totalValueFloor': 0.0,
            'totalValueCeling': 0.0,
            'totalValueIncludeAllItems': False,
            'totalValueMode': "IncludeMatchedItems",
            'collections': collections_regular,
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
            'type': row['mecanica'],
            'useNewProgressiveAlgorithm': False,
            'percentualDiscountValueList': []
        }
        print(payload)
        return payload

def _load_scheduled_wp_tables_to_s3(ds):
    import pandas as pd
    import io
    wp_query = """
        SELECT * FROM ecommdata.cruce_wp_pdv cwp
            WHERE id_vtex is null and nombre_carga_wp is not null
        """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_prod")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(wp_query)
    results = cursor.fetchall()
    cursor.close()
    columns_name = [i[0] for i in cursor.description]
    cursor.close()
    pg_connection.close()
    df_wp = pd.DataFrame(results, columns=columns_name)
    '''
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"carga_promociones/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    #Save to S3
    buffer = io.StringIO()
    df_wp.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"carga_promociones/{exec_date}/carga_promociones{date_aux}.csv"
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

def _load_promotions_from_s3(ti):
    import pandas as pd

    filename = ti.xcom_pull(key="return_value", task_ids=["_load_scheduled_wp_tables_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    promotions_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(promotions_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    print(df.info())

    '''
    df_wp['vtexid_name'] = df_wp[['vtex_id', 'nombre_sku']].apply(
        lambda row: {'id': row['vtex_id'], 'name': row['nombre_sku']}, axis=1)

    len_grouped = df_wp.shape[0]
    print(f"Tamaño grouped_df: {len_grouped}")
    if len_grouped == 0:
        print('El número de cabecera no arroja promociones, posiblemente por filtros vtex o lista8')
        return pd.DataFrame()
    print(df_wp.info())
    # GENERAL FIELDS OF CREATE PROMOTION
    df_wp['payload_prom'] = df_wp.apply(
        lambda row: payload_prom(row), axis=1)
    
    accountName = Variable.get("VTEX_ACCOUNT_NAME")
    environment = Variable.get("VTEX_ENV")

    X_VTEX_API_AppKey = Variable.get("X_VTEX_API_AppKey")
    X_VTEX_API_AppToken = Variable.get("X_VTEX_API_AppToken")

    headers = {
        'Accept': "application/json",
        'Content-Type': "application/json",
        "X-VTEX-API-AppKey": X_VTEX_API_AppKey,
        "X-VTEX-API-AppToken":  X_VTEX_API_AppToken,
        "Connection": "keep-alive"
    }
    
    '''for promocion in objeto['promotion_loads']:
        base = f"https://{accountName}.{environment}.com.br"
        url = base+"/api/rnb/pvt/calculatorconfiguration"
        r = requests.post(url, headers=headers, json=promocion)
        print("r.status_code: ", r.status_code)
        print("r.content: ", r.content)
        promocion['status'] = r.status_code
        promocion['text'] = r.text
        contents.append(json.loads(r.text))
        contents.append(promocion)
    pd.DataFrame(contents).to_excel(f"./respuestas.xlsx")
    with open(f"OUT_{file}", "w+") as archivo:
        json.dump({"promos": contents}, archivo)
    return print("CARGADO PROMOCIONES")'''

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_promociones_workflow_load_unimarc',
    default_args=default_args,
    description="Extracción y carga de promociones a VTEX Unimarc.",
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2023, 8, 18, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "promotions","VTEX", "unimarc"],
) as dag:

    dag.doc_md = """
    calculo distancia y tiempo ordenes dia anterior
    """ 
    t0 = PythonOperator(
        task_id = "_load_scheduled_wp_tables_to_s3",
        python_callable = _load_scheduled_wp_tables_to_s3,
    )

    t0