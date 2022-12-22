from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pendulum

def get_stock():
    import pandas as pd
    import requests
    import io
    from pprint import pprint

    #### IMPORTA CSV
    
    file_name = 'forecast_and_planning/obtencion_stock_meli/publi_meli_19dic2022.xlsx'
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+file_name)
    if not s3_hook.check_for_key(file_name, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % file_name)
    
    stock_object = s3_hook.get_key(file_name, bucket_name = s3_bucket)
    data_stock = stock_object.get()['Body'].read()

    # df_lect = pd.read_csv(stock_object.get()["Body"], sep=';')

    api_obtain_token = Variable.get('MELI_TOKEN_API')
    body_obtain_token = {
    "grant_type": "refresh_token",
    "client_id": Variable.get('MELI_CLIENT_ID'),
    "client_secret": Variable.get('MELI_CLIENT_SECRET'),
    "redirect_uri": Variable.get('MELI_REDIRECT_URI'),
    "refresh_token": Variable.get('MELI_REFRESH_TOKEN'),
    }
    header_obtain = {
        "Content-Type" : "application/json",
    }
    request_post = requests.post(api_obtain_token, data = body_obtain_token, headers=header_obtain)
    response = request_post.json()
    print (request_post.status_code)


    api_url = Variable.get('MELI_TOKEN_API')
    bearer_token = response['access_token']
    header = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type" : "application/json",}


    products_api = Variable.get('MELI_PRODUCTS_API_FORMAT')
    get_stock_seller = Variable.get('MELI_STOCK_API_FORMAT')
    get_non_available_stock = Variable.get('MELI_STOCK_DETAILS_API_FORMAT')

    df_lect = pd.read_excel(io.BytesIO(data_stock), sheet_name='Publicaciones', usecols="A:M",
    names=['product_id','num_variante','sku','titulo','variantes','cantidad', 'precio', 'moneda', 'descripcion', 'forma de envio','tipo de publicacion', 'cargo por venta', 'estado'],
    skiprows=2)



    total_inventory_id = []
    df_get_id = df_lect['product_id'].dropna()
    print (df_get_id)
    largo = len(list(df_get_id))

    for x in range(largo):
        product_id_value = df_get_id.iat[x]
        r = requests.get(products_api.format(product_id_value), headers=header)
        # pprint (r.json())
        print (r.status_code)
        response = r.json()
        # registro = []
        # registro.append(response['inventory_id'])
        total_inventory_id.append(response['inventory_id'])
        if x == 50:
            break

    total_inventory_id = [x for x in total_inventory_id if x is not None]

    print (total_inventory_id)

    # df2 = pd.DataFrame(total_inventory_id, columns = ['inventory_id'])
    # df2 = df2.dropna()
    # largo2 = len(df2)
    # print (largo2)
    # print (df2)

    x = 0
    y = 0
    tabla_1 = []
    total_data_available = []

    # r = requests.get(get_non_available_stock.format('LSAS09272'), headers=header)
    # print (r.status_code)

    for inventory_id_value in total_inventory_id:
        print (inventory_id_value)
        r = requests.get(get_non_available_stock.format(str(inventory_id_value)), headers=header)
        print (r.status_code)
        pprint (r.json())
        response = r.json()

        registro = []
        registro.append(response["total"])
        registro.append(response["available_quantity"])
        registro.append(response["not_available_quantity"])
        registro.append(response["inventory_id"])
        registro.append(response["external_references"][0]["id"])
        tabla_1.append(registro)
        print (registro)
        x = x+1
        if x == 50:
            break

    columns_t1 = ["cantidad_total",
                "cantidad_disponible",
                "cantidad_no_disponible",
                "inventory_id",
                "product_id",
                ]

    for inventory_id_value in total_inventory_id:
        print (inventory_id_value)
        r = requests.get(get_non_available_stock.format(inventory_id_value), headers=header)
        print (r.status_code)
        response = r.json()

        registro = []
        registro.append(response["available_quantity"])
        registro.append(response["inventory_id"])
        registro.append(response["external_references"][0]["id"])
        #segundo nivel
        not_available_status = response.get("not_available_detail",[])
        for not_available in not_available_status:
            registro_2 = registro.copy()
            registro_2.append(not_available["status"])
            registro_2.append(not_available["quantity"])

            # tercer nivel
            conditions = not_available.get("conditions", [])
            for condition in conditions:
                registro_3 = registro_2.copy()
                registro_3.append(condition["condition"])
                registro_3.append(condition["quantity"])

                total_data_available.append(registro_3)
                print("NIVEL 3")
                pprint(registro_3)

            if len(conditions) == 0:
                registro_2 = registro_2 + [None, None]
                total_data_available.append(registro_2)
                print("NIVEL 2")
                pprint(registro_2)
        if len(not_available_status) == 0:
            registro = registro + [None,None,None,None]
            total_data_available.append(registro)
            print("NIVEL 1")
            pprint(registro)
        
        y = y+1
        if y == 50:
            break

    df_list = pd.DataFrame(tabla_1, columns=columns_t1)
    print (df_list)
    # df_list.to_csv('output_mlfile/df_tabla1.csv', index=False, sep=';')

    # pprint (total_data_available)

    columns = ["available_quantity",
                "inventory_id",
                "prodct_id",
                "status",
                "not_available_quantity",
                "condition",
                "quantity"]

    df_tot = pd.DataFrame(total_data_available, columns=columns)
    print (df_tot)
    # df_tot.to_csv('output_mlfile/df_total.csv', index=False, sep=';')



default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_obtenciob_stock_meli',
    default_args=default_args,
    description="Automatización de obtención de stock de MELI",
    schedule_interval="0 23 * * *",
    start_date=pendulum.datetime(2022, 12, 21, tz="America/Santiago"),
    catchup=False,
    tags=["OPS","AWS","ETL", "unimarc", "forecast_and_planning", "MELI", "obtencion stock"],
) as dag:

    dag.doc_md = """
    Obtención de stock en base a documento, transformarlos y \n
    exportar a tabla de BDD.
    """ 

    t0 = PythonOperator(
        task_id = "obtener_stock_general",
        python_callable = get_stock,
    )

