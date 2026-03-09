from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, BranchPythonOperator

from datetime import datetime
import pendulum

def _check_time(ts):
    exec_datetime = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')
    exec_datetime_utc = pendulum.timezone("utc").convert(exec_datetime)
    local_tz = pendulum.timezone("America/Santiago")
    exec_datetime_local = local_tz.convert(exec_datetime_utc)
    time_str = exec_datetime_local.strftime('%H%M')
    print(f"Local execution time: {exec_datetime_local.strftime('%Y/%m/%d %H:%M:%S')}")
    if int(time_str[:2]) > 23 or int(time_str[:2]) < 12:
        print("Outside execution hours. Skipping tasks.")
        return "skip_dag_run"
    else:
        print("Expected time range. Executing tasks.")
        return "check_if_dag_ran_today"

def _check_if_dag_ran_today(ds):
    exec_date_string = ds.replace("-", "/")
    response_files_path = f"rappi/api/stock/post/full/responses/{exec_date_string}/"

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching prefix: "+response_files_path)
    if not s3_hook.check_for_prefix(bucket_name=s3_bucket, prefix=response_files_path, delimiter="/"):
        print("Response prefix not found.\nExecuting a FULL LOAD...")
        return "calculate_full_request_body"
    else:
        print("Response prefix found.\nExecuting a DELTA LOAD...")
        return "calculate_delta_request_body"


def _calculate_delta_request_body(ds, ts):
    import json
    import os
    import pandas as pd
    import pytz

    store_id_list = [
        '0469',
        '0903',
        '0333',
        '0931',
        '0717',
        '0017',
        '0681',
        '0030',
        '0375',
        '0761',
        '0345',
        '0344',
        '0917',
        '0336',
        '0956',
        '0332',
        '0581',
        '0914',
        '0111',
        '0445',
        '0953',
        '0025',
        '0086',
        '0626',
        '0051',
        '0028',
    ]

    curr_working_directory = os.getcwd()
    print(os.getcwd())
    exec_datetime = datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')
    localtimezone = pytz.timezone("America/Santiago")
    exec_datetime = exec_datetime.replace(tzinfo=pytz.utc).astimezone(localtimezone)
    exec_datetime = exec_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    with open(curr_working_directory+f"/dags/unimarc/sql/rappi_stock_delta_load.sql", "r") as query_file:
        rappi_stock_query = query_file.read()
    
    rappi_stock_query = rappi_stock_query.replace("{ds}", ds)
    rappi_stock_query = rappi_stock_query.replace("{ts}", exec_datetime)

    print("Base query:")
    print(rappi_stock_query)

    store_body_file_paths = []
    exec_datetime_string = exec_datetime[:16].replace("-", "/").replace("T", "/").replace(":", "")
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()

    for store_id in store_id_list:
        body_file_path = f"rappi/api/stock/post/delta/requests/{exec_datetime_string}_{store_id}"

        # Check if file is already loaded
        s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
        if s3_hook.check_for_key(body_file_path, bucket_name=s3_bucket):
            print(f"File {body_file_path} already exists on S3 bucket. Skipping...")
            continue

        rappi_stock_query_store = rappi_stock_query.replace("{store_id}", store_id)

        df_store = pd.read_sql_query(rappi_stock_query_store, pg_connection)

        print(f"Number of records found: {len(df_store.index)} for store: {store_id}")
        if len(df_store.index) == 0:
            print("NO RECORDS FOUND.")
            continue

        df_store = df_store.dropna()
        df_store["id"] = df_store["id"].astype("int").astype("str")
        df_store["price"] = df_store["price"].astype("int")
        df_store["discount_price"] = df_store["discount_price"].astype("int")
        df_store["is_available"] = True
        dict_body = df_store.to_dict(orient="records")
        json_body = json.dumps(dict_body)

        store_body_file_path = body_file_path + ".json"

        s3_hook.load_string(json_body,
                    key=store_body_file_path,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print("Request payload saved to S3.")
        
        store_body_file_paths.append(store_body_file_path)

    return store_body_file_paths

def _calculate_full_request_body(ts):
    import jaydebeapi
    import json
    import os
    import pandas as pd

    exec_datetime_string = ts[:10].replace("-", "/")
    store_id_list = [
        "0332", "0343", "0402", "0982", "0011",
        "0375", "0022", "0030", "0086", "0046",
        "0111", "0062", "0345", "0336", "0602",
        "0058", "0953", "0009", "0477", "0344",
        "0331", "0626", "0025", "0028", "0980",
        "0326", "0475", "0476", "0357", "0903",
        "0325", "0717", "0353", "0087", "0763",
        "0961", "0328", "0916", "0683", "0017",
        "0956", "0333", "0469", "0917", "0355",
        "0761", "0939", "0645", "0581", "0931",
        "0027", "0759", "0958", "0340", "0458",
        "0051", "0644", "0008", "0714", "0954",
        "0912", "0681", "0978", "0914", "0923",
        "0960", "0957", "0905", "0445", "0902",
        "0926",
    ]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    dsn_database = Variable.get("DW_SECRET_DATABASE") 
    dsn_hostname = Variable.get("DW_SECRET_HOSTNAME")
    dsn_port = "5480" 
    dsn_uid = Variable.get("DW_SECRET_USER")
    dsn_pwd = Variable.get("DW_PASSWORD")
    jdbc_driver_name = "org.netezza.Driver" 
    jdbc_driver_loc = os.path.join('/opt/airflow/include/jdbcdriver/nzjdbc.jar')

    connection_string = 'jdbc:netezza://' + dsn_hostname + ':' + dsn_port + '/' + dsn_database
    conn = jaydebeapi.connect(jdbc_driver_name, connection_string, {'user': dsn_uid, 'password': dsn_pwd},jars=jdbc_driver_loc)
    cur = conn.cursor()

    for store_id in store_id_list:
        body_file_path = f"rappi/api/stock/post/full/requests/{exec_datetime_string}/{store_id}.json"
        
        # Check if file is already loaded
        if s3_hook.check_for_key(body_file_path, bucket_name=s3_bucket):
            print(f"File {body_file_path} already exists on S3 bucket. Skipping...")
            continue
        
        stock_query = f"""
            SELECT P.ean AS ean --ean (PRIMARIO) ean_ppal / UPC
                    , CASE
                        WHEN P.CONT_CONV_UMB > 1 THEN CAST(CAST(sa.sku_product AS int) AS varchar(25)) || '_' || P.CONT_CONV_UMB
                        ELSE CAST(CAST(sa.sku_product AS int) AS varchar(25)) --id (sin ceros a la izquierda)
                    END AS id
                    , precio.PRECIO_MODAL AS price -- price
                    , CASE WHEN WF.PRECIO IS NULL THEN precio.PRECIO_MODAL 
                            ELSE WF.PRECIO END AS discount_price  --discount_price
                    , FLOOR(NBR_ITM / P.CONT_CONV_UMB) AS stock --stock
                    , ou.ou_id AS store_id --probar desde dim_store
                    , P.NM AS name -- name
                    , P.BRAND_DESC AS trademark -- trademark desde DIM_SKU_ATTR
                    , CASE 
                        WHEN p.unidad_de_medida IN ('KG', 'KGV') THEN 'WW'
                        ELSE 'U'
                    END AS sale_type -- sale_type U, WW
            FROM DWC_SMU.SMU.VW_FACT_STOCK S
            LEFT JOIN DWC_SMU.SMU.VW_DIM_SKU_ATTR SA ON SA.SKU_KEY  = S.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PRODUCT P ON P.SKU_KEY = SA.SKU_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ORGANIZATION_UNIT OU ON OU.OU_KEY = S.OU_KEY --probar contra dim_store
            LEFT JOIN DWC_SMU.SMU.VW_DIM_ALMACEN A ON A.ALMACEN_KEY =S.ALMACEN_KEY
            LEFT JOIN DWC_SMU.SMU.VW_DIM_PARTICULARIDAD PART ON S.PARTICULARIDAD_KEY =PART.PARTICULARIDAD_KEY
            INNER JOIN (SELECT _t.FECHA_CARGA
                                , LPAD(_t.CODIGO_MATERIAL , 18, 0) AS material
                                , CASE WHEN _t.UMV = 'UN' THEN 'ST' ELSE _t.UMV END AS UMV
                                , _t1.PRECIO_MODAL
                        FROM (SELECT MAX(FECHA_CARGA) AS FECHA_CARGA
                                        , CODIGO_MATERIAL
                                        , UMV
                                FROM NZ_BU.ECOMERCE.VW_POSC_ACT_H_PRECIO_MODAL_UNI
                                GROUP BY CODIGO_MATERIAL, UMV) _t
                        INNER JOIN NZ_BU.ECOMERCE.VW_POSC_ACT_H_PRECIO_MODAL_UNI _t1
                                ON _t.FECHA_CARGA=_t1.FECHA_CARGA
                                    AND _t.CODIGO_MATERIAL=_t1.CODIGO_MATERIAL
                                    AND _t.UMV=_t1.UMV) precio
                            ON precio.MATERIAL = SA.SKU_PRODUCT
                            AND precio.umv = p.UNIDAD_DE_MEDIDA
            LEFT JOIN (SELECT EAN
                                , min(PRECIO_PROMOCIONAL) AS PRECIO
                        FROM NZ_BU.ECOMERCE.VW_WORKFLOW
                        WHERE FECHA_INICIO_DE_PROMOCION <= TO_CHAR(NOW(),'YYYY-MM-DD')
                        AND FECHA_FIN_DE_PROMOCION >= TO_CHAR(NOW(),'YYYY-MM-DD')
                        AND TIPO_PROMOCION IN (1,4)
                        AND REGISTRO_VALIDO = 'X'
                        AND ORGANIZACION_VENTAS = '1000'
                        AND CANAL_DISTRIBUCION = '10'
                        AND (ID_MECANICA NOT IN (25, 26, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99)
                            OR N_PROMOCION IN (
                                5640752022,
                                5640762022,
                                5640772022,
                                5640782022,
                                5640792022,
                                5640802022,
                                5640812022,
                                5552412022,
                                5552422022,
                                5552432022,
                                5630882022,
                                5630892022,
                                5630902022,
                                5631152022,
                                5631162022,
                                5631172022,
                                5631182022,
                                5631192022,
                                5631202022,
                                5631212022
                            )
                        )
                        GROUP BY EAN ) WF ON WF.EAN = P.EAN
            WHERE A.ALMACEN_COD = '0001'
            AND S.APLICA_STOCK = 'S'
            AND DATE_VALUE = TO_CHAR(NOW() - INTERVAL '1 days','YYYY-MM-DD')
            AND OU.OU_ID = '{store_id}'
            AND (P.NLS_PD_DSC IS NOT NULL OR P.UNIDAD_DE_MEDIDA IN ('KG', 'KGV'))
            AND P.UNIDAD_DE_MEDIDA  IS NOT NULL
            AND PART.PARTICULARIDAD_COD = 'A'
            AND S.TIPO_STOCK_KEY IN (9161419180, 9145314683)
            AND FLOOR(NBR_ITM / P.CONT_CONV_UMB) > 0
            AND p.indic_ean_ppal = 'X';
        """
        
        print(f"Ejecutando tienda: {store_id}")
        cur.execute(stock_query)
        results = cur.fetchall()
        columns = [i[0] for i in cur.description]
        df = pd.DataFrame(results, columns=columns)
        df["PRICE"] = df["PRICE"].astype("int")
        df["DISCOUNT_PRICE"] = df["DISCOUNT_PRICE"].astype("int")
        df.columns= df.columns.str.lower()
        df["is_available"] = True

        print(f"Número de registros: {len(df.index)}")
        print(df.columns)

        dict_body = df.to_dict(orient="records")
        json_body = json.dumps(dict_body)

        s3_hook.load_string(json_body,
                    key=body_file_path,
                    bucket_name=s3_bucket,
                    replace=True,
                    encrypt=False)
        print("Request payload saved to S3.")
    
    return

def _stock_and_prices_delta_post_request(ds):
    print("DELTA LOAD")
    import json
    import requests
    
    exec_datetime_string = ds.replace("-", "/")
    prefix = f"rappi/api/stock/post/delta/requests/{exec_datetime_string}/"
    responses_prefix = f"rappi/api/stock/post/delta/responses/{exec_datetime_string}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Number of files found: {len(s3_file_list)}")

    if len(s3_file_list) == 0:
        print("NO FILES FOUND.")
        return

    for body_file in s3_file_list:
        file_name = body_file.split("/")[-1]
        print("Searching file: "+body_file)
        if not s3_hook.check_for_key(body_file, bucket_name=s3_bucket):
            raise Exception("Key %s does not exist." % body_file)

        json_body_object = s3_hook.get_key(body_file, bucket_name=s3_bucket)
        json_body_string = json_body_object.get()["Body"].read()
        json_body = json.loads(json_body_string)
        payload = {
            "type": "delta",
            "records": json_body
        }

        print(f"Number of records found: {len(payload['records'])}")

        rappi_endpoint = "https://services.grability.rappi.com/api/cpgs-integration/datasets"

        headres = {
            "api_key": Variable.get("RAPPI_API_KEY"),
            "Content-Type": "application/json"
        }
        response = requests.post(url=rappi_endpoint, json=payload, headers=headres)
        print(response.status_code)
        try:
            response_json = response.json()
            response_string = json.dumps(response_json)
            s3_hook.load_string(response_string,
                  key=responses_prefix+file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
            print("Response body saved to S3.")
        except Exception as e:
            print(e)
            print("Error on response.")
            break
    return

def _stock_and_prices_full_post_request(ti, ds):
    import json
    import requests
    print("FULL LOAD")
    
    exec_datetime_string = ds.replace("-", "/")
    prefix = f"rappi/api/stock/post/full/requests/{exec_datetime_string}/"
    responses_prefix = f"rappi/api/stock/post/full/responses/{exec_datetime_string}/"
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Number of files found: {len(s3_file_list)}")

    if len(s3_file_list) == 0:
        print("NO FILES FOUND.")
        return

    for body_file in s3_file_list:
        file_name = body_file.split("/")[-1]
        print("Searching file: "+body_file)
        if not s3_hook.check_for_key(body_file, bucket_name=s3_bucket):
            raise Exception("Key %s does not exist." % body_file)

        json_body_object = s3_hook.get_key(body_file, bucket_name=s3_bucket)
        json_body_string = json_body_object.get()["Body"].read()
        json_body = json.loads(json_body_string)
        payload = {
            "records": json_body
        }

        print(f"Number of records found: {len(payload['records'])}")
        rappi_endpoint = "https://services.grability.rappi.com/api/cpgs-integration/datasets"

        headres = {
            "api_key": Variable.get("RAPPI_API_KEY"),
            "Content-Type": "application/json"
        }
        response = requests.post(url=rappi_endpoint, json=payload, headers=headres)
        print(response.status_code)
        try:
            response_json = response.json()
            response_string = json.dumps(response_json)
            s3_hook.load_string(response_string,
                  key=responses_prefix+file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)
            print("Response body saved to S3.")
        except Exception as e:
            print(e)
            print("Error on response.")
            break
    return

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    "proc_rappi_post_stock_precio",
    default_args=default_args,
    description="Carga de stock y precios regulares a través de la API de Rappi.",
    schedule=None, 
    start_date=pendulum.datetime(2022, 11, 2, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,

    tags=["OPS", "Rappi", "API", "POST", "delta", "stock", "precios"],
) as dag:

    dag.doc_md = """
    Envía stock y precios regulares para cada SKU perteneciente a las tiendas presentes en Rappi \n
    Por cada tienda presente en Rappi (esto es, que tengan id_rappi no nulo en ecommdata.tiendas), se 
    obtiene el stock y precios regulares desde ecommdata.stock y ecommdata.precios y se envían a un
    endpoint de Rappi mediante una POST request.\n
    Este proceso depende del DAG *etl_stock_incremental_load*.\n
    - Delta load: las cargas siguentes del día deben representar la variación de stock.
    """ 

    t0 = BranchPythonOperator(
        task_id = "check_time",
        python_callable = _check_time
    )

    # t1 = BranchPythonOperator(
    #     task_id = "check_if_dag_ran_today",
    #     python_callable = _check_if_dag_ran_today,
    # )

    # t2 = PythonOperator(
    #     task_id = "calculate_full_request_body",
    #     python_callable = _calculate_full_request_body
    # )

    t3 = PythonOperator(
        task_id = "calculate_delta_request_body",
        python_callable = _calculate_delta_request_body
    )

    # t4 = PythonOperator(
    #     task_id = "stock_and_prices_full_post_request",
    #     python_callable = _stock_and_prices_full_post_request,        
    # )

    t5 = PythonOperator(
        task_id = "stock_and_prices_delta_post_request",
        python_callable = _stock_and_prices_delta_post_request,
    )

    td = EmptyOperator(
        task_id = "skip_dag_run"
    )

    t0 >> [t3, td]
    t3 >> t5 
