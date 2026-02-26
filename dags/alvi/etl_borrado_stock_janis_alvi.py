from airflow import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pendulum

from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime, timedelta

def _stopper_lista8(ts):

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'CSV'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    query = """
        select count(1) as tiendas_activas
        from ecommdata_alvi.tiendas t
        where t.status = 1 and t.id != '1';
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    active_stores = results[0][0]
    stores_found = len(s3_file_list)
    print(f"active stores: {results}")
    print(f"stores found: {stores_found}")

    if stores_found >= active_stores:
        return
    else:
        raise Exception(f"Not all active stores found")

def _yesterday_stopper_lista8(ts):

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d")
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'CSV'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    query = """
        select count(1) as tiendas_activas
        from ecommdata_alvi.tiendas t
        where t.status = 1 and t.id != '1';
    """

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    active_stores = results[0][0]
    stores_found = len(s3_file_list)
    print(f"active stores: {results}")
    print(f"stores found: {stores_found}")

    if stores_found >= active_stores:
        return
    else:
        raise Exception(f"Not all active stores found")

def _save_lista8_exclusions_in_s3(ts):
    import pandas as pd
    from io import StringIO

    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"datastage/L8_alvi/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    print(f"Files detected today: {s3_file_list}")

    exec_date_y = datetime.strptime(ts[:10], "%Y-%m-%d")
    exec_date_y = exec_date_y.strftime("%Y/%m/%d")
    prefix_y = f"datastage/L8_alvi/{exec_date_y}/"

    s3_file_list_y = s3_hook.list_keys(s3_bucket, prefix=prefix_y)
    print(f"Files detected yesterday: {s3_file_list_y}")

    column_types = {
        "CENTRO_x": "str",
        "MATERIAL":	"str",
        "UM VTA":	"str",
        "DESCRIPCION_x": "str"
    }

    column_names = {
        "CENTRO_x": "id_tienda",
        "MATERIAL":	"material",
        "UM VTA":	"umv",
        "DESCRIPCION_x": "descripcion"
    }

    for s3_file in s3_file_list:
        s3_file_y = ''
        for temp_s3_file_y in s3_file_list_y:
            if temp_s3_file_y[-8:] == s3_file[-8:]:
                s3_file_y = temp_s3_file_y
        if not s3_file.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        if not s3_file_y.endswith((".csv", ".CSV")):
            # Skip empty any non-csv file
            continue
        print(f"Comparing files: {s3_file} and {s3_file_y}")
        
        lista8_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
        dfB = pd.read_csv(lista8_object.get()["Body"], sep=";")
        dfB["STOCK X UMV"] = dfB["STOCK X UMV"].str.replace(',','.')
        
        lista8_object_y = s3_hook.get_key(s3_file_y, bucket_name=s3_bucket)
        dfA = pd.read_csv(lista8_object_y.get()["Body"], sep=";")
        dfA["STOCK X UMV"] = dfA["STOCK X UMV"].str.replace(',','.')

        df = pd.merge(dfA, dfB, on=['MATERIAL', 'UM VTA'], how="outer", indicator=True
              ).query('_merge=="left_only"')

        df = df[['CENTRO_x','MATERIAL', 'UM VTA', 'DESCRIPCION_x']]
        df = df.astype(column_types)

        buffer = StringIO()
        id_tienda = s3_file[-8:-4]
        len_df = str(len(df)).zfill(4)
        file_name = f"borrado_stock_alvi/{exec_date}/{id_tienda}/borrado_stock-{id_tienda}-{len_df}.csv"
        print(f"saving file {file_name}")

        aws_conn_id="aws_s3_connection"
        df.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        s3_hook.load_string(buffer.getvalue(),
                  key=file_name,
                  bucket_name=s3_bucket,
                  replace=True,
                  encrypt=False)

    return

def _send_stock_0_to_janis(ts):
    import requests
    import pandas as pd
    
    exec_date = datetime.strptime(ts[:10], "%Y-%m-%d") + timedelta(days=1)
    exec_date = exec_date.strftime("%Y/%m/%d")
    prefix = f"borrado_stock_alvi/{exec_date}/"
    print(prefix)
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    s3_file_list = list(filter(lambda x: (x[-3:] == 'csv'), s3_file_list))
    print(f"Files detected: {s3_file_list}")

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_ALVI_API_KEY = Variable.get("JANIS_ALVI_API_KEY")
    JANIS_ALVI_API_SECRET = Variable.get("JANIS_ALVI_API_SECRET")
    JANIS_ALVI_CLIENT = Variable.get("JANIS_ALVI_CLIENT")

    headers = {
    "janis-api-key" : JANIS_ALVI_API_KEY,
    "janis-api-secret" : JANIS_ALVI_API_SECRET,
    "janis-client" : JANIS_ALVI_CLIENT,
    "Connection" : "keep-alive"
    }

    

    for s3_file in s3_file_list:
        if (int(s3_file[-8:-4]) < 100):
            payload=[]
            s3_object = s3_hook.get_key(s3_file, bucket_name=s3_bucket)
            df = pd.read_csv(s3_object.get()["Body"], sep=",")
            for ind in df.index:
                material = str(df['MATERIAL'][ind]).zfill(18)
                id_tienda = str(int(df['CENTRO_x'][ind])).zfill(4)
                row = {"IdSku": material, "Quantity": 0, "Store": id_tienda}
                payload.append(row)
            payload = str(payload).replace("'", '"')
            response = requests.request("POST", url, headers=headers, data=payload)
            print(f"[L = {s3_file[-8:-4]} - S = {s3_file[-13:-9]}] response from file {s3_file}:")
            print(response.text)


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
    'etl_borrado_stock_janis_alvi',
    default_args=default_args,
    description="Borrado de stock janis alvi en base a productos removidos de lista8.",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "SAP", "ecommdata_alvi", "lista8", "stock", "janis", "alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Borrado de stock janis alvi en base a productos removidos de lista8."
    """ 
    t0 = S3KeySensor(
        task_id = "wait_for_lista8_flag_file",
        bucket_key = "datastage/L8_alvi/{{(execution_date + macros.timedelta(days=1)).strftime('%Y/%m/%d')}}/LISTA_8A.TRG",
        bucket_name = Variable.get("AWS_S3_BUCKET_NAME"),
        aws_conn_id = "aws_s3_connection",
        timeout = 60*60,
        retries = 3,
        retry_delay = timedelta(minutes=1),
    ) 

    t1 = PythonOperator(
        task_id = "stopper_lista8",
        python_callable = _stopper_lista8
    )

    t1_y = PythonOperator(
        task_id = "yesterday_stopper_lista8",
        python_callable = _yesterday_stopper_lista8
    )

    t2 = PythonOperator(
        task_id = "save_lista8_exclusions_in_s3",
        python_callable = _save_lista8_exclusions_in_s3
    )

    t3 = PythonOperator(
        task_id = "send_stock_0_to_janis",
        python_callable = _send_stock_0_to_janis
    )


    t0 >> t1 >> t1_y >> t2 >> t3
