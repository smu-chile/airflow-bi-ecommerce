from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from utils.janis_utils import _execute_mariadb_query
from utils.slack_utils import dag_success_slack, dag_failure_slack

import pendulum

def get_ppum_data_from_janis(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO
    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"atributos_janis/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")


    query = """
    SELECT
        s.ref_id,
        CASE
            WHEN mu.id = 14 AND ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) < 1 THEN ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) * 1000
            WHEN mu.id = 2 AND ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) < 1 THEN ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) * 1000
            ELSE ROUND((s.unit_multiplier * s.unit_multiplier_un), 2)
        END AS unit,
        CASE
            WHEN mu.id = 14 AND ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) < 1 THEN 'ml'
            WHEN mu.id = 2 AND ROUND((s.unit_multiplier * s.unit_multiplier_un), 2) < 1 THEN 'g'
            ELSE mu.ref_unit
        END AS unit_type
    FROM
        skus s
    LEFT JOIN
        measurement_units mu ON s.measurement_unit_un = mu.id
    WHERE
        mu.id IN (14,2)
        and unit_multiplier is not null
        and unit_multiplier_un is not null;
    """

    results, columns = _execute_mariadb_query(query)

    df = pd.DataFrame(results, columns=columns)

    df.info()

    # Create a DataFrame to store the categorized items
    df_atributos = pd.DataFrame(columns=['ref_id', 'attribute'])

    # Dictionary for attribute mappings
    attribute_mapping = {
        'g': {
            (0,200): 'hasta 200 g',
            (201,249):'201 a 400 g',
            (250): '250 g',
            (251,399):'201 a 400 g',
            (400): '400 g',
            (401,499):'401 a 600 g',
            (500): '500 g',
            (501,600):'401 a 600 g',
            (601,800):'601 a 800 g',
            (801, 999):'801 a 1000 g',
        }
        ,'Kg': {
            (1): '1 Kg',
            (1.001, 1.2): '1 a 1.2 Kg',
            (1.201, 1.4): '1.201 a 1.4 Kg',
            (1.401, 1.6): '1.401 a 1.6 Kg',
            (1.601, 1.8): '1.601 a 1.8 Kg',
            (1.801, 1.999): '1.801 a 2 Kg',
            (2): '2 Kg',
            (3): '3 Kg',
            (5): '5 Kg',
            (7): '7 Kg',
            (9): '9 Kg',
            (15): '15 Kg',
            (18): '18 Kg'
        }
        ,'L': {
            (1): '1 L',
            (1.201, 1.4): '1.201 a 1.4 L',
            (1.401, 1.499): '1.401 a 1.6 L',
            (1.5): '1.5 L',
            (1.501, 1.600): '1.401 a 1.6 L',
            (1.601, 1.749): '1.601 a 1.8 L',
            (1.75): '1.75 L',
            (1.751, 1.800): '1.601 a 1.8 L',
            (1.801, 1.999): '1.801 a 2 L',
            (2): '2 L',
            (2.5): '2.5 L',
            (3): '3 L',
            (5): '5 L',
            (6): '7 L'
        }
        ,'ml': {
            (0,199): 'hasta 200 ml',
            (200): '200 ml',
            (201,249):'201 a 400 ml',
            (250): '250 ml',
            (251,299):'201 a 400 ml',
            (300): '300 ml',
            (301,349):'201 a 400 ml',
            (350): '350 ml',
            (351,400):'201 a 400 ml',
            (401,499):'401 a 600 ml',
            (500): '500 ml',
            (501,599):'401 a 600 ml',
            (600): '600 ml',
            (601,749):'601 a 800 ml',
            (750): '750 ml',
            (751,800):'601 a 800 ml',
            (801, 999):'801 a 1000 ml',
        }
        
    }
    df_atributos = pd.DataFrame(columns=['ref_id', 'attribute'])
    df_atributos['ref_id'] = df['ref_id']

    def map_attribute(row):
        unit_type = row['unit_type']
        unit_value = row['unit']

        mapping = attribute_mapping.get(unit_type, {})
        for value_range, attribute in mapping.items():
            if isinstance(value_range, tuple):
                if value_range[0] <= unit_value <= value_range[1]:
                    return attribute
            else:
                if unit_value == value_range:
                    return attribute

        return None
    df_atributos['attribute'] = df.apply(map_attribute, axis=1)
    df_atributos = df_atributos.dropna(subset=['attribute'])

    buffer = io.StringIO()
    df_atributos.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"atributos_janis/{exec_date}/atributo_contenido_{date_aux}.csv"
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



def set_atributo_contenido(ti):
    import pandas as pd
    import requests

    filename = ti.xcom_pull(key="return_value", task_ids=["get_ppum_data_from_janis"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_contenido_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_contenido_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df.info()

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    
    jst = []
    for _, row in df.iterrows():
        item = {
            "item_id": str(row["ref_id"]),
            "attributes": [
                {
                    "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_CONTENIDO")),
                    "values": [str(row["attribute"])]
                }
            ]
        }
        jst.append(item)

    # Partición de big-json
    lim_json = 500
    total_size = len(jst)
    if total_size > lim_json:
        jst = [jst[i:i+lim_json] for i in range(0, len(jst), lim_json)]
    else:
        jst = [jst]
    
    API_JANIS = Variable.get("JANIS_API_URL")
    cargando = 0
    for arr_dic in jst:
        r = requests.post(f'{API_JANIS}attribute_value', headers = headers, json=arr_dic)
        cargando += len(arr_dic )
        if r.status_code == 200:
            print(f"Productos actualizados: {cargando} de {total_size} con EXITO")
        else:
            print(f"Carga sin éxito | Status_Code: {r.status_code} ")
            print(f"Response Print: {r.content}")
            continue
    print("La carga de atributo contenido a finalizado")      
    return


default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'proc_contenido_ppum_janis.py',
    default_args=default_args,
    description="""""",
    schedule_interval="0 7 * * MON",
    start_date = pendulum.datetime(2023, 3, 8, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    tags=["janis", "ppum", "ecommdata_unimarc", "atributos_producto", "SERGIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    t0 = PythonOperator(
        task_id='get_ppum_data_from_janis',
        python_callable=get_ppum_data_from_janis
    )

    t1 = PythonOperator(
        task_id='set_atributo_contenido',
        python_callable=set_atributo_contenido
    )

    t0 >> t1
