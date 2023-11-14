from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.netezza_utils import load_custom_query_to_s3

import pendulum

def _load_limite_compra_dw_table(ti,ds):
    import pandas as pd
    import sqlalchemy
    
    limit_file = ti.xcom_pull(key="return_value", task_ids=["load_custom_query_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+limit_file)
    if not s3_hook.check_for_key(limit_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % limit_file)

    limit_object = s3_hook.get_key(limit_file, bucket_name=s3_bucket)

    df = pd.read_csv(limit_object.get()["Body"])
    
    print(f"Number of records extracted: {len(df.index)}")

    # Rename columns to match workspace schema:
    columns_rename = {
            "EAN" : "ean",
            "NM" : "nombre_producto",
            "SKU_PRODUCT" : "material",
            "AVG_PRODUCT" : "unidad_promedio_orden",
            "PURCHASE_LIMIT": "limite_compra",
            "AVG_WEIGHT": "peso_promedio_orden",
            "UNIDAD_MEDIDA" : "unidad_medida"
    }
    df = df.rename(columns=columns_rename)

    df = df.astype({
        "ean": "string",
        "nombre_producto": "string",
        "material": "string"
    }, errors="ignore")

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="limite_compra_dw",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')
    print("Data loaded to Postgres")

    return

def set_lim_compra(ti):
    import requests

    limite_query = """Select 
                    from ecommdata.limite_compra_dw lcd
                    left join skus s on s.erp_id = LPAD(lcd.material, 18, '0')                      
    """

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    # Creación de big-json
    jst = []
    for x in lista_ref_id:
        item = {
            "item_id": x,
            "attributes": [
                {
                    "id": str(Variable.get("JANIS_REF_ID_ATRIBUTO_ID_CATEGORIA")),
                    "values": ["12"]
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

    # Seteo vía API al atriubuto limite de compra de la lista de refid
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
            raise ValueError("Janis API response != 200")
    print("La carga de límites a finalizado")          
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_limite_compra_dw',
    default_args=default_args,
    description="Extraer promedio de unidades por orden de Datawarehouse y setear limite de compra en JANIS",
    schedule_interval="30 8 1 1-12 *",
    start_date=pendulum.datetime(2023, 6, 1, tz="America/Santiago"),
    catchup=False,
    tags=["ecommdata", "VTEX", "promociones", "unimarc","workflow"],
) as dag:
    
    dag.doc_md = """
    construir y cargar promociones diarias de VTEX. \n
    Upsert en tabla ecommdata.promociones_diarias.
    """ 

    t0 = PythonOperator(
        task_id = "load_custom_query_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """SELECT
                            LPAD(fvt.EAN, 18, '0'),
                            dph.NM,
                            dph.SKU_PRODUCT,
                            ROUND(AVG(fvt.CANTIDAD_UNIDADES)) AS AVG_PRODUCT,
                            CASE 
                                WHEN fvt.UNIDAD_MEDIDA IN ('KG', 'KGV') THEN 0
                                ELSE ROUND(AVG(fvt.CANTIDAD_UNIDADES) + STDDEV_POP(fvt.CANTIDAD_UNIDADES))
                            END AS PURCHASE_LIMIT,
                            round(AVG(fvt.PESO))/1000 AS AVG_WEIGHT,
                            fvt.UNIDAD_MEDIDA
                        FROM
                            DWC_SMU.SMU.VW_FACT_VENTA_ITEM fvt
                        LEFT JOIN
                            DWC_SMU.SMU.VW_DIM_PRODUCT_HIERARCHY dph ON fvt.EAN = dph.EAN 
                        WHERE
                            FVT.FECHA_HORA >= current_date - 30
                        GROUP BY
                            fvt.EAN, dph.NM, dph.SKU_PRODUCT,fvt.UNIDAD_MEDIDA 
                        ORDER BY
                            fvt.EAN;
            """,
            "query_name": "limite_compra_dw",
        }
    )

    t1 = PostgresOperator(
        task_id = "clear_table",
        postgres_conn_id="postgresql_conn",
        sql="""
        truncate ecommdata.limite_compra_dw
        """
    )

    t2 = PythonOperator(
        task_id = "load_limite_compra_dw_table",
        python_callable = _load_limite_compra_dw_table
    )

    t0 >> t1 >> t2