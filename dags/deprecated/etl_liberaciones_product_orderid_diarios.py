from airflow import DAG
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import pendulum

def _liberacion_diara(ts):
    import pandas as pd
    import numpy as np
    import requests
    import time

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
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type" : "application/json",}


    headers = { 
        'accept': 'application/json',
        "Authorization": f"Bearer {bearer_token}",
    }


    response = requests.get('https://api.mercadopago.com/v1/account/release_report/list', headers=headers)
    print (response.json()[0])
    filename = response.json()[0]['file_name']
    print (response.json()[0]['file_name'])

    headers = {
        "Authorization": f"Bearer {bearer_token}",
    }

    response = requests.get(Variable.get('MERCADOPAGO_API_RELEASE_REPORT').format(filename), headers=headers)

    from io import StringIO
    buffer = StringIO(response.text)
    try:
        df_releases = pd.read_csv(buffer, sep=';')
    except Exception as e:
        print (str(e)[:3000])
    
    columns_main_release = [
        'fecha_de_liberacion', 
        'id_de_operacion_en_mercado_pago', 
        'numero_de_identificacion',
        'tipo_de_registro', 
        'descripcion', 
        'monto_neto_acreditado',
        'monto_neto_debitado', 
        'monto_bruto_de_la_operacion', 
        'monto_recibido_por_compras_por_split',
        'comision_de_mercado_pago_o_mercado_libre', 
        'comision_por_ofrecer_cuotas_sin_interes',
        'costo_de_envio', 
        'impuestos_cobrados_por_retenciones_iibb', 
        'cupon_de_descuento',
        'cuota',
        'medio_de_pago', 
        'detalle_de_impuestos',
        'impuesto_descontado_del_valor_bruto',
        'fecha_de_aprobacion',
        'id_de_caja',
        'nombre_de_caja',
        'id_de_caja_definido_por_el_usuario', 
        'id_de_la_sucursal',
        'nombre_de_la_sucursal',
        'id_de_sucursal_definido_por_el_usuario',
        'moneda',
        'impuestos_desagregados', 
        'id_del_envio',
        'modo_de_envio',
        'id_de_la_orden',
        'id_del_paquete',
        'datos_extra',
        'costo_por_ofrecer_descuento'
    ]

    df_releases = df_releases.rename(columns={
        'DATE':'fecha_de_liberacion', 
        'SOURCE_ID':'id_de_operacion_en_mercado_pago',
        'EXTERNAL_REFERENCE':'numero_de_identificacion', 
        'RECORD_TYPE':'tipo_de_registro', 
        'DESCRIPTION':'descripcion',
        'NET_CREDIT_AMOUNT':'monto_neto_acreditado', 
        'NET_DEBIT_AMOUNT':'monto_neto_debitado',
        'GROSS_AMOUNT':'monto_bruto_de_la_operacion',
        'SELLER_AMOUNT':'monto_recibido_por_compras_por_split', 
        'MP_FEE_AMOUNT':'comision_de_mercado_pago_o_mercado_libre',
        'FINANCING_FEE_AMOUNT':'comision_por_ofrecer_cuotas_sin_interes',
        'SHIPPING_FEE_AMOUNT':'costo_de_envio',
        'TAXES_AMOUNT':'impuestos_cobrados_por_retenciones_iibb',
        'COUPON_AMOUNT':'cupon_de_descuento',
        'INSTALLMENTS':'cuota',
        'PAYMENT_METHOD':'medio_de_pago',
        'TAX_DETAIL':'detalle_de_impuestos',
        'TAX_AMOUNT_TELCO':'impuesto_descontado_del_valor_bruto',
        'TRANSACTION_APPROVAL_DATE':'fecha_de_aprobacion',
        'POS_ID':'id_de_caja','POS_NAME':'nombre_de_caja',
        'EXTERNAL_POS_ID':'id_de_caja_definido_por_el_usuario',
        'STORE_ID':'id_de_la_sucursal',
        'STORE_NAME':'nombre_de_la_sucursal',
        'EXTERNAL_STORE_ID':'id_de_sucursal_definido_por_el_usuario',
        'CURRENCY':'moneda', 
        'TAXES_DISAGGREGATED':'impuestos_desagregados',
        'SHIPPING_ID':'id_del_envio',
        'SHIPMENT_MODE':'modo_de_envio',
        'ORDER_ID':'id_de_la_orden',
        'PACK_ID':'id_del_paquete',
        'METADATA':'datos_extra',
        'EFFECTIVE_COUPON_AMOUNT':'costo_por_ofrecer_descuento'}
    )

    df_releases = df_releases[columns_main_release]
    df_releases = df_releases[df_releases['tipo_de_registro'] != 'total']
    df_releases = df_releases[df_releases['tipo_de_registro'] != 'subtotal']
    print (df_releases.dtypes)

    columns_query = ",".join(columns_main_release)
    values_query = ",".join(["%s" for column in columns_main_release])
    df_releases = df_releases.fillna("NULL")
    records = list(df_releases.to_records(index=False))

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
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.liberaciones ("""+columns_query+""") 
        VALUES ("""+values_query+""")
"""

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return


def _orderid_packid_table():

    import pandas as pd
    import numpy as np
    import requests

    ### MONGO
    mongo_hook = MongoHook(conn_id="mongodb_meli_conn")
    pipeline_mongo = [{'$project':{
        'id':1, 
        'pack_id':1}}, 
        ]

    mergeids_cursor = mongo_hook.aggregate(
        mongo_db = Variable.get('MELI_ITEMS_DB_MONGO'),
        mongo_collection="orders",
        aggregate_query = pipeline_mongo,
    )

    mergeids = list(mergeids_cursor)    
    if len(mergeids) == 0:
        raise Exception('Error, lista vacía')
    df_packid_orderid = pd.DataFrame(mergeids, dtype='object')

    columns_main = [
        'order_id', 'pack_id'
        ]
    df_packid_orderid = df_packid_orderid.rename(columns={'id':'order_id'})
    df_packid_orderid = df_packid_orderid[columns_main]
    print(df_packid_orderid.dtypes)

    columns_query = ",".join(columns_main)
    values_query = ",".join(["%s" for column in columns_main])
    df_packid_orderid = df_packid_orderid.fillna("NULL")
    df_packid_orderid = df_packid_orderid[df_packid_orderid['pack_id'] != 'nan']
    records = list(df_packid_orderid.to_records(index=False))

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
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata_meli.orderid_packid ("""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (order_id)
        DO NOTHING;;
    """

    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_liberaciones_ids_diarios_MELI',
    default_args=default_args,
    description="Automatización de obtención de liberaciones MELI, y de tabla intermedia pack_id y order_id",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2023, 1, 27, tz="America/Santiago"),
    catchup=False,
    tags=["MELI", "liberaciones", "conciliacion","MongoDB"],
) as dag:

    dag.doc_md = """
    Obtención de liberaciones desde MELI, y obtención de tabla intermedia order_id y pack_id desde MongoDB.
    """ 

    t0 = PythonOperator(
        task_id = "liberacion_diara",
        python_callable = _liberacion_diara,
    )

    t1 = PythonOperator(
        task_id = "tabla_packid_orderid",
        python_callable = _orderid_packid_table,
    )

t0>>t1
