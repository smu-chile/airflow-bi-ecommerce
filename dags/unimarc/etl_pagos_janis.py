from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import load_custom_query_to_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_success_slack, dag_failure_slack

from datetime import datetime

import pendulum

def _get_correct_max_transaction_date(ti):
    max_transaction_date = ti.xcom_pull(key="return_value", task_ids="get_max_transaction_date")
    if max_transaction_date is None:
        return 0
    return max_transaction_date

def _incremental_load_pagos_janis(ti):
    import numpy as np
    import pandas as pd
    
    attributes_file = ti.xcom_pull(key="return_value", task_ids=["load_incremental_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+attributes_file)
    if not s3_hook.check_for_key(attributes_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % attributes_file)

    attributes_object = s3_hook.get_key(attributes_file, bucket_name=s3_bucket)

    df = pd.read_csv(attributes_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[[
        "id",
        "seq_id",
        "transaction_id",
        "payment_id",
        "tid",
        "merchant_id",
        "transaction_date",
        "payment_system",
        "payment_system_name",
        "payment_group",
        "erp_id",
        "erp_id_bk",
        "value",
        "value_original",
        "reference_value",
        "installments",
        "interest_rate",
        "auth_date",
        "coupon",
        "authorization_code",
        "terminal",
        "batch_number",
        "connector",
        "connector_status",
        "capture_date",
        "capture_amount",
        "gift_card_id",
        "gift_card_caption",
        "gift_card_provider",
        "gift_card_as_discount",
        "flags",
        "status",
        "redemption_code",
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "id": "id",
        "seq_id": "id_orden",
        "transaction_id": "id_transaccion",
        "payment_id": "id_pago",
        "tid": "tid",
        "merchant_id": "id_merchant",
        "transaction_date": "fecha_transaccion_unixtime",
        "payment_system": "id_sistema_de_pago",
        "payment_system_name": "sistema_de_pago",
        "payment_group": "grupo_pago",
        "erp_id": "id_sap",
        "erp_id_bk": "id_sap_bk",
        "value": "valor",
        "value_original": "valor_original",
        "reference_value": "valor_referencia",
        "installments": "cuotas",
        "interest_rate": "tasa_interes",
        "auth_date": "fecha_autorizacion",
        "coupon": "cupon",
        "authorization_code": "codigo_autorizacion",
        "terminal": "terminal",
        "batch_number": "lote",
        "connector": "conector",
        "connector_status": "estado_conector",
        "capture_date": "fecha_captura",
        "capture_amount": "monto_captura",
        "gift_card_id": "id_gift_card",
        "gift_card_caption": "gift_card_descripcion",
        "gift_card_provider": "proveedor_gift_card",
        "gift_card_as_discount": "gift_card_como_descuento",
        "flags": "flags",
        "status": "estado",
        "redemption_code": "codigo_canje"
    }
    df = df.rename(columns=columns_rename)

    df["fecha_transaccion"] = pd.to_datetime(df["fecha_transaccion_unixtime"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_autorizacion"] = pd.to_datetime(df["fecha_autorizacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_captura"] = pd.to_datetime(df["fecha_captura"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")


    df = df.astype({
        "id": "int64",
        "id_orden": "int64",
        "id_transaccion": "string",
        "id_pago": "string",
        "tid": "string",
        "id_merchant": "string",
        "fecha_transaccion": "string",
        "fecha_transaccion_unixtime": "int64",
        "id_sistema_de_pago": "string",
        "sistema_de_pago": "string",
        "grupo_pago": "string",
        "id_sap": "string",
        "id_sap_bk": "string",
        "valor": "int64",
        "valor_original": "int64",
        "valor_referencia": "int64",
        "cuotas": "int64",
        "tasa_interes": "float",
        "fecha_autorizacion": "string",
        "cupon": "int64",
        "codigo_autorizacion": "string",
        "terminal": "string",
        "lote": "string",
        "conector": "string",
        "estado_conector": "int64",
        "fecha_captura": "string",
        "monto_captura": "int64",
        "id_gift_card": "string",
        "gift_card_descripcion": "string",
        "proveedor_gift_card": "string",
        "gift_card_como_descuento": "string",
        "flags": "int64",
        "estado": "int64",
        "codigo_canje": "string"
    }, errors="ignore")

    columns = [
        "id_orden",
        "id_transaccion",
        "id_pago",
        "tid",
        "id_merchant",
        "fecha_transaccion",
        "fecha_transaccion_unixtime",
        "id_sistema_de_pago",
        "sistema_de_pago",
        "grupo_pago",
        "id_sap",
        "id_sap_bk",
        "valor",
        "valor_original",
        "valor_referencia",
        "cuotas",
        "tasa_interes",
        "fecha_autorizacion",
        "cupon",
        "codigo_autorizacion",
        "terminal",
        "lote",
        "conector",
        "estado_conector",
        "fecha_captura",
        "monto_captura",
        "id_gift_card",
        "gift_card_descripcion",
        "proveedor_gift_card",
        "gift_card_como_descuento",
        "flags",
        "estado",
        "codigo_canje",
    ]

    df = df[["id"] + columns]
    columns_query = ",".join(columns)
    excluded_query = ",".join(["EXCLUDED."+column for column in columns])
    values_query = "%s,"+",".join(["%s" for column in columns])
    df = df.fillna("NULL")
    records = list(df.to_records(index=False))
    
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
    print(f"Number of records to load: {str(len(fixed_records))}")
    incremental_query = """
        INSERT INTO ecommdata.pagos_janis (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""");
    """
    print(incremental_query)
    pg_hook = PostgresHook(conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres on ecommdata.pagos_janis")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_pagos_janis_unimarc_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla atributos desde Janis Unimarc Replica hasta Workspace.",
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2023, 3, 27, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "pagos_janis", "Unimarc", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de pagos de Janis Unimarc a Workspace. \n
    INSERT basado en fecha_transaccion.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_transaction_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
            "table_name": "pagos_janis", 
            "updated_at_field": "fecha_transaccion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "check_full_or_incremental_load",
        python_callable = _get_correct_max_transaction_date
    )

    t2 = PythonOperator(
        task_id = "load_incremental_table_to_s3",
        python_callable = load_custom_query_to_s3,
        op_kwargs = {
            "query": """
                SELECT wo.seq_id
                    , wop.*
                FROM janis_jackie.wms_order_payments wop
                JOIN janis_jackie.wms_orders wo
                    on wo.id = wop.order_id 
                WHERE wop.transaction_date > {{ti.xcom_pull(key="return_value", task_ids="check_full_or_incremental_load")}} or SUBSTRING_INDEX(SUBSTRING_INDEX('{{ts}}', 'T', -1), '+', 1) = '03:30:00'
            """,
            "query_name": "wms_order_payments",
        }
    )

    t3 = PythonOperator(
        task_id = "incremental_load_pagos_janis",
        python_callable = _incremental_load_pagos_janis
    )

    t0 >> t1 >> t2 >> t3
