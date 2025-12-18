from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_alvi_utils import incremental_unixtime_load_table_s3, load_custom_query_to_s3
from utils.postgres_utils import get_max_updated_at_value
from utils.slack_utils import dag_failure_slack, dag_success_slack

from datetime import datetime

import pendulum

def _incremental_load_ordes_table(ti):
    import numpy as np
    import pandas as pd
    
    orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    # Select only relevant columns:
    df = df[["seq_id",
            "id",
            "vtex_id",
            "ecommerce_account",
            "website_name",
            "customer",
            "customer_address",
            "store",
            "product_qty",
            "product_qty_picked",
            "product_substituted_qty",
            "product_substitute_qty",
            "product_qty_missing",
            "items_qty",
            "items_qty_picked",
            "items_substituted_qty",
            "items_substitute_qty",
            "items_qty_missing",
            "total_original",
            "total_discount",
            "total_changes",
            "invoice_ammount",
            "total_shipping",
            "status",
            "status_vtex",
            "date_created",
            "invoice_date",
            "date_picked",
            "date_modified",
            "picker",
            "invoice_number"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "seq_id": "id",
        "id": "janis_id",
        "website_name": "nombre_website",
        "customer": "id_cliente_janis",
        "customer_address": "id_direccion_cliente_janis",
        "store": "id_tienda_janis",
        "product_qty": "productos_solicitados",
        "product_qty_picked": "productos_facturados",
        "product_substituted_qty": "productos_substituidos",
        "product_substitute_qty": "productos_substitutos",
        "product_qty_missing": "productos_faltantes",
        "items_qty": "unidades_solicitadas",
        "items_qty_picked": "unidades_facturadas",
        "items_substituted_qty": "unidades_sustituidas",
        "items_substitute_qty": "unidades_sustitutas",
        "items_qty_missing": "unidades_faltantes",
        "total_original": "venta_creada_bruta",
        "total_discount": "total_descuento_bruto",
        "total_changes": "total_cambios_bruto",
        "invoice_ammount": "venta_facturada_bruta",
        "total_shipping": "cobro_despacho_bruto",
        "status": "estado_janis",
        "status_vtex": "estado_vtex",
        "date_created": "fecha_creacion",
        "invoice_date": "fecha_facturacion",
        "date_picked": "fecha_picking",
        "date_modified": "fecha_modificacion",
        "picker": "id_picker",
        "invoice_number": "documento_electronico"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
    df["canal_venta"] = ""
    df["id_cliente_vtex"] = ""
    df["cod_tienda"] = ""
    df["nombre_tienda"] = ""
    df["venta_creada_neta"] = df["venta_creada_bruta"]/1.19
    df["total_descuento_neto"] = df["total_descuento_bruto"]/1.19
    df["total_cambios_neto"] = df["total_cambios_bruto"]/1.19
    df["venta_facturada_neta"] = df["venta_facturada_bruta"]/1.19
    df["cobro_despacho_neto"] = df["cobro_despacho_bruto"]/1.19
    df["nombre_picker"] = ""
    df["rut_picker"] = ""
    df["empresa_picker"] = ""
    df["fecha_creacion"] = pd.to_datetime(df["fecha_creacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_facturacion"] = pd.to_datetime(df["fecha_facturacion"], unit="s").dt.date
    df["fecha_picking"] = pd.to_datetime(df["fecha_picking"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    
    # Replace non-numeric picker's ids with NULL
    df["id_picker"] = pd.to_numeric(df["id_picker"], errors="coerce")

    # Cast numeric values to int
    df = df.round({
        "venta_creada_neta": 0,
        "total_descuento_neto": 0,
        "total_cambios_neto": 0,
        "venta_facturada_neta": 0,
        "cobro_despacho_neto": 0
    })

    df["venta_creada_neta"] = df["venta_creada_neta"].fillna(0)
    df["total_descuento_neto"] = df["total_descuento_neto"].fillna(0)
    df["total_cambios_neto"] = df["total_cambios_neto"].fillna(0)
    df["venta_facturada_neta"] = df["venta_facturada_neta"].fillna(0)
    df["cobro_despacho_neto"] = df["cobro_despacho_neto"].fillna(0) 

    df = df.astype({
        "venta_creada_neta": "int",
        "total_descuento_neto": "int",
        "total_cambios_neto": "int",
        "venta_facturada_neta": "int",
        "cobro_despacho_neto": "int",
        "fecha_creacion": "string",
        "fecha_facturacion": "string",
        "fecha_picking": "string",
        "fecha_modificacion": "string",
        "id_picker": "int",
        "documento_electronico": "int64"
    }, errors="ignore")

    columns = [
        "janis_id",
        "vtex_id",
        "ecommerce_account",
        "nombre_website",
        "id_cliente_janis",
        "id_direccion_cliente_janis",
        "id_tienda_janis",
        "productos_solicitados",
        "productos_facturados",
        "productos_substituidos",
        "productos_substitutos",
        "productos_faltantes",
        "unidades_solicitadas",
        "unidades_facturadas",
        "unidades_sustituidas",
        "unidades_sustitutas",
        "unidades_faltantes",
        "venta_creada_bruta",
        "total_descuento_bruto",
        "total_cambios_bruto",
        "venta_facturada_bruta",
        "cobro_despacho_bruto",
        "estado_janis",
        "estado_vtex",
        "fecha_creacion",
        "fecha_facturacion",
        "fecha_picking",
        "fecha_modificacion",
        "canal_venta",
        "id_cliente_vtex",
        "cod_tienda",
        "nombre_tienda",
        "venta_creada_neta",
        "total_descuento_neto",
        "total_cambios_neto",
        "venta_facturada_neta",
        "cobro_despacho_neto",
        "nombre_picker",
        "rut_picker",
        "empresa_picker",
        "fecha_modificacion_unixtime",
        "id_picker",
        "documento_electronico"
    ]

    df = df[["id"]+columns]

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
        INSERT INTO ecommdata_alvi.ordenes_janis (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
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

def _get_order_marketing_data(ti, ts):
    import pandas as pd

    orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")

    order_ids = df["id"].tolist()

    iter_size = 500
    iterations = (len(order_ids) // iter_size) + 1

    s3_object_list = []

    for i in range(iterations):
        order_ids_sublist = order_ids[i*iter_size:(i+1)*iter_size]
        order_ids_string = f"({','.join([str(order_id) for order_id in order_ids_sublist])})"
        order_marketing_data_query = f"""
            SELECT *
            FROM janis_alvicl.wms_order_marketing_data womd
            WHERE womd.order_id in {order_ids_string};
        """
        print(order_marketing_data_query)
        s3_object_name = load_custom_query_to_s3(ts, 
                                                order_marketing_data_query, 
                                                "wms_order_marketing_data", 
                                                extra_prefix=str(i))
        s3_object_list.append(s3_object_name)

    return s3_object_list
        
def _update_orders_sales_channel(ti):
    import pandas as pd
    order_marketing_data_files = ti.xcom_pull(key="return_value", task_ids=["get_order_marketing_data"])[0]
    print(order_marketing_data_files)

    if order_marketing_data_files is None:
        print("No records found.")
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    for mkt_data_file in order_marketing_data_files:
        print("Searching file: "+mkt_data_file)
        if not s3_hook.check_for_key(mkt_data_file, bucket_name=s3_bucket):
            raise Exception("Key %s does not exist." % mkt_data_file)

        orders_object = s3_hook.get_key(mkt_data_file, bucket_name=s3_bucket)

        df = pd.read_csv(orders_object.get()["Body"])

        df = df[["order_id", "utm_source"]].dropna()
        records = df.to_dict("records")

        records = [str((record["order_id"],record["utm_source"])) for record in records]
        records_string = ",".join(records)

        update_query = f"""
            UPDATE ecommdata_alvi.ordenes_janis as oj SET
                canal_venta = data.canal_venta
            FROM ( VALUES
                {records_string}
            ) as data(id_orden, canal_venta)
            WHERE oj.janis_id = data.id_orden ;
        """

        print(update_query)
        pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
        pg_connection = pg_hook.get_conn()
        cursor = pg_connection.cursor()
        cursor.execute(update_query)
        pg_connection.commit()
        cursor.close()
        pg_connection.close()
        print("Data uploaded in Postgres")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_ordenes_janis_alvi_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla ordenes desde Janis Alvi Replica hasta Workspace.",
    schedule_interval="30 * * * *",
    start_date=pendulum.datetime(2022, 5, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata_alvi", "ordenes_janis", "Alvi", "MATIAS"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de ordenes de Janis Alvi a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata_alvi",
            "table_name": "ordenes_janis", 
            "updated_at_field": "fecha_modificacion_unixtime",
            "is_unixtime": True
        }
    )

    t1 = PythonOperator(
        task_id = "incremental_unixtime_load_table_to_s3",
        python_callable = incremental_unixtime_load_table_s3,
        op_kwargs = {
            "table_name": "wms_orders", 
            "xcom_updated_date_task_id": "get_max_updated_at_date", 
            "updated_column": "date_modified"
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_orders_alvi_table",
        python_callable = _incremental_load_ordes_table
    )

    t3 = PythonOperator(
        task_id = "get_order_marketing_data",
        python_callable = _get_order_marketing_data
    )

    t4 = PythonOperator(
        task_id = "update_orders_sales_channel",
        python_callable = _update_orders_sales_channel
    )

    t0 >> t1 >> t2 >> t3 >> t4
