from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.janis_utils import incremental_unixtime_load_table_s3, load_full_table_to_s3, load_custom_query_to_s3
from utils.postgres_utils import get_max_updated_at_value

from datetime import datetime

def _incremental_load_orders_table(ti):
    import numpy as np
    import pandas as pd
    
    orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
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
            "call_center_operator_id",
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
        "invoice_number": "documento_electronico"
    }
    df = df.rename(columns=columns_rename)

    # Calculate extra columns:
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
    df["fecha_facturacion"] = pd.to_datetime(df["fecha_facturacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_picking"] = pd.to_datetime(df["fecha_picking"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_modificacion_unixtime"] = df["fecha_modificacion"]
    df["fecha_modificacion"] = pd.to_datetime(df["fecha_modificacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")

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
        "documento_electronico": "int64"
    }, errors="ignore")

    custom_data_fields_full = ti.xcom_pull(key="return_value", task_ids=["order_custom_data_field_full_load"])[0] 
    custom_data_fields_incremental = ti.xcom_pull(key="return_value", task_ids=["order_custom_data_field_incremental_load"])[0]

    custom_data_fields_file = custom_data_fields_full if custom_data_fields_full is not None else custom_data_fields_incremental

    print("Searching file: "+custom_data_fields_file)
    if not s3_hook.check_for_key(custom_data_fields_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % custom_data_fields_file)

    custom_data_fields_object = s3_hook.get_key(custom_data_fields_file, bucket_name=s3_bucket)
    df_cdf = pd.read_csv(custom_data_fields_object.get()["Body"])

    # Filter custom_data_fields_dataframe
    df_cdf_sa = df_cdf[df_cdf["field"] == "sourceApp"]
    df_cdf_sa = df_cdf_sa[["order_id", "value"]]

    df = df.merge(df_cdf_sa, left_on="janis_id", right_on="order_id", how="left")
    df["value"] = df["value"].fillna(0)
    df["value"] = df["value"].astype("int")
    df["canal_venta"] = np.where(df["value"] == 1, "app",
                  np.where((df["call_center_operator_id"].isna()) | (df["call_center_operator_id"] == 0), "sitio", "callcenter"))
    
    df = df.drop(columns=["order_id", "call_center_operator_id", "value"])

    df_cdf_wb = df_cdf[df_cdf["field"] == "wantBags"]
    df_cdf_wb = df_cdf_wb[["order_id", "value"]]

    df = df.merge(df_cdf_wb, left_on="janis_id", right_on="order_id", how="left")
    df["value"] = df["value"].fillna("no")
    df["requiere_bolsas"] = np.where(df["value"] == "si", True, False)
    
    df = df.drop(columns=["order_id", "value"])

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
        "documento_electronico",
        "requiere_bolsas"
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
        INSERT INTO ecommdata.ordenes_janis (id,"""+columns_query+""") 
        VALUES ("""+values_query+""")
        ON CONFLICT (id)
        DO UPDATE SET ("""+columns_query+""") = ("""+excluded_query+""") 
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

def _evaluate_full_or_incremental_load(ti):
    max_updated_at_value = ti.xcom_pull(key="return_value", task_ids=["get_max_updated_at_date"])[0]
    if max_updated_at_value is None:
        return "order_custom_data_field_full_load"
    else:
        return "order_custom_data_field_incremental_load"

def _order_custom_data_field_incremental_load(ti, ts):
    import pandas as pd
    new_orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+new_orders_file)
    if not s3_hook.check_for_key(new_orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % new_orders_file)

    new_orders_object = s3_hook.get_key(new_orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(new_orders_object.get()["Body"])
    print(f"Number of records found: {len(df.index)}")

    new_order_ids = df["id"].tolist()
    if len(new_order_ids) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    new_order_ids_string = "("+",".join([str(order_id) for order_id in new_order_ids])+")"

    janis_query = f"""
        SELECT *
        FROM janis_jackie.wms_order_custom_data_fields AS wocdf
        WHERE wocdf.order_id IN {new_order_ids_string};
    """
    print(janis_query)

    file_name = load_custom_query_to_s3(ts, query=janis_query, query_name="wms_orders_custom_data_fields")

    return file_name

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_ordenes_janis_incremental_load',
    default_args=default_args,
    description="Extracción y carga de tabla ordenes desde Janis Replica hasta Workspace.",
    schedule="*/30 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "ordenes_janis", "unimarc", "cyber"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de ordenes de Janis a Workspace. \n
    UPSERT incremental basado en fecha_modificacion_unixtime.
    """ 
    t0 = PythonOperator(
        task_id = "get_max_updated_at_date",
        python_callable = get_max_updated_at_value,
        op_kwargs = {
            "schema": "ecommdata",
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
        task_id = "incremental_load_orders_table",
        python_callable = _incremental_load_orders_table,
        trigger_rule = "none_failed"
    )

    t3 = BranchPythonOperator(
        task_id = "evaluate_full_or_incremental_load",
        python_callable = _evaluate_full_or_incremental_load
    )

    t4 = PythonOperator(
        task_id = "order_custom_data_field_full_load",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_order_custom_data_fields"}

    )

    t5 = PythonOperator(
        task_id = "order_custom_data_field_incremental_load",
        python_callable = _order_custom_data_field_incremental_load
    )

    t0 >> t1
    t1 >> t3 >> [t4, t5]
    t4 >> t2
    t5 >> t2
