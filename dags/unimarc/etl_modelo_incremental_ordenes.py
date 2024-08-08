from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
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
            "call_center_operator_id",
            "invoice_number",
            "picker",
            "cart_id"
            ]]

    df = df[df["store"] != 38]

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
        "invoice_number": "documento_electronico",
        "picker": "id_picker",
        "cart_id": "janis_cart_id"
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
        "documento_electronico": "int64",
        "id_picker": "int",
        "janis_cart_id": "string"
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
    df["value"] = np.where(df["value"] == "Android", 1, df["value"])
    df["value"] = np.where(df["value"] == "iOS", 1, df["value"])
    df["value"] = df["value"].astype("int")
    df["canal_venta"] = np.where(df["value"] == 1, "app",
                  np.where((df["call_center_operator_id"].isna()) | (df["call_center_operator_id"] == 0), "sitio", "callcenter"))
    
    df = df.drop(columns=["order_id", "call_center_operator_id", "value"])

    df_cdf_cl = df_cdf[df_cdf["field"] == "clientLevel"]
    df_cdf_cl = df_cdf_cl[["order_id", "value"]]
    df = df.merge(df_cdf_cl, left_on="janis_id", right_on="order_id", how="left")
    df["value"] = df["value"].fillna("")
    df["value"] = df["value"].astype("str")
    df["nivel_cliente"] = df["value"]

    df = df.drop(columns=["order_id", "value"])

    marketing_data_fields_file = ti.xcom_pull(key="return_value", task_ids=["order_marketing_data_field_incremental_load"])[0]

    print("Searching file: "+marketing_data_fields_file)
    if not s3_hook.check_for_key(marketing_data_fields_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % marketing_data_fields_file)

    marketing_data_fields_object = s3_hook.get_key(marketing_data_fields_file, bucket_name=s3_bucket)
    df_mdf = pd.read_csv(marketing_data_fields_object.get()["Body"])

    # Filter marketing_data_fields_dataframe
    df_mdf = df_mdf[["order_id", "utm_source"]]

    df = df.merge(df_mdf, left_on="janis_id", right_on="order_id", how="left")
    df["utm_source"] = df["utm_source"].fillna("NULL")
    
    df = df.drop(columns=["order_id"])

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
        "id_picker",
        "janis_cart_id",
        "utm_source",
        "nivel_cliente"
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
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

    return

def _get_new_orders_from_s3(ti):
    import pandas as pd

    orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    df = df[df["store"] != 38]
    print(f"Number of records found: {len(df.index)}")

    return df

def _get_order_items_from_janis(ts, ti):
    # Search based on wms_orders.id
    df = _get_new_orders_from_s3(ti)
    order_ids = df["id"].tolist()
    if len(order_ids) == 0:
        s3_object_name = "empty"
        return s3_object_name
    query_order_ids = "(" + ",".join([str(order_id) for order_id in order_ids]) + ")"
    query = f"""
        SELECT *
        FROM janis_jackie.wms_order_items AS woi
        WHERE woi.order_id IN {query_order_ids} 
    """
    print(query)
    s3_object_name = load_custom_query_to_s3(ts, query, "wms_order_items")
    return s3_object_name


def _order_items_table_incremental_load(ti):
    import numpy as np
    import pandas as pd
    
    df_orders = _get_new_orders_from_s3(ti)
    df_orders = df_orders[["id", "seq_id"]]
    df_orders = df_orders.rename(columns={"id": "original_id"})

    order_items_file = ti.xcom_pull(key="return_value", task_ids=["get_order_items_from_janis"])[0]

    if ti.xcom_pull(key="return_value", task_ids=['get_order_items_from_janis'])[0] == "empty":
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_items_file)
    if not s3_hook.check_for_key(order_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_items_file)

    order_items_object = s3_hook.get_key(order_items_file, bucket_name=s3_bucket)

    column_types = {
        "ref_id": "string",
        "ean": "string",
    } 

    df = pd.read_csv(order_items_object.get()["Body"], dtype=column_types)
    df = df[[
        "id", 
        "order_id",
		"item_index",
		"substitute_of", 
		"sku",
		"product",
		"ref_id",
		"ean",
		"picker",
		"name",
		"list_price",
		"price",
		"selling_price",
		"selling_price_original",
		"quantity",
		"quantity_picked",
		"substitute_type",
		"brand",
		"category",
		"measurement_unit",
		"unit_multiplier",
        "note"
    ]]  

    df = df.merge(df_orders, how="inner", left_on="order_id", right_on="original_id").drop(columns=["order_id", "original_id"])

    # # Ensure correct datatypes:
    df["item_index"] = df["item_index"].astype("int", errors="ignore")
    df["substitute_of"] = df["substitute_of"].astype("int", errors="ignore")
    df["picker"] = df["picker"].astype("int", errors="ignore")
    df["list_price"] = df["list_price"].astype("int", errors="ignore")
    df["price"] = df["price"].astype("int", errors="ignore")
    df["selling_price"] = df["selling_price"].astype("int", errors="ignore")
    df["selling_price_original"] = df["selling_price_original"].astype("int", errors="ignore")
    df["quantity"] = df["quantity"].astype("int", errors="ignore")
    df["quantity_picked"] = df["quantity_picked"].astype("int", errors="ignore")
    df["substitute_type"] = df["substitute_type"].astype("int", errors="ignore")
    df["brand"] = df["brand"].astype("int", errors="ignore")
    df["category"] = df["category"].astype("int", errors="ignore")
    df["unit_multiplier"] = df["unit_multiplier"].astype("float", errors="ignore")

    columns_rename = {
        "seq_id": "id_orden",
		"item_index": "indice_item",
		"substitute_of": "id_producto_substituido",
		"sku": "sku_vtex_id",
		"product": "producto_vtex_id",
		"picker": "id_picker",
		"name": "descripcion",
		"list_price": "precio_lista",
		"price": "precio",
		"selling_price": "precio_venta",
		"selling_price_original": "precio_venta_original",
		"quantity": "unidades_solicitadas",
		"quantity_picked": "unidades_pickeadas",
		"substitute_type": "id_tipo_substitucion",
		"brand": "id_marca",
		"category": "ref_id_categoria",
		"measurement_unit": "unidad_de_medida",
		"unit_multiplier": "multiplicador_unidad",
        "note": "nota"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    columns = [
        "id_orden",
		"indice_item",
		"id_producto_substituido",
		"sku_vtex_id",
		"producto_vtex_id",
        "ref_id",
		"ean",
		"id_picker",
		"descripcion",
		"precio_lista",
		"precio",
		"precio_venta",
		"precio_venta_original",
		"unidades_solicitadas",
		"unidades_pickeadas",
		"id_tipo_substitucion",
		"id_marca",
		"ref_id_categoria",
		"unidad_de_medida",
		"multiplicador_unidad",
        "nota"
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
        INSERT INTO ecommdata.orden_productos (id,"""+columns_query+""") 
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
    print("Data loaded to Postgres: ecommdata.orden_productos table.")

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
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
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

def _order_marketing_data_field_incremental_load(ti, ts):
    import pandas as pd
    new_orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
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
        FROM janis_jackie.wms_order_marketing_data AS womd
        WHERE womd.order_id IN {new_order_ids_string};
    """
    print(janis_query)

    file_name = load_custom_query_to_s3(ts, query=janis_query, query_name="wms_orders_marketing_data_fields")

    return file_name

def _incremental_load_orders_38_table(ti):
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
            "call_center_operator_id",
            "invoice_number",
            "picker",
            "cart_id"
            ]]

    df = df[df["store"] == 38]

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
        "invoice_number": "documento_electronico",
        "picker": "id_picker",
        "cart_id": "janis_cart_id"
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
        "documento_electronico": "int64",
        "id_picker": "int",
        "janis_cart_id": "string"
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
    df["value"] = np.where(df["value"] == "Android", 1, df["value"])
    df["value"] = np.where(df["value"] == "iOS", 1, df["value"])
    df["value"] = df["value"].astype("int")
    df["canal_venta"] = np.where(df["value"] == 1, "app",
                  np.where((df["call_center_operator_id"].isna()) | (df["call_center_operator_id"] == 0), "sitio", "callcenter"))
    
    df = df.drop(columns=["order_id", "call_center_operator_id", "value"])

    df_cdf_cl = df_cdf[df_cdf["field"] == "clientLevel"]
    df_cdf_cl = df_cdf_cl[["order_id", "value"]]
    df = df.merge(df_cdf_cl, left_on="janis_id", right_on="order_id", how="left")
    df["value"] = df["value"].fillna("")
    df["value"] = df["value"].astype("str")
    df["nivel_cliente"] = df["value"]

    df = df.drop(columns=["order_id", "value"])

    marketing_data_fields_file = ti.xcom_pull(key="return_value", task_ids=["order_marketing_data_field_incremental_load"])[0]

    print("Searching file: "+marketing_data_fields_file)
    if not s3_hook.check_for_key(marketing_data_fields_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % marketing_data_fields_file)

    marketing_data_fields_object = s3_hook.get_key(marketing_data_fields_file, bucket_name=s3_bucket)
    df_mdf = pd.read_csv(marketing_data_fields_object.get()["Body"])

    # Filter marketing_data_fields_dataframe
    df_mdf = df_mdf[["order_id", "utm_source"]]

    df = df.merge(df_mdf, left_on="janis_id", right_on="order_id", how="left")
    df["utm_source"] = df["utm_source"].fillna("NULL")
    
    df = df.drop(columns=["order_id"])

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
        "id_picker",
        "janis_cart_id",
        "utm_source",
        "nivel_cliente"
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
        INSERT INTO ecommdata.ordenes_janis_38 (id,"""+columns_query+""") 
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

def _get_new_orders_38_from_s3(ti):
    import pandas as pd

    orders_file = ti.xcom_pull(key="return_value", task_ids=["incremental_unixtime_load_table_to_s3"])[0]
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+orders_file)
    if not s3_hook.check_for_key(orders_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % orders_file)

    orders_object = s3_hook.get_key(orders_file, bucket_name=s3_bucket)

    df = pd.read_csv(orders_object.get()["Body"])
    df = df[df["store"] == 38]
    print(f"Number of records found: {len(df.index)}")

    return df

def _order_items_38_table_incremental_load(ti):
    import numpy as np
    import pandas as pd
    
    df_orders = _get_new_orders_38_from_s3(ti)
    df_orders = df_orders[["id", "seq_id"]]
    df_orders = df_orders.rename(columns={"id": "original_id"})

    order_items_file = ti.xcom_pull(key="return_value", task_ids=["get_order_items_from_janis"])[0]

    if ti.xcom_pull(key="return_value", task_ids=['get_order_items_from_janis'])[0] == "empty":
        return

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+order_items_file)
    if not s3_hook.check_for_key(order_items_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % order_items_file)

    order_items_object = s3_hook.get_key(order_items_file, bucket_name=s3_bucket)

    column_types = {
        "ref_id": "string",
        "ean": "string",
    } 

    df = pd.read_csv(order_items_object.get()["Body"], dtype=column_types)
    df = df[[
        "id", 
        "order_id",
		"item_index",
		"substitute_of", 
		"sku",
		"product",
		"ref_id",
		"ean",
		"picker",
		"name",
		"list_price",
		"price",
		"selling_price",
		"selling_price_original",
		"quantity",
		"quantity_picked",
		"substitute_type",
		"brand",
		"category",
		"measurement_unit",
		"unit_multiplier",
        "note"
    ]]  

    df = df.merge(df_orders, how="inner", left_on="order_id", right_on="original_id").drop(columns=["order_id", "original_id"])

    # # Ensure correct datatypes:
    df["item_index"] = df["item_index"].astype("int", errors="ignore")
    df["substitute_of"] = df["substitute_of"].astype("int", errors="ignore")
    df["picker"] = df["picker"].astype("int", errors="ignore")
    df["list_price"] = df["list_price"].astype("int", errors="ignore")
    df["price"] = df["price"].astype("int", errors="ignore")
    df["selling_price"] = df["selling_price"].astype("int", errors="ignore")
    df["selling_price_original"] = df["selling_price_original"].astype("int", errors="ignore")
    df["quantity"] = df["quantity"].astype("int", errors="ignore")
    df["quantity_picked"] = df["quantity_picked"].astype("int", errors="ignore")
    df["substitute_type"] = df["substitute_type"].astype("int", errors="ignore")
    df["brand"] = df["brand"].astype("int", errors="ignore")
    df["category"] = df["category"].astype("int", errors="ignore")
    df["unit_multiplier"] = df["unit_multiplier"].astype("float", errors="ignore")

    columns_rename = {
        "seq_id": "id_orden",
		"item_index": "indice_item",
		"substitute_of": "id_producto_substituido",
		"sku": "sku_vtex_id",
		"product": "producto_vtex_id",
		"picker": "id_picker",
		"name": "descripcion",
		"list_price": "precio_lista",
		"price": "precio",
		"selling_price": "precio_venta",
		"selling_price_original": "precio_venta_original",
		"quantity": "unidades_solicitadas",
		"quantity_picked": "unidades_pickeadas",
		"substitute_type": "id_tipo_substitucion",
		"brand": "id_marca",
		"category": "ref_id_categoria",
		"measurement_unit": "unidad_de_medida",
		"unit_multiplier": "multiplicador_unidad",
        "note": "nota"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    columns = [
        "id_orden",
		"indice_item",
		"id_producto_substituido",
		"sku_vtex_id",
		"producto_vtex_id",
        "ref_id",
		"ean",
		"id_picker",
		"descripcion",
		"precio_lista",
		"precio",
		"precio_venta",
		"precio_venta_original",
		"unidades_solicitadas",
		"unidades_pickeadas",
		"id_tipo_substitucion",
		"id_marca",
		"ref_id_categoria",
		"unidad_de_medida",
		"multiplicador_unidad",
        "nota"
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
        INSERT INTO ecommdata.orden_productos_38 (id,"""+columns_query+""") 
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
    print("Data loaded to Postgres: ecommdata.orden_productos_38 table.")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_modelo_incremental_ordenes_unimarc',
    default_args=default_args,
    description="Extracción y carga de tabla ordenes desde Janis Replica hasta Workspace.",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "ecommdata", "ordenes_janis", "unimarc", "orden_productos", "MATIAS"],
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
            "updated_column": "date_modified",
            "inclusive": True
        }
    )

    t2 = PythonOperator(
        task_id = "incremental_load_orders_table",
        python_callable = _incremental_load_orders_table
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

    t5a = PythonOperator(
        task_id = "order_marketing_data_field_incremental_load",
        python_callable = _order_marketing_data_field_incremental_load,
        trigger_rule = "none_failed"
    )

    t6 = PythonOperator(
        task_id = "get_order_items_from_janis",
        python_callable = _get_order_items_from_janis
    )

    t7 = PythonOperator(
        task_id = "orden_productos_incremental_load",
        python_callable = _order_items_table_incremental_load
    )

    t2a = PythonOperator(
        task_id = "incremental_load_orders_38_table",
        python_callable = _incremental_load_orders_38_table
    )

    t7a = PythonOperator(
        task_id = "orden_productos_incremental_38_load",
        python_callable = _order_items_38_table_incremental_load
    )

    # Ordenes
    t0 >> t1
    t1 >> t3 >> [t4, t5]
    t4 >> t5a
    t5 >> t5a
    t5a >> t2
    t2 >> t2a
    # Orden productos
    t1 >> t6 >> t7
    t7 >> t7a
