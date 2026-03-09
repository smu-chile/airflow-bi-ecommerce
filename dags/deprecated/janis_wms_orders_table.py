from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.janis_utils import load_full_table_to_s3

from datetime import datetime
from io import StringIO

import boto3
import pandas as pd
import sqlalchemy

def load_wms_ordes_from_s3_to_postgres(ti):
    file_name = ti.xcom_pull(key="return_value", task_ids=["load_full_table_to_s3"])[0]

    access_key = Variable.get("AWS_ACCESS_KEY")
    secret_key = Variable.get("AWS_SECRET_KEY")
    bucket_name = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_resource = boto3.resource("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-east-1")
    bucket = s3_resource.Bucket(bucket_name)
    csv_file = bucket.Object(file_name)

    df = pd.read_csv(csv_file.get()["Body"])
    df = df.head(100)
    print("Number of records: ")
    print(len(df.index))
    print(df.columns)

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
            "date_modified"
            ]]

    # Rename columns to match workspace schema:
    columns_rename = {
        "seq_id": "orden",
        "id": "janis_id",
        "website_name": "nombre_website",
        "customer": "id_cliente_janis",
        "customer_address": "id_direccion_cliente_janis",
        "store": "store_id",
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
        "date_modified": "fecha_modificacion"
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
    df["fecha_facturacion"] = pd.to_datetime(df["fecha_facturacion"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
    df["fecha_picking"] = pd.to_datetime(df["fecha_picking"], unit="s").dt.tz_localize('UTC').dt.tz_convert("America/Santiago")
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
        "cobro_despacho_neto": "int"
    })

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    # Save to PostgreSQL:
    df.to_sql(name="ordenes",
                con=engine,         
                schema="ecommdata",         
                if_exists='replace',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

    return



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'janis_wms_orders_full_table_load',
    default_args=default_args,
    description="""Extracción y carga historica de tabla wms_orders desde Janis Replica.""",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["DATA", "Janis", "S3", "Workspace"],
) as dag:

    dag.doc_md = """
    Extracción y carga de tabla de wms_orders de Janis.
    Extracción y carga historica de tabla wms_orders desde Janis Replica.
    Este proceso recargará la historia completa de la tabla wms_orders, por lo que se recomienda precaución al momento de ejecutarlo.
    Es posible que este proceso tarde varios minutos en ejecutar.            
    """ 
    t0 = PythonOperator(
        task_id = "load_full_table_to_s3",
        python_callable = load_full_table_to_s3,
        op_kwargs = {"table_name": "wms_orders"}
    )

    t1 = PythonOperator(
        task_id = "load_wms_ordes_from_s3_to_postgres",
        python_callable = load_wms_ordes_from_s3_to_postgres
    )

    t0 >> t1
