from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.netezza_utils import netezza_full_table_load_to_s3

from datetime import datetime, timedelta


def _create_initial_promotions_table(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    
    dw_promotion_file = ti.xcom_pull(key="return_value", task_ids=["netezza_vw_workflow_initial_load"])[0]

    s3_bucket = Variable.get('AWS_S3_BUCKET_NAME', default_var='default-bucket')
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+dw_promotion_file)
    if not s3_hook.check_for_key(dw_promotion_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % dw_promotion_file)

    dw_promotion_object = s3_hook.get_key(dw_promotion_file, bucket_name=s3_bucket)
    columns_types = {
        "ID_WORKFLOW": "int64",
        "N_PROMOCION": "int64",
		"NOMBRE_PROMOCION": "str",
		"ID_EVENTO": "int",
		"DESCRIPCION_EVENTO_PROMOCIONAL": "str",
		"ID_MECANICA": "float",
		"DESCRIPCION_MECANICA": "str",
		"MATERIAL": "int64",
		"DESC_MATERIAL": "str",
		"UN_MEDIDA_VENTA": "str",
		"ORGANIZACION_VENTAS": "str",
		"CANAL_DISTRIBUCION": "str",
		"EAN": "str",
		"LINEA": "str",
		"DESCRIPCION_LINEA": "str",
		"DESC_MARCA": "str",
		"TIPO_PROMOCION": "int",
		"DESC_PROMOCION": "str",
		"PRECIO_MODAL": "float",
		"PRECIO_MODAL_TOTAL": "float",
		"PRECIO_PROMOCIONAL": "float",
		"PRECIO_TOTAL_PROMOCIONAL": "float",
		"AHORRO": "float",
		"AHORRO_TOTAL": "float",
		"CANTIDAD_N": "int",
		"CANTIDAD_M": "int",
		"PRECIO_FIJO": "float",
		"DESDE_KG": "float",
		"PRECIO_KILO": "float",
		"LLEVAS_N": "float",
		"PRECIO_N": "float",
		"PORCENTAJE_N": "float",
		"COSTO_UNIDAD_MEDIDA_DE_PEDIDO": "float",
		"COSTO_UNIDAD_MEDIDA_DE_VENTA": "float",
		"TIPO_FINANCIAMIENTO": "str",
        "IMPORTE_NEGOCIADO": "float",
		"PORCENTAJE_FINANCIAMIENTO": "float",
		"COSTO_NETO_UMP": "float",
		"PORCENTAJE_COSTO_PROMOCIONAL": "float",
		"DESDE_SELL_IN": "str",
		"HASTA_SELL_IN": "str",
		"FECHA_INICIO_DE_PROMOCION": "str",
		"FECHA_FIN_DE_PROMOCION": "str",
		"PORCENTAJE_DE_DESCUENTO": "float",
		"PROMOEVENTMECHANISM": "str",
		"DESCUENTOFINAL": "float",
		"FECHA_MODIFICACION": "str",
        "REGISTRO_VALIDO": "str"
    }
    df = pd.read_csv(dw_promotion_object.get()["Body"], dtype=columns_types)
    df = df[["ID_WORKFLOW", "N_PROMOCION",	"NOMBRE_PROMOCION", "ID_EVENTO", "DESCRIPCION_EVENTO_PROMOCIONAL", "ID_MECANICA",
		    "DESCRIPCION_MECANICA", "MATERIAL", "DESC_MATERIAL", "UN_MEDIDA_VENTA", "ORGANIZACION_VENTAS",
		    "CANAL_DISTRIBUCION", "EAN", "LINEA", "DESCRIPCION_LINEA", "DESC_MARCA", "TIPO_PROMOCION",
		    "DESC_PROMOCION", "PRECIO_MODAL", "PRECIO_MODAL_TOTAL", "PRECIO_PROMOCIONAL", "PRECIO_TOTAL_PROMOCIONAL",
		    "AHORRO", "AHORRO_TOTAL", "CANTIDAD_N", "CANTIDAD_M", "PRECIO_FIJO", "DESDE_KG", "PRECIO_KILO",
		    "LLEVAS_N", "PRECIO_N", "PORCENTAJE_N", "COSTO_UNIDAD_MEDIDA_DE_PEDIDO", "COSTO_UNIDAD_MEDIDA_DE_VENTA",
		    "TIPO_FINANCIAMIENTO", "IMPORTE_NEGOCIADO", "PORCENTAJE_FINANCIAMIENTO", "COSTO_NETO_UMP",
		    "PORCENTAJE_COSTO_PROMOCIONAL", "DESDE_SELL_IN", "HASTA_SELL_IN", "FECHA_INICIO_DE_PROMOCION",
		    "FECHA_FIN_DE_PROMOCION", "PORCENTAJE_DE_DESCUENTO", "PROMOEVENTMECHANISM", "DESCUENTOFINAL",
		    "FECHA_MODIFICACION", "REGISTRO_VALIDO", "NOMBRE_DEL_PROVEEDOR_SELL_OUT", "PROVEEDOR_SELL_OUT"]]  


    df["ID_MECANICA"] = df["ID_MECANICA"].astype("int", errors="ignore")
    
    # Fix date types:
    print("Fixing date datatype columns...")
    df["DESDE_SELL_IN"] = pd.to_datetime(df["DESDE_SELL_IN"], format="%Y-%m-%d", errors="ignore")
    df["HASTA_SELL_IN"] = pd.to_datetime(df["HASTA_SELL_IN"], format="%Y-%m-%d", errors="ignore")
    df["FECHA_INICIO_DE_PROMOCION"] = pd.to_datetime(df["FECHA_INICIO_DE_PROMOCION"], format="%Y-%m-%d", errors="ignore")
    df["FECHA_FIN_DE_PROMOCION"] = pd.to_datetime(df["FECHA_FIN_DE_PROMOCION"], format="%Y-%m-%d", errors="ignore")
    df["FECHA_MODIFICACION"] = pd.to_datetime(df["FECHA_MODIFICACION"], format="%Y-%m-%d %H:%M:%S.000", errors="ignore")

    # Fix percentage data:
    print("Fixing percentage columns...")
    df["PORCENTAJE_N"] = df["PORCENTAJE_N"]/100
    df["PORCENTAJE_FINANCIAMIENTO"] = df["PORCENTAJE_FINANCIAMIENTO"]/100
    df["PORCENTAJE_COSTO_PROMOCIONAL"] = df["PORCENTAJE_COSTO_PROMOCIONAL"]/100
    df["PORCENTAJE_DE_DESCUENTO"] = df["PORCENTAJE_DE_DESCUENTO"]/100
    df["DESCUENTOFINAL"] = df["DESCUENTOFINAL"]/100

    # Fix boolean data:
    print("Fixing boolean datatype columns...")
    df["REGISTRO_VALIDO"] = np.where(df["REGISTRO_VALIDO"] == "X", True, False)

    # Left pad material column:
    df["MATERIAL"] = df["MATERIAL"].astype("string", errors="ignore").str.pad(18, side="left", fillchar="0")

    columns_rename = {
        "ID_WORKFLOW": "id_workflow",
        "N_PROMOCION": "n_promocion",
		"NOMBRE_PROMOCION": "nombre_promocion",
		"ID_EVENTO": "id_evento",
		"DESCRIPCION_EVENTO_PROMOCIONAL": "descripcion_evento_promocional",
		"ID_MECANICA": "id_mecanica",
		"DESCRIPCION_MECANICA": "descripcion_mecanica",
		"MATERIAL": "material",
		"DESC_MATERIAL": "descripcion_material",
		"UN_MEDIDA_VENTA": "umv",
		"ORGANIZACION_VENTAS": "organizacion_ventas",
		"CANAL_DISTRIBUCION": "canal_distribucion",
		"EAN": "ean",
		"LINEA": "linea",
		"DESCRIPCION_LINEA": "descripcion_linea",
		"DESC_MARCA": "marca",
		"TIPO_PROMOCION": "tipo_promocion",
		"DESC_PROMOCION": "desc_promocion",
		"PRECIO_MODAL": "precio_modal",
		"PRECIO_MODAL_TOTAL": "precio_modal_total",
		"PRECIO_PROMOCIONAL": "precio_promocional",
		"PRECIO_TOTAL_PROMOCIONAL": "precio_total_promocional",
		"AHORRO": "ahorro",
		"AHORRO_TOTAL": "ahorro_total",
		"CANTIDAD_N": "cantidad_n",
		"CANTIDAD_M": "cantidad_m",
		"PRECIO_FIJO": "precio_fijo",
		"DESDE_KG": "desde_kg",
		"PRECIO_KILO": "precio_kilo",
		"LLEVAS_N": "llevas_n",
		"PRECIO_N": "precio_n",
		"PORCENTAJE_N": "porcentaje_n",
		"COSTO_UNIDAD_MEDIDA_DE_PEDIDO": "costo_unidad_medida_de_pedido",
		"COSTO_UNIDAD_MEDIDA_DE_VENTA": "costo_unidad_medida_de_venta",
		"TIPO_FINANCIAMIENTO": "tipo_financiamiento",
        "IMPORTE_NEGOCIADO": "importe_negociado",
		"PORCENTAJE_FINANCIAMIENTO": "porcentaje_financiamiento",
		"COSTO_NETO_UMP": "costo_neto_ump",
		"PORCENTAJE_COSTO_PROMOCIONAL": "porcentaje_costo_promocional",
		"DESDE_SELL_IN": "desde_sell_in",
		"HASTA_SELL_IN": "hasta_sell_in",
		"FECHA_INICIO_DE_PROMOCION": "fecha_inicio_de_promocion",
		"FECHA_FIN_DE_PROMOCION": "fecha_fin_de_promocion",
		"PORCENTAJE_DE_DESCUENTO": "porcentaje_de_descuento",
		"PROMOEVENTMECHANISM": "promo_event_mechanism",
		"DESCUENTOFINAL": "porcentaje_descuento_final",
		"FECHA_MODIFICACION": "fecha_modificacion",
        "REGISTRO_VALIDO": "registro_valido",
        "NOMBRE_DEL_PROVEEDOR_SELL_OUT": "nombre_proveedor_sell_out",
        "PROVEEDOR_SELL_OUT": "proveedor_sell_out"
    }

    df = df.rename(columns=columns_rename)

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    truncate_query = "TRUNCATE TABLE ecommdata.workflow_promociones"
    connection.execute(text(truncate_query))
    connection.close()

    # Save to PostgreSQL:
    df.to_sql(name="workflow_promociones",
                con=engine,         
                schema="ecommdata",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL. Table: ecommdata.workflow_promociones")

    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'workflow_promotions_table_initial_load',
    default_args=default_args,
    description="Extraction and initial load of workflow_promotion data.",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["DATA", "DW", "S3", "Workspace", "Promociones"],
) as dag:

    dag.doc_md = """
    Extract workflow table data (promotions) from Datawarehouse
    with filters for a full initial load to S3, then select specific
    columns from csv file and load it to Postgres workspace. \n
    This process will attempt to TRUNCATE the table before loading records.
    """ 

    t0 = PythonOperator(
        task_id = "netezza_vw_workflow_initial_load", 
        python_callable = netezza_full_table_load_to_s3,
        op_kwargs = {"table_name": "NZ_BU.ECOMERCE.VW_WORKFLOW",
                    "where": """ FECHA_INICIO_DE_PROMOCION >= '2021-01-01'
                                AND REGISTRO_VALIDO = 'X'
                                AND ORGANIZACION_VENTAS = '1000'
                                AND CANAL_DISTRIBUCION in ('10','70')
                                AND ID_EVENTO <> '572' """
        },
        retries = 2,
        retry_delay = timedelta(minutes=1),
        execution_timeout = timedelta(minutes=60)
    )

    t1 = PythonOperator(
        task_id = "create_initial_workflow_promotions_table",
        python_callable = _create_initial_promotions_table
    )

    t0 >> t1
