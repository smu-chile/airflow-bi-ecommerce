from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime

def _send_stock_0_to_janis(ds):
    import pandas as pd
    import sqlalchemy
    import requests

    print(f"date: {ds}")
    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    query = """
        select SUBSTRING(hvd.ref_id,1,18) as material
        , hvd.id_tienda as id_tienda
        from ecommdata.historia_venta_dw hvd
        inner join ecommdata.productos p on hvd.ref_id = p.ref_id
        left join catalogo.categoria_tienda_inmovilizada cti on hvd.id_tienda = cti.id_tienda and p.id_categoria = cti.categoria
        where case 
            when hvd.venta_ayer is true then 0
            when hvd.venta_2 is true then 1
            when hvd.venta_3 is true then 2
            when hvd.venta_4 is true then 3
            when hvd.venta_5 is true then 4
            when hvd.venta_6 is true then 5
            when hvd.venta_7 is true then 6
            when hvd.venta_8 is true then 7
            when hvd.venta_9 is true then 8
            when hvd.venta_10 is true then 9
            else 10
        end = cti.dias_sin_venta 
    """
    df = pd.read_sql(query, engine)

    base_url = Variable.get("JANIS_API_URL")

    url = f"{base_url}stock"

    JANIS_API_KEY = Variable.get("JANIS_API_KEY")
    JANIS_API_SECRET = Variable.get("JANIS_API_SECRET")
    JANIS_CLIENT = Variable.get("JANIS_CLIENT")

    headers = {
    "janis-api-key" : JANIS_API_KEY,
    "janis-api-secret" : JANIS_API_SECRET,
    "janis-client" : JANIS_CLIENT,
    "Connection" : "keep-alive"
    }

    payload=[]
    for ind in df.index:
        material = str(df['material'][ind]).zfill(18)
        id_tienda = str(int(df['id_tienda'][ind])).zfill(4)
        row = {"IdSku": material, "Quantity": 0, "Store": id_tienda}
        payload.append(row)
    payload = str(payload).replace("'", '"')
    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.text)
    return

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_borrado_stock_inmovilizados',
    default_args=default_args,
    description="Borrado de Stock Janis en base a historia de ventas en dw y parametros entregados en tabla catalogo.categoria_tienda_inmovilizada.",
    schedule_interval="0 10 * * *",
    start_date=datetime(2023, 1, 30),
    catchup=False,
    tags=["Janis", "ecommdata", "catalogo", "inmovilizados", "stock"],
) as dag:

    dag.doc_md = """
    Borrado de Stock Janis en base a historia de ventas en dw y parametros entregados en tabla catalogo.categoria_tienda_inmovilizada.
    """ 
    
    t0 = PostgresOperator(
        task_id = "truncate_table_sales_history",
        postgres_conn_id="postgresql_conn",
        sql="""
            truncate ecommdata.historia_venta_dw
        """
    )
    
    t1 = PostgresOperator(
        task_id = "load_table_sales_history",
        postgres_conn_id="postgresql_conn",
        sql="""
        insert into ecommdata.historia_venta_dw
        select CONCAT(lnr.material,'-',lnr.umv), lnr.id_tienda,
        case 
            when {{ds}}::date - interval '1 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_ayer,
        case 
            when {{ds}}::date - interval '2 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_2,
        case 
            when {{ds}}::date - interval '3 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_3,
        case 
            when {{ds}}::date - interval '4 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_4,
        case 
            when {{ds}}::date - interval '5 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_5,
        case 
            when {{ds}}::date - interval '6 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_6,
        case 
            when {{ds}}::date - interval '7 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_7,
        case 
            when {{ds}}::date - interval '8 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_8,
        case 
            when {{ds}}::date - interval '9 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_9,
        case 
            when {{ds}}::date - interval '10 day' = any (t1.fechas_facturacion) then true
            else false
        end as venta_10
        from ecommdata.lista8 lnr
        left join (
            select LPAD(vst.material, 18, '0') as material, LPAD(vst.id_tienda, 4, '0') as id_tienda , array_agg(vst.fecha) as fechas_facturacion
            from ecommdata.venta_sku_tienda vst
            group by LPAD(vst.material, 18, '0'), LPAD(vst.id_tienda, 4, '0'))t1 on lnr.material = t1.material and lnr.id_tienda = t1.id_tienda;
                """,
    )

    t2 = PythonOperator(
        task_id = "send_stock_0_to_janis",
        python_callable = _send_stock_0_to_janis
    )

    t0 >> t1 >> t2
