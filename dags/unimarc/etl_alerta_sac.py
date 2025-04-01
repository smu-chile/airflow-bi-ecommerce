from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

def get_top_productos():
    import pandas as pd
    productos_query = """with productos as (
                        select frp.fecha_picking::date as fecha, frp.ref_id_producto_substituido as ref_id_substituido, count(frp.unidades_pickeadas) as cant_sustituciones 
                        from operaciones_unimarc.found_rate_productos frp 
                        where frp.id_tienda ilike '0442'
                        and frp.ref_id_producto_substituido is not null 
                        and frp.fecha_picking is not null
                        --and frp.producto_substituto is false 
                        --and frp.unidades_solicitadas > 0
                        and frp.fecha_picking >= now() - interval '60 minutes' 
                        group by frp.fecha_picking::date, frp.ref_id_producto_substituido
    )
    select * from productos p order by cant_sustituciones desc"""
    print(productos_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(productos_query)
    results = cursor.fetchall()
    results=pd.DataFrame(results)
    results.columns = ["fecha","ref_id_substituido","cant_sustituciones"]
    print(results.head())
    cursor.close()
    pg_connection.close()

    return results

def send_to_slack():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    results = get_top_productos()
    buffer = io.BytesIO()
    writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
    results.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
    writer.close()
    buffer.seek(0)
    token = Variable.get("token_slack_bot")
    client = WebClient(token=token)

    try:
        client.files_upload(
            channels = Variable.get("token_slack_channel_pruebas"),
            initial_comment = "Productos con mayor cantidad de sustituciones",
            filename = "top_productos.xlsx",
            content = buffer.getvalue()
            )
    except SlackApiError as e:
        print(f"Error sending message: {e}")



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_alertas_sac',
    default_args=default_args,
    description="generación de alertas para sac",
    schedule_interval="*/15 * * * *",
    start_date=pendulum.datetime(2025, 3, 30, tz="America/Santiago"),
    catchup=False,
    tags=["Alertas", "SAC", "Found Rate", "Coyhaique", "FRANCISCO"]
) as dag:
    

    dag.doc_md = """
    Alertas para SAC
    """ 

    t0 = PythonOperator(
        task_id = "load_tables_to_postgres",
        python_callable = get_top_productos,
    )
    
    t1 = PythonOperator(
        task_id = "send_to_slack",
        python_callable = send_to_slack,
    )

    t0 >> t1