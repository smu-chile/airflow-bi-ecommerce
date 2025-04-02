from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import pendulum

def get_top_productos():
    import pandas as pd
    productos_query = """WITH productos AS (
                        SELECT frp.fecha_picking::date AS fecha, 
                               frp.ref_id_producto_substituido AS ref_id_substituido, 
                               COUNT(frp.unidades_pickeadas) AS cant_sustituciones 
                        FROM operaciones_unimarc.found_rate_productos frp 
                        WHERE frp.id_tienda ILIKE '0442'
                        AND frp.ref_id_producto_substituido IS NOT NULL 
                        AND frp.fecha_picking IS NOT NULL
                        AND frp.fecha_picking >= NOW() - INTERVAL '60 minutes' 
                        GROUP BY frp.fecha_picking::date, frp.ref_id_producto_substituido
                    )
                    SELECT * FROM productos p 
                    ORDER BY cant_sustituciones DESC"""
    
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    print(productos_query)
    try:
        # Ejecutamos la consulta SQL
        results = pg_hook.get_records(productos_query)
        
        # Verificamos si se obtuvieron resultados
        if not results:
            print("No data returned from query")
            return pd.DataFrame()  # Retornamos un DataFrame vacío si no hay resultados
        
        # Convertimos los resultados a un DataFrame
        df = pd.DataFrame(results, columns=["fecha", "ref_id_substituido", "cant_sustituciones"])
        print(df.head())
        return df
    except Exception as e:
        print(f"Error while executing query: {e}")
        return pd.DataFrame()  # En caso de error, retornar un DataFrame vacío

def send_to_slack():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io

    results = get_top_productos()
    
    # Si el DataFrame está vacío, no enviamos nada a Slack
    if results.empty:
        print("No data to send to Slack.")
        return
    
    buffer = io.BytesIO()
    writer = pd.ExcelWriter(buffer, engine='xlsxwriter')
    results.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
    writer.close()
    buffer.seek(0)
    token = Variable.get("token_slack_bot")
    client = WebClient(token=token)

    try:
        client.files_upload(
            channels=Variable.get("token_slack_channel_pruebas"),
            initial_comment="Productos con mayor cantidad de sustituciones",
            filename="top_productos.xlsx",
            content=buffer.getvalue()
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
    description="Generación de alertas para SAC",
    schedule_interval="*/15 * * * *",
    start_date=pendulum.datetime(2025, 3, 30, tz="America/Santiago"),
    catchup=False,
    tags=["Alertas", "SAC", "Found Rate", "Coyhaique", "FRANCISCO"]
) as dag:
    
    dag.doc_md = """
    Alertas para SAC
    """ 

    t0 = PythonOperator(
        task_id="get_products_from_postgres",
        python_callable=get_top_productos,
    )
    
    t1 = PythonOperator(
        task_id="send_to_slack",
        python_callable=send_to_slack,
    )

    t0 >> t1