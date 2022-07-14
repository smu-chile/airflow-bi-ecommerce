from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

def _insert_table_from_ecommdata_into_DM(ti, ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    query = f"""
    SELECT s.*
    FROM ecommdata.stock s
    inner join ecommdata.productos p on s.ref_id = p.ref_id
    inner join ecommdata.marcas m on p.id_marca = m.id
    where s.fecha = '{ds}' and m.nombre in ('SOPROLE', 'NEXT', 'UNO', 'MANJARATE', 'QUILQUE');
    """
    pg_hook = PostgresHook("postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    pg_connection.close()
    
    df = pd.DataFrame(
        data = results,
        columns = ['fecha', 'id_tienda', 'glosa_tienda', 'id_bodega', 'nombre_bodega', 'ref_id', 'material', 'descripcion', 'c1', 'c2', 'c3', 'multiplicador_unidad_medida', 'unidades_pack', 'stock_janis', 'stock_seguridad_janis', 'stock_infinito_janis', 'tipo_operacion_janis', 'stock_vtex', 'stock_reservado_vtex', 'stock_disponible_vtex', 'stock_infinito_vtex', 'fecha_publicacion_janis', 'fecha_modificacion_janis', 'ultima_actualizacion']
    )

    if len(df) == 0:
        print("No new data to save")
        return

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("DM_HOST")
    database = Variable.get("DM_DB")
    username = Variable.get("DM_USER")
    password = Variable.get("DM_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    df.to_sql(name="stock",
                    con=engine,         
                    schema="soprole",         
                    if_exists='append',
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL. Table: soprole.stock")

    return
    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}
with DAG(
    'dm_stock_soprole',
    default_args=default_args,
    description="Carga de stock de soprole en datamind",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2022, 7, 14),
    catchup=False,
    tags=["data", "datamind", "stock", "soprole"],
) as dag:

    dag.doc_md = """
    Carga de tabla de productos no encontrados en base a datos de found rate unimarc.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_stock",
        external_dag_id='etl_stock_incremental_load',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id = "insert_table_from_ecommdata_into_DM",
        python_callable = _insert_table_from_ecommdata_into_DM
    )
    

    t0 >> t1
