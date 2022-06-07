from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime

def _upsert_table_from_ecommdata_into_DM(ti):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    query = f"""
    select date(frp.fecha_picking) as fecha_proceso
    , frp.ref_id
    , s.ean_primario as ean
    , frp.id_tienda
    , m.nombre as marca
    , COUNT(distinct frp.orden) as ordenes_afectadas
    , SUM(frp.unidades_solicitadas - frp.unidades_pickeadas) as unidades_faltantes
    , case
        when date_part('minute', frp.fecha_picking::time) < 30 then date_trunc('hour', frp.fecha_picking::time)
        else date_trunc('hour', frp.fecha_picking::time)::interval + ('00:30:00')::interval
    end as inicio_bloque
    , case
        when date_part('minute', frp.fecha_picking::time) < 30 then date_trunc('hour', frp.fecha_picking::time)::interval + ('00:30:00')::interval
        else date_trunc('hour', frp.fecha_picking::time)::interval + ('01:00:00')::interval
    end as fin_bloque
    , now() as fecha_modificacion
    from operaciones_unimarc.found_rate_productos frp
    left join ecommdata.skus s on frp.ref_id = s.ref_id
    left join ecommdata.productos p  on frp.ref_id = p.ref_id
    left join ecommdata.marcas m on p.id_marca = m.id
    where frp.estado_foundrate = 1 and date(frp.fecha_picking) = current_date and m.nombre in ('SOPROLE', 'NEXT', 'UNO', 'MANJARATE', 'QUILQUE')
    group by frp.ref_id, s.ean_primario,date(frp.fecha_picking), frp.id_tienda, m.nombre, date_trunc('hour', frp.fecha_picking::time), inicio_bloque, fin_bloque;
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
        columns = ['fecha_proceso', 'ref_id', 'ean', 'id_tienda', 'marca', 'ordenes_afectadas', 'unidades_faltantes', 'inicio_bloque', 'fin_bloque', 'fecha_modificacion']
    )

    df['unidades_faltantes'] = df['unidades_faltantes'] * 1.0

    print("Number of records to be loaded: "+str(len(df.index)))

    host = Variable.get("DM_HOST")
    database = Variable.get("DM_DB")
    username = Variable.get("DM_USER")
    password = Variable.get("DM_PASSWORD")
    
    conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    connection = engine.connect()
    upsert_query = f""" 
                    INSERT INTO soprole.alerta_found_rate
                    VALUES {','.join([str(i) for i in list(df.to_records(index=False))])}
                    ON CONFLICT ON CONSTRAINT alerta_found_rate_pk
                    DO UPDATE SET ordenes_afectadas = excluded.ordenes_afectadas,
                    unidades_faltantes = excluded.unidades_faltantes,
                    fecha_modificacion = excluded.fecha_modificacion
            """
    connection.execute(text(upsert_query))
    connection.close()

    print("Data saved to PostgreSQL. Table: soprole.alerta_found_rate")

    

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}
with DAG(
    'dm_productos_no_encontrados',
    default_args=default_args,
    description="Carga de tabla de productos no encontrados",
    schedule_interval="30 * * * *",
    start_date=datetime(2022, 6, 2),
    catchup=False,
    tags=["data", "datamind", "not_found", "unimarc"],
) as dag:

    dag.doc_md = """
    Carga de tabla de productos no encontrados en base a datos de found rate unimarc.
    """ 
    t0 = ExternalTaskSensor(
        task_id="wait_for_found_rate_productos",
        external_dag_id='etl_found_rate_productos_unimarc',
        external_task_id=None,
        allowed_states=['success'],
        failed_states=['failed']
    )

    t1 = PythonOperator(
        task_id = "upsert_table_from_ecommdata_into_DM",
        python_callable = _upsert_table_from_ecommdata_into_DM
    )
    

    t0 >> t1
