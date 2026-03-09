from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
import pendulum

def _insert_table_from_ecommdata_into_DM(ts, ds):
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text
    import numpy as np

    query = f"""
    SELECT s.fecha, s.id_tienda, s.glosa_tienda, s.id_bodega, s.nombre_bodega, s.ref_id, s.material, s.descripcion, s.c1, s.c2, s.c3, s.multiplicador_unidad_medida, s.unidades_pack, s.stock_janis, s.stock_seguridad_janis, s.stock_infinito_janis, s.tipo_operacion_janis, s.stock_vtex, s.stock_reservado_vtex, s.stock_disponible_vtex, s.stock_infinito_vtex, s.fecha_publicacion_janis, s.fecha_modificacion_janis, s.ultima_actualizacion, m.nombre as marca
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
        columns = ['fecha', 'id_tienda', 'glosa_tienda', 'id_bodega', 'nombre_bodega', 'ref_id', 'material', 'descripcion', 'c1', 'c2', 'c3', 'multiplicador_unidad_medida', 'unidades_pack', 'stock_janis', 'stock_seguridad_janis', 'stock_infinito_janis', 'tipo_operacion_janis', 'stock_vtex', 'stock_reservado_vtex', 'stock_disponible_vtex', 'stock_infinito_vtex', 'fecha_publicacion_janis', 'fecha_modificacion_janis', 'ultima_actualizacion' ,'marca']
    )

    if len(df) == 0:
        print("No new data to save")
        return

    print("Number of records to be loaded: "+str(len(df.index)))

    df = df.astype({
        'fecha_publicacion_janis' : "string",
        'fecha_modificacion_janis' : "string",
        'ultima_actualizacion' : "string"
    }, errors="ignore")

    columns = [
        'id_tienda',
        'glosa_tienda',
        'id_bodega',
        'nombre_bodega',
        'ref_id',
        'material',
        'descripcion',
        'c1',
        'c2',
        'c3',
        'multiplicador_unidad_medida',
        'unidades_pack',
        'stock_janis',
        'stock_seguridad_janis',
        'stock_infinito_janis',
        'tipo_operacion_janis',
        'stock_vtex',
        'stock_reservado_vtex',
        'stock_disponible_vtex',
        'stock_infinito_vtex',
        'fecha_publicacion_janis',
        'fecha_modificacion_janis',
        'ultima_actualizacion',
        'marca'
    ]

    columns_query = ",".join(columns)
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
        INSERT INTO soprole.stock (fecha, """+columns_query+""") 
        VALUES ("""+values_query+""");
    """
    truncate_query = "TRUNCATE TABLE soprole.stock;"
    print(incremental_query)

    host = Variable.get("DM_HOST")
    database = Variable.get("DM_DB")
    username = Variable.get("DM_USER")
    password = Variable.get("DM_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)
    
    pg_connection = engine.raw_connection()
    cursor = pg_connection.cursor()
    cursor.execute(truncate_query)
    cursor.executemany(incremental_query, fixed_records)
    pg_connection.commit()
    cursor.close()
    pg_connection.close()
    print("Data loaded to Postgres")

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
    schedule="0 0/4 * * *",
    start_date=pendulum.datetime(2022, 7, 14, tz="America/Santiago"),
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
        failed_states=['failed'],
        timeout = 60*60
    )

    t1 = PythonOperator(
        task_id = "insert_table_from_ecommdata_into_DM",
        python_callable = _insert_table_from_ecommdata_into_DM
    )
    

    t0 >> t1
