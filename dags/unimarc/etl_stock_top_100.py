from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta

import pendulum

def load_top_100_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io
    from io import StringIO

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"top_100/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    df_promos = pd.DataFrame()

    
    top_100_query = f"""WITH SalesData AS (
            SELECT
                ved.ref_id_sku,
                ved.id_tienda,
                COUNT(ved.ref_id_sku) AS recurrencia,
                SUM(venta_umv / s.multiplicador_unidad_medida) AS venta_unidades,
                SUM(venta_neta) AS venta_plata
            FROM
                ecommdata.ventas_ecommerce_datawarehouse ved
                LEFT JOIN ecommdata.skus s ON s.ref_id = ved.ref_id_sku
            WHERE
                fecha_facturacion >= '{ds}'::date - 30
                AND ved.id_tienda IN ('0581','1917','0442','0347','0336','0034')
                AND ved.ref_id_sku <> '000000000000630792-UN'
            GROUP BY
                ved.ref_id_sku, ved.id_tienda
        ),
        RankedData AS (
            SELECT
                ref_id_sku,
                id_tienda,
                recurrencia,
                venta_unidades,
                venta_plata,
                DENSE_RANK() OVER (ORDER BY recurrencia DESC) AS recurrencia_rank,
                DENSE_RANK() OVER (ORDER BY venta_unidades DESC) AS unidades_rank,
                DENSE_RANK() OVER (ORDER BY venta_plata DESC) AS plata_rank
            FROM
                SalesData
        )
        SELECT
            r.id_tienda,
            ROW_NUMBER() OVER (PARTITION BY r.id_tienda ORDER BY (0.5 * recurrencia_rank + 0.3 * unidades_rank + 0.2 * plata_rank)) AS ranking,
            r.ref_id_sku,
            s.nombre_sku,
            CASE
                WHEN s2.stock_janis IS NULL THEN 0
                ELSE s2.stock_janis
            END AS stock_dia,
            r.recurrencia_boleta,
            ROUND(r.venta_unidades::numeric) AS venta_unidades,
            r.venta_pesos
        FROM
            RankedData r
            LEFT JOIN ecommdata.skus s ON s.ref_id = r.ref_id_sku
            LEFT JOIN ecommdata.stock s2 ON r.id_tienda = s2.id_tienda AND r.ref_id_sku = s2.ref_id
            LEFT JOIN ecommdata.lista8 l8 ON ((l8.material::text || '-'::text) || l8.umv::text) = r.ref_id_sku AND r.id_tienda = l8.id_tienda
        WHERE
            s2.fecha = '{ds}'::date
            AND l8.fecha = '{ds}'::date
            AND l8.material::text || '-'::text || l8.umv::text IS NOT NULL
        ORDER BY
            ranking, id_tienda
        LIMIT (SELECT COUNT(DISTINCT id_tienda) FROM SalesData) * 100;"""
    print(top_100_query)

    cursor.execute(top_100_query)
    results = cursor.fetchall()
    columns_name = [i[0] for i in cursor.description]

    df_temp = pd.DataFrame(results, columns=columns_name)
    df_promos = pd.concat([df_promos, df_temp], ignore_index=True)

    cursor.close()
    pg_connection.close()

    buffer = io.StringIO()
    df_promos.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"top_100/{exec_date}/top_100_stock_{date_aux}.csv"
    buffer.seek(0)
    print("se logro transformar el dataframe a un archivo .csv")
    print(f"con fecha {ds} y nombre de filename como {filename}")
    s3_hook.load_string(buffer.getvalue(),
                key=filename,
                bucket_name=s3_bucket,
                replace=True,
                encrypt=False)
    
    print(f"File load on S3: {prefix}")

    return filename

def load_cantidad_promociones_to_postgres(ti):
    import pandas as pd
    import numpy as np
    import sqlalchemy

    cantidad_promociones_file = ti.xcom_pull(key="return_value", task_ids=["load_cantidad_promociones_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+cantidad_promociones_file)
    if not s3_hook.check_for_key(cantidad_promociones_file, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % cantidad_promociones_file)

    limit_object = s3_hook.get_key(cantidad_promociones_file, bucket_name=s3_bucket)

    df = pd.read_csv(limit_object.get()["Body"])

    columns = ["cantidad_promociones_activas", "descripcion_mecanica"]

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
        INSERT INTO ecommdata.cantidad_promociones_diarias (dia,id_mecanica,"""+columns_query+""",canal_distribucion) 
        VALUES ("""+values_query+""")
        ON CONFLICT (dia,id_mecanica,canal_distribucion)
        DO NOTHING; 
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


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_stock_top_100',
    default_args=default_args,
    description="Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de stock de top 100 SKUs segmentados por tienda",
    schedule_interval="0 7 * * *",
    start_date=pendulum.datetime(2022, 8, 11, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ecommdata", "stock", "Unimarc", "ventas_ecommerce_dw"],
) as dag:

    dag.doc_md = """
    Extracción de datos de tabla ventas_ecommerce_dw y posterior carga de stock de top 100 SKUs segmentados por tienda\n
    """ 
    t0 = PythonOperator(
        task_id = "load_top_100_to_s3",
        python_callable = load_top_100_to_s3,
    )

    t0