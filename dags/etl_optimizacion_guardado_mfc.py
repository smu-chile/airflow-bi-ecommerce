from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import pendulum

from datetime import datetime, timedelta

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()
    return results

def optimizacion_to_s3():
    import pandas as pd

    query="""WITH promos AS (
                SELECT DISTINCT CONCAT(material,'-',REPLACE(umv, 'ST', 'UN')) AS ref_id
                FROM ecommdata.workflow_promociones wp
                WHERE wp.id_mecanica <> ALL (ARRAY[124,36, 67, 72, 99, 84, 37, 51, 93, 53, 96, 77, 59])
                AND wp.fecha_inicio_de_promocion <= current_date - 7
                AND wp.fecha_fin_de_promocion >= current_date
                AND wp.tipo_promocion <> 3
                AND wp.n_promocion NOT IN (5552152024)
                AND wp.nombre_promocion::text NOT LIKE '%MFC%'
                AND wp.nombre_promocion::text NOT LIKE '%UNIPAY%'
                AND wp.nombre_promocion::text NOT LIKE '%917%'
                AND wp.nombre_promocion::text NOT LIKE '%BANCO ESTADO%'
                AND wp.nombre_promocion::text NOT LIKE '%LOC%'
                AND wp.nombre_promocion::text NOT LIKE 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
                AND wp.nombre_promocion::text NOT LIKE '%HUACHALALUME%'
                AND wp.nombre_promocion::text NOT LIKE '%LOCAL%'
            ),
            venta AS (
                SELECT _v.id_tienda, _v.ref_id_sku, ROUND(AVG(venta_umv), 2) AS venta_umv_avg, ROUND(AVG(venta_neta), 2) AS venta_neta_avg
                FROM (
                    SELECT id_tienda, ref_id_sku, fecha_facturacion::date, SUM(venta_umv) AS venta_umv, SUM(venta_neta) AS venta_neta
                    FROM ecommdata.ventas_ecommerce_datawarehouse ved
                    WHERE id_tienda = '1917'
                    AND fecha_facturacion >= current_date - 60
                    AND venta_umv > 0
                    GROUP BY id_tienda, ref_id_sku, fecha_facturacion::date
                ) AS _v
                GROUP BY _v.id_tienda, _v.ref_id_sku
                ORDER BY 4 DESC
            ),
            ubi AS (
                SELECT DISTINCT fecha_carga, "TOM ID" AS ref_id, "Quantity On-Hand" AS stock,
                    CASE
                        WHEN "Storage Area" LIKE '%DYNAMIC%' THEN 'DYNAMIC'
                        WHEN "Storage Area" LIKE '%MANUAL%' THEN 'MANUAL'
                        WHEN "Storage Area" LIKE '%OSR%' THEN 'OSR'
                        ELSE NULL
                    END AS ubi
                FROM ecommdata.inventario_manual_mfc imm 
                WHERE fecha_carga = current_date
            )
            SELECT v.*,
                COUNT(um.ubi) > 0 AS has_stock,
                SUM(CASE WHEN um.ubi = 'MANUAL' THEN um.stock ELSE 0 END) AS stock_manual,
                SUM(CASE WHEN um.ubi = 'OSR' THEN um.stock ELSE 0 END) AS stock_osr,
                SUM(CASE WHEN um.ubi = 'DYNAMIC' THEN um.stock ELSE 0 END) AS stock_dynamic,
                v.venta_umv_avg*7 as stock_objetivo
            FROM venta v
            LEFT JOIN promos p ON v.ref_id_sku = p.ref_id
            LEFT JOIN ubi um ON um.ref_id = v.ref_id_sku
            WHERE p.ref_id IS NULL
            GROUP BY v.id_tienda, v.ref_id_sku, v.venta_umv_avg, v.venta_neta_avg
            ORDER BY v.venta_umv_avg DESC;"""
    df = query_to_df(query)
    print(df)
    
    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_optimicacion_guardado_mfc',
    default_args=default_args,
    description="Calculo de optimizacion de guardado de productos por zonas en el MFC",
    schedule_interval= "0 9 * * *",
    start_date=pendulum.datetime(2023, 9, 27, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "postgres", "MFC", "s3", "stock", "PATRICIO"],
) as dag:

    dag.doc_md = """
    Calculo de optimizacion de guardado de productos por zonas en el MFC a travez de determinar inventario vs DoH. \n
    1 vez al dia.
    """ 

    t0 = PythonOperator(
        task_id = "optimizacion_to_s3",
        python_callable = optimizacion_to_s3,
    )

    #t1 = PythonOperator(
    #    task_id = "optimizacion_to_postgres",
    #    python_callable = optimizacion_to_postgres,
    #)

    t0#>> t1