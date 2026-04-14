from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils.slack_utils import dag_success_slack, dag_failure_slack
import pendulum
from datetime import datetime, timedelta

default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_poligonos_historico',
    default_args=default_args,
    description="Carga tabla historica de poligonos tiendas",
    schedule_interval="15 8 * * *",
    start_date=pendulum.datetime(2026, 4, 8, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "Janis", "forcast_and_plannig", "polygons", "unimarc", "MAURICIO"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:
    dag.doc_md = """
    Actualiza la tabla poligonos_historico de forma incremental y idempotente.
    Se ejecuta después del DAG etl_poligonos.
    """
    
    t1 = PostgresOperator(
        task_id="upsert_poligonos_historico",
        postgres_conn_id="postgresql_conn",
        sql="""
            -- 1. Actualizar los que están presentes hoy. 
            -- Si VTEX dice isActive=true, eliminacion=NULL. Si VTEX dice isActive=false, asignamos eliminacion.
            INSERT INTO forecast_and_planning.poligonos_historico (
                polygon, transportadora, "shippingMethod", "deliveryChannel", 
                "isActive", coordenadas, fecha_creacion_vtex, 
                fecha_actualizacion, fecha_eliminacion
            )
            SELECT 
                polygon,
                MAX(name::TEXT) as transportadora,
                MAX("shippingMethod"::TEXT) as "shippingMethod",
                MAX("deliveryChannel"::TEXT) as "deliveryChannel",
                bool_or("isActive") as "isActive",
                MAX(coordenadas::TEXT) as coordenadas,
                '{{ ds }}'::date as fecha_creacion_vtex,
                '{{ ds }}'::date as fecha_actualizacion,
                CASE WHEN bool_or("isActive") THEN NULL ELSE '{{ ds }}'::date END as fecha_eliminacion
            FROM forecast_and_planning.poligonos
            WHERE fecha = '{{ ds }}'::date
              AND polygon IS NOT NULL
            GROUP BY polygon
            ON CONFLICT (polygon) DO UPDATE SET
                transportadora = EXCLUDED.transportadora,
                "shippingMethod" = EXCLUDED."shippingMethod",
                "deliveryChannel" = EXCLUDED."deliveryChannel",
                "isActive" = EXCLUDED."isActive",
                fecha_actualizacion = CASE 
                    WHEN forecast_and_planning.poligonos_historico.coordenadas IS DISTINCT FROM EXCLUDED.coordenadas
                      OR forecast_and_planning.poligonos_historico.transportadora IS DISTINCT FROM EXCLUDED.transportadora
                      OR forecast_and_planning.poligonos_historico."shippingMethod" IS DISTINCT FROM EXCLUDED."shippingMethod"
                      OR forecast_and_planning.poligonos_historico."deliveryChannel" IS DISTINCT FROM EXCLUDED."deliveryChannel"
                      OR forecast_and_planning.poligonos_historico."isActive" IS DISTINCT FROM EXCLUDED."isActive"
                    THEN GREATEST(forecast_and_planning.poligonos_historico.fecha_actualizacion, EXCLUDED.fecha_actualizacion)
                    ELSE forecast_and_planning.poligonos_historico.fecha_actualizacion
                END,
                coordenadas = EXCLUDED.coordenadas,
                fecha_creacion_vtex = LEAST(forecast_and_planning.poligonos_historico.fecha_creacion_vtex, EXCLUDED.fecha_creacion_vtex),
                fecha_eliminacion = CASE 
                    WHEN EXCLUDED."isActive" THEN NULL
                    ELSE COALESCE(forecast_and_planning.poligonos_historico.fecha_eliminacion, EXCLUDED.fecha_eliminacion)
                END;

            -- 2. Marcar como eliminados y NO ACTIVOS aquellos que ya ni siquiera vinieron hoy
            UPDATE forecast_and_planning.poligonos_historico
            SET 
                fecha_eliminacion = COALESCE(fecha_eliminacion, '{{ ds }}'::date - INTERVAL '1 day'),
                "isActive" = FALSE
            WHERE ("isActive" = TRUE OR fecha_eliminacion IS NULL)
              AND polygon NOT IN (
                  SELECT polygon 
                  FROM forecast_and_planning.poligonos 
                  WHERE fecha = '{{ ds }}'::date
                    AND polygon IS NOT NULL
              );
        """
    )
    
    t1
