from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from datetime import datetime
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values


def obtener_y_cargar_ventanas():
    # Cargar variables
    PG_HOST = Variable.get("POSTGRESQL_HOST")
    PG_USER = Variable.get("POSTGRESQL_USER")
    PG_PASS = Variable.get("POSTGRESQL_PASSWORD")
    PG_DB   = Variable.get("POSTGRESQL_DB")
    VTEX_APP_KEY    = Variable.get("X_VTEX_API_AppKey")
    VTEX_APP_TOKEN  = Variable.get("X_VTEX_API_AppToken")

    HEADERS = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-VTEX-API-AppKey": VTEX_APP_KEY,
        "X-VTEX-API-AppToken": VTEX_APP_TOKEN
    }

    # Obtener ubicaciones VTEX desde PostgreSQL
    query = """
    SELECT DISTINCT latitud, longitud, sales_channel
    FROM forecast_and_planning.ubicaciones_despacho_vtex
    WHERE latitud IS NOT NULL AND longitud IS NOT NULL AND sales_channel IS NOT NULL;
    """
    conn = psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port="5432"
    )
    df_coords = pd.read_sql(query, conn)
    conn.close()

    # Función para consultar ventanas desde VTEX
    def obtener_ventanas_despacho(lat, lon, sc):
        url = f"https://unimarc.vtexcommercestable.com.br/api/checkout/pvt/orderForms/simulation?sc={sc}"
        payload = {
            "items": [{"id": "66916", "quantity": "1", "seller": "1"}],
            "country": "CHL",
            "geoCoordinates": [lon, lat]
        }

        try:
            response = requests.post(url, json=payload, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            resultados = []
            for sla in data.get("logisticsInfo", [])[0].get("slas", []):
                courier = sla.get("deliveryIds", [{}])[0]
                for v in sla.get("availableDeliveryWindows", []):
                    resultados.append({
                        "metodo_despacho": sla.get("name"),
                        "courier_id": courier.get("courierId"),
                        "courier_name": courier.get("courierName"),
                        "start": v["startDateUtc"],
                        "end": v["endDateUtc"]
                    })
            return resultados
        except Exception as e:
            print(f"❌ Error en ({lat}, {lon}) - SC {sc}: {e}")
            return []

    # Ejecutar y recolectar resultados
    resultados = []
    for _, row in df_coords.iterrows():
        lat = row["latitud"]
        lon = row["longitud"]
        sc = row["sales_channel"]

        print(f"🔍 Consultando ventanas de despacho para ({lat}, {lon}) - SC {sc}...")

        ventanas = obtener_ventanas_despacho(lat, lon, sc)
        for v in ventanas:
            resultados.append({
                "metodo_despacho": v["metodo_despacho"],
                "courier_id": v["courier_id"],
                "courier_name": v["courier_name"],
                "sales_channel": sc,
                "fecha_inicio": v["start"],
                "fecha_fin": v["end"],
                "latitud": lat,
                "longitud": lon
            })

    # Guardar en PostgreSQL
    if not resultados:
        print("⚠️ No se encontraron ventanas.")
        return

    df_resultado = pd.DataFrame(resultados)
    df_resultado["fecha_inicio"] = pd.to_datetime(df_resultado["fecha_inicio"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_resultado["fecha_fin"] = pd.to_datetime(df_resultado["fecha_fin"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df_resultado = df_resultado.drop_duplicates(subset=["courier_id", "fecha_inicio", "fecha_fin"])

    columnas = [
        "metodo_despacho", "courier_id", "courier_name",
        "sales_channel", "fecha_inicio", "fecha_fin", "latitud", "longitud"
    ]
    registros = [tuple(row[col] for col in columnas) for _, row in df_resultado.iterrows()]

    conn_insert = psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port="5432"
    )
    insert_query = f"""
        INSERT INTO forecast_and_planning.ventanas_despacho_vtex ({', '.join(columnas)})
        VALUES %s
        ON CONFLICT (courier_id, fecha_inicio, fecha_fin)
        DO NOTHING
        RETURNING 1;
    """
    with conn_insert.cursor() as cur:
        execute_values(cur, insert_query, registros)
        inserted = cur.rowcount
        conn_insert.commit()
    conn_insert.close()

    print(f"✅ Insertados {inserted} nuevos registros (con ON CONFLICT DO NOTHING).")


# DAG definition
default_args = {
    'owner': 'ecommerce_data',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    'etl_vtex_ventanas_despacho',
    default_args=default_args,
    description='DAG para cargar ventanas de despacho VTEX desde ubicaciones',
    schedule_interval="30 8 * * *",
    start_date=pendulum.datetime(2025, 2, 5, tz="America/Santiago"),
    catchup=False,
    tags=["VTEX", "ventanas", "despacho", "historico"]
) as dag:
    t0 = PythonOperator(
        task_id="cargar_ventanas_despacho_vtex",
        python_callable=obtener_y_cargar_ventanas
    )

    t0
