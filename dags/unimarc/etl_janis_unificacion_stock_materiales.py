from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pendulum

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

def _send_stock_sap_to_janis(ds):
    import pandas as pd
    import requests
    import json
    from collections import defaultdict
    from airflow.models import Variable



    # 📄 Query embebida desde SAP
    query = f"""
        select sdb.sku_product AS SKU_PRODUCT   
            , sdb.id_tienda as OU_ID
            , sdb.nbr_item AS STOCK
        from ecommdata.stock_dw_bq sdb 
        where sdb.id_tienda in ('0469',
            '0069',
            '0472',
            '0458',
            '0109',
            '0465',
            '0903',
            '0333',
            '0347',
            '0917',
            '0336',
            '0332',
            '0581',
            '0717',
            '0017',
            '0375',
            '0788',
            '0959',
            '0914',
            '0920',
            '0034',
            '0111',
            '1917',
            '0445',
            '0778',
            '0025',
            '0686',
            '0086',
            '0464',
            '0681',
            '0755',
            '0442',
            '0736',
            '0713',
            '0761',
            '0345',
            '0344',
            '0626',
            '0021',
            '0007',
            '0441',
            '0600',
            '0028',
            '0767',
            '0916',
            '0018',
            '0089',
            '0362',
            '0327',
            '0088'
        )
        and sdb.fecha = current_date
        and sdb.nbr_item > 0
        and sdb.SKU_PRODUCT IN ('000000000000051712', '000000000000051813',
                '000000000000051728', '000000000000051845',
                '000000000000051802', '000000000000668742',
                '000000000000051806', '000000000000674766',
                '000000000000038087', '000000000000038088')
    """
    
    df = query_to_df(query)
    print(f"🔍 Filas extraídas desde DW: {len(df)}")

    if df.empty:
        raise Exception("❌ No hay stock disponible para los SKU solicitados.")

    # 📦 Pares de SKU
    sku_pares = [
        ('000000000000051712', '000000000000051813'),
        ('000000000000051728', '000000000000051845'),
        ('000000000000051802', '000000000000668742'),
        ('000000000000051806', '000000000000674766'), ## stock produtos ariztia + super pollo
        ('000000000000038087', '000000000000038088')  ## stock productos platanos
    ]

    # 🧩 Excepciones warehouse
    warehouse_excepciones = {
        '0332': '15f52fc',
        '0469': '0003',
        '0581': '18bced3',
        '0917': '193949d',
        '0956': '956',
    }
    tiendas_sin_warehouse_default = ['0463', '0486', '0576', '0915', '0931', '0979']

    # 🌐 Config API Janis
    base_url = Variable.get("JANIS_API_URL")
    url = f"{base_url}stock"
    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    # 🧱 Agrupación por tienda y SKU
    grouped = df.groupby(['OU_ID', 'SKU_PRODUCT'])['STOCK'].sum().reset_index()

    df["STOCK"] = df["STOCK"].astype(float).round(2)

    tienda_to_payload = defaultdict(list)
    total_tiendas = 0
    total_skus_enviados = 0

    for tienda_id in grouped['OU_ID'].unique():
        if tienda_id in tiendas_sin_warehouse_default:
            print(f"⛔ Tienda {tienda_id} excluida.")
            continue

        warehouse = warehouse_excepciones.get(tienda_id, tienda_id)
        skus_cargados = 0
        unidades_totales = 0

        for sku1, sku2 in sku_pares:
            subset = grouped[
                (grouped['OU_ID'] == tienda_id) &
                (grouped['SKU_PRODUCT'].isin([sku1, sku2]))
            ]

            if subset.empty:
                continue

            print(f"\n📊 [ANTES] Stock individual en tienda {tienda_id} para el par ({sku1}, {sku2}):")
            stock_individual = {}
            for _, row in subset.iterrows():
                stock_val = int(row['STOCK'])
                stock_individual[row['SKU_PRODUCT']] = stock_val
                print(f"   - SKU: {row['SKU_PRODUCT']} → {stock_val} unidades")

            total_stock = sum(stock_individual.values())
            print(f"➕ [UNIFICADO] Stock total combinado para el par ({sku1}, {sku2}) en tienda {tienda_id}: {total_stock} unidades")

            for sku in (sku1, sku2):
                tienda_to_payload[tienda_id].append({
                    "IdSku": sku,
                    "Quantity": int(total_stock),
                    "Store": tienda_id,
                    "Warehouse": warehouse,
                    "Type": 1
                })
                print(f"🧾 Payload → SKU: {sku} | Quantity: {total_stock} | Store: {tienda_id} | Warehouse: {warehouse}")
                skus_cargados += 1
                unidades_totales += total_stock
        print(f"🧾 Tienda: {tienda_id} | SKU: {sku} | Unified Stock: {total_stock} | Warehouse: {warehouse}")

        if skus_cargados > 0:
            total_tiendas += 1
            total_skus_enviados += skus_cargados
            print(f"📌 {tienda_id} → {skus_cargados} SKUs | {unidades_totales} unidades totales")

    # 📤 Envío a Janis
    for tienda, payload in tienda_to_payload.items():
        print(f"⬆️ Enviando {len(payload)} SKUs a tienda {tienda}")  
        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=10)
            print(f"📦 Store: {tienda} | Status: {response.status_code} | Response: {response.text}")
            if response.status_code != 200:
                print(f"❌ Error ({response.status_code}) al cargar tienda {tienda}: {response.text}")
            else:
                print(f"✅ Carga exitosa para tienda {tienda}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Excepción al cargar tienda {tienda}: {str(e)}")



# 🎛 Configuración del DAG
default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    'etl_janis_unificacion_stock_materiales',
    default_args=default_args,
    description="Suma el stock de pares de ref_id específicos de productos y los carga duplicados por SKU en la API de Janis (pendiente de escalabilidad)",
    schedule_interval= "0 7 * * *", 
    start_date=pendulum.datetime(2025, 6, 4, tz="America/Santiago"),
    catchup=False,
    tags=["Janis", "Pollos", "Stock", "ecommdata","KEVIN"],
) as dag:

    dag.doc_md = """
    ### DAG: etl_janis_unificacion_stock_materiales
    Suma el stock de pares de ref_id específicos de productos y los carga duplicados por SKU en la API de Janis.
    """

    t0 = PythonOperator(
        task_id="send_stock_sap_to_janis",
        python_callable=_send_stock_sap_to_janis,
    )

    t0 
