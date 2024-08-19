from airflow import DAG
from airflow import macros
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json
import pendulum

def venta_mfc_semana():
    import pandas as pd
    ventas_query = """WITH stock_takeoff AS (
            select tom_id, quantity_on_hand
            from ecommdata.stock_mfc_takeoff smt2
            where fecha = (select max(fecha) from ecommdata.stock_mfc_takeoff smt3 where fecha >= current_date)
            ),
            multiplicador as (
            select distinct ref_id, multiplicador_unidad_medida 
            from ecommdata.skus s
            where erp_id in (select material from ecommdata.maestra_reposicion_mfc mrm)
            and ref_id in (select distinct material||'-'||umv from ecommdata.lista8 l where id_tienda = '0917')	)
        select distinct mrm.*,
        vpsm.domingo, vpsm.lunes, vpsm.martes,vpsm.miercoles,vpsm.jueves,vpsm.viernes,vpsm.sabado,
        um.mfc_is_item_side,
        case
            when st.quantity_on_hand is null then 0 
            else st.quantity_on_hand
        end as "stock_takeoff",
        m.multiplicador_unidad_medida
        from ecommdata.maestra_reposicion_mfc mrm 
        left join ecommdata.venta_prom_semanal_mfc vpsm
        on vpsm.material = mrm.material
        left join ecommdata.ubicacion_mfc um 
        on mrm.material = um.sap_code 
        left join stock_takeoff st
        on split_part(st.tom_id,'-',1) = mrm.material
        left join multiplicador m
        on mrm.material = split_part(m.ref_id ,'-',1)
        where m.multiplicador_unidad_medida is not null
                    """
    print(ventas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()

    return results

def reposicion():
    import pandas as pd
    ventas_query = """ with producto as (
                    select distinct material, nombre, id_categoria
                    from ecommdata.productos p
                    where material in (select material from ecommdata.maestra_reposicion_mfc mrm)
                    and ref_id in (select distinct material||'-'||umv from ecommdata.lista8 l where id_tienda = '0917'))
                select distinct msr.material,c.n1,c.n2,c.n3,p.nombre,msr.solicitado,
                msr.reponer,
                case 
                    when _t.promedio_umv_boleta is null then null
                    else ((1/_t.promedio_umv_boleta)*solicitado)::numeric(6,0)
                end as "cargar_Tom"
                from ecommdata.mfc_solicitud_reposicion msr
                left join producto p 
                on p.material  = msr.material
                left join (select distinct split_part(ved.ref_id_sku,'-',1) as material, round(avg(ved.venta_umv),1) as "promedio_umv_boleta"
                        from ecommdata.ventas_ecommerce_datawarehouse ved
                        where id_tienda = '0917'
                        and fecha_facturacion::date > current_date -75
                        and venta_umv > 0
                        and split_part(ved.ref_id_sku,'-',2) = 'KG'
                        and split_part(ved.ref_id_sku,'-',1) in (select material from ecommdata.maestra_reposicion_mfc mrm)
                        group by split_part(ved.ref_id_sku,'-',1)
                        ) as _t
                on _t.material = msr.material
                left join ecommdata.categorias c 
                on p.id_categoria = c.id
                where msr.solicitado >0
                and msr.reponer is true;
                    """
    print(ventas_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ventas_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    results = pd.DataFrame(results, columns=column_names)
    print(results.head(20))
    cursor.close()
    pg_connection.close()

    return results
def calcular_venta_futura(row, dias_de_la_semana, nombre_dia):
        index_dia_actual = dias_de_la_semana.index(nombre_dia)
        ventas_futuras = 0
        # Asegura que el 'lead_time' es un entero y maneja casos donde podría ser NaN o similar
        lead_time = int(row.get('lead_time', 0))
        for i in range(lead_time):
            dia = dias_de_la_semana[(index_dia_actual + i) % len(dias_de_la_semana)]
            ventas_futuras += row.get(dia, 0)  # Asume 0 si no hay datos para ese día
        return ventas_futuras

def reposicion_to_s3(ds):
    import pandas as pd
    import numpy as np
    import io

    exec_date = ds.replace("-", "/")
    date_aux = ds.replace("-", "_")
    prefix = f"mfc_reposicion/{exec_date}/"
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")

    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    df = venta_mfc_semana()

    fecha = datetime.strptime(ds, '%Y-%m-%d')
    print(fecha)
    dia_de_la_semana = (fecha.weekday()+1)%7
    print(dia_de_la_semana)
    nombre_dia = ['domingo', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado'][dia_de_la_semana]
    print(nombre_dia)

    # Aplicamos la condición del contador igual a 0
    #df = df[df['contador'] == 0]
    df.info()

    dias_de_la_semana = ['domingo', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado']
    df['venta_futura'] = df.apply(calcular_venta_futura, args=(dias_de_la_semana, nombre_dia), axis=1)

    # Lógica para decidir si se necesita reponer actualizada para usar venta_futura
    df['stock_takeoff'] = df['stock_takeoff'].fillna(0)

    condlist = [
        df["venta_futura"] > df["minimo"],
        df["venta_futura"] <= df["minimo"]
    ]
    choicelist = [True, False]
    df["reponer"] = np.select(condlist, choicelist)

    condlist = [
        df["reponer"] == False,
        (df["reponer"] == True) & (df["stock_takeoff"] > df["venta_futura"]),
        (df["reponer"] == True) & (df["stock_takeoff"] <= df["venta_futura"])
    ]
    choicelist = [False, False, True]
    df["reponer"] = np.select(condlist, choicelist)

    # Ajustar 'solicitado' en función de 'maximo' y 'minimo'
    df["venta_hoy"] = df[str(nombre_dia)]
    df["stock_objetivo"] = df["doh_objetivo"] * df["venta_hoy"]
    print(df.head())
    df["solicitado"] = df["stock_objetivo"] + df["venta_futura"] - df["stock_takeoff"]

    #ajustar por limites de max y min
    df["solicitado"] = np.select(
        [df["solicitado"] > df["maximo"], df["solicitado"] < df["minimo"]],
        [df["maximo"], df["minimo"]],
        default=df["solicitado"]
    )

    #en caso que sol == max, restar 
    df["solicitado"] = np.select(
        [df["solicitado"] == df["maximo"], df["solicitado"] + df["stock_takeoff"] >= df["maximo"]],
        [df["maximo"]-df["stock_takeoff"], df["maximo"]-df["stock_takeoff"]],
        default=df["solicitado"]
    )
    print()
    df['multiplicador_unidad_medida'] = df['multiplicador_unidad_medida'].astype(float)
    df["solicitado"] = np.ceil(df["solicitado"] / df["multiplicador_unidad_medida"]) * df["multiplicador_unidad_medida"]

    condlist = [
        df["reponer"] == False,
        (df["reponer"] == True) & (df["solicitado"] > 0),
        (df["reponer"] == True) & (df["solicitado"] <= 0)
    ]
    choicelist = [False, True, False]
    df["reponer"] = np.select(condlist, choicelist)
    # Mantenemos solo los registros donde 'reponer' es True o 1 ?
    #df = df[df["reponer"] == 1]
    #df = df[df['solicitado'] > 0]

    # Convertimos el DataFrame a un archivo CSV y lo cargamos a S3
    buffer = io.StringIO()
    df.to_csv(buffer, header=True, index=False, encoding="utf-8")
    filename = f"mfc_reposicion/{exec_date}/mfc_reposicion_{date_aux}.csv"
    buffer.seek(0)
    s3_hook.load_string(buffer.getvalue(), key=filename, bucket_name=s3_bucket, replace=True)

    print(f"Archivo cargado en S3: {prefix}{filename}")
    return filename

def reposicion_to_postgres(ti):
    import numpy as np
    import pandas as pd
    import sqlalchemy
    from sqlalchemy import text

    filename = ti.xcom_pull(key="return_value", task_ids=["reposicion_to_s3"])[0]

    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

    print("Searching file: "+filename)
    if not s3_hook.check_for_key(filename, bucket_name=s3_bucket):
        raise Exception("Key %s does not exist." % filename)

    s_stock_object = s3_hook.get_key(filename, bucket_name=s3_bucket)

    df = pd.read_csv(s_stock_object.get()["Body"])
    if len(df.index) == 0:
        print("There are no new nor updated records to load. Task will exit as successfull.")
        return
    
    print(f"Number of records extracted: {len(df.index)}")
    df["material"] = df["material"].apply(lambda x: str(x).zfill(18))
    df = df[['material','maximo','minimo','stock_takeoff','venta_futura','reponer','solicitado']]
    df.columns = ['material','maximo','minimo','stock_takeoff','venta','reponer','solicitado']
    df['reponer'] = df['reponer'].astype(bool)
    df = df.drop_duplicates()
    df.info()

    host = Variable.get("POSTGRESQL_HOST")
    database = Variable.get("POSTGRESQL_DB")
    username = Variable.get("POSTGRESQL_USER")
    password = Variable.get("POSTGRESQL_PASSWORD")
    
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/"+database
    engine = sqlalchemy.create_engine(conn_url)

    with engine.begin() as conn:
        conn.execute("TRUNCATE ecommdata.mfc_solicitud_reposicion") 
        df.to_sql(name="mfc_solicitud_reposicion",
                    con=conn,         
                    schema="ecommdata",         
                    if_exists='append',         
                    index=False,         
                    chunksize=20000,         
                    method='multi')

    print("Data saved to PostgreSQL.")

    return

def picking_order_janis(ds):
    import requests
    import io
    import pandas as pd

    JANIS_ORDER_URL = "https://janis.in/api/oms/order"

    headers = {
        "janis-api-key": Variable.get("JANIS_API_KEY"),
        "janis-api-secret": Variable.get("JANIS_API_SECRET"),
        "janis-client": Variable.get("JANIS_CLIENT"),
        "Connection": "keep-alive"
    }

    df = reposicion()
    df['material'] = df['material'].astype(str) + '-UN'

    materiales = df['material'].unique().tolist()
    materiales_str = ",".join([f"'{material}'" for material in materiales])

    ordenes_query = f"""
                WITH base_query AS (
                    SELECT 
                        s.ref_id,
                        s.nombre_sku,
                        c.n2 as categoria,
                        CONCAT('https://unimarc.vteximg.com.br', is2.imagen) AS link_imagen
                    FROM 
                        ecommdata.skus s
                    LEFT JOIN 
                        ecommdata.productos p ON s.ref_id = p.ref_id
                    LEFT JOIN 
                        ecommdata.categorias c ON p.id_categoria = c.id
                    LEFT JOIN 
                        ecommdata.imagenes_sku is2 ON is2.ref_id = s.ref_id
                    WHERE
                        is2.imagen ilike '%UN-01%'
                    AND 
                        s.ref_id IN ({materiales_str}) -- dynamically populated
                ),
                lowest_id AS (
                    SELECT MIN(id) - 1 AS min_id FROM ecommdata.ordenes_janis_38
                ),
                grouped_skus AS (
                    SELECT 
                        bq.*,
                        DENSE_RANK() OVER (ORDER BY bq.categoria) AS group_rank,
                        li.min_id
                    FROM 
                        base_query bq, 
                        lowest_id li
                )
                SELECT 
                    ref_id,
                    nombre_sku,
                    categoria,
                    link_imagen,
                    (min_id - group_rank + 1) AS id_orden
                FROM 
                    grouped_skus;
                """
    print(ordenes_query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    pg_connection = pg_hook.get_conn()
    cursor = pg_connection.cursor()
    cursor.execute(ordenes_query)
    column_names = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    df_skus = pd.DataFrame(results, columns=column_names)
    cursor.close()
    pg_connection.close()

    df_joined = pd.merge(df_skus, df[['material', 'solicitado']], left_on='ref_id', right_on='material', how='left')
    df_joined.drop('material', axis=1, inplace=True)

    order_forms = []
    grouped = df_joined.groupby('id_orden')
    
    for id_orden, group in grouped:
        items = []
        item_quantity = 0
        product_quantity = 0
        
        for numero_sku, row in enumerate(group.itertuples(), start=0):
            item = {
                "itemIndex": numero_sku,
                "skuRefId": row.ref_id,
                "skuName": row.nombre_sku,
                "quantity": row.solicitado,
                "price": 1,
                "commercialCondition": 1,
                "imageUrl": row.link_imagen
            }
            items.append(item)
            item_quantity += row.solicitado
            product_quantity += 1

        order_form = {
            "ecommercePlatformId": "2",
            "salesChannel": "39",
            "storeRefId": "0054",
            "seqId": id_orden,
            "ecomId": id_orden,
            "customer": {
                "docType": "rutCHL",
                "doc": "150766362",
                "email": "sgil@smu.cl",
                "firstname": "Sergio",
                "lastname": "Gil",
                "phone": "948857331"
            },
            "customerAddress": {
                "ecomId": "NotAvailableNotAvailable",
                "city": "Santiago",
                "country": "CHL",
                "number": "5100",
                "postalCode": "null",
                "state": "REGIÓN METROPOLITANA",
                "street": "Av. Los Pajaritos",
                "streetType": "route",
                "neighborhood": "Maipú",
                "complement": None,
                "reference": None,
                "receiver": "Sergio Gil",
                "lat": "70.741198",
                "lng": "-33.4745884"
            },
            "items": items,
            "payments": [
                {
                    "transactionId": id_orden, 
                    "paymentId": id_orden,
                    "paymentSystemRefId": "916",
                    "value": 1,
                    "referenceValue": 1
                }
            ],
            "shippings": [
                {
                    "country": "CL",
                    "city": "Santiago",
                    "state": "Estacion Central",
                    "street": "Coronel Godoy",
                    "number": "0128",
                    "neighborhood": "Estacion Central",
                    "postalCode": "77539",
                    "complement": "",
                    "receiver": "Sergio Gil",
                    "shippingDate": ds
                }
            ],
            "logistics": [
                {
                    "carrierRefId": "0054",
                    "warehouseRefId": "0054",
                    "itemIndex": 0,
                    "logisticPrice": 0,
                    "logisticListPrice": 0,
                    "logisticSellingPrice": 0,
                    "shippingEstimateDate": ds
                }
            ],
            "itemsQty": item_quantity,
            "productQty": product_quantity,
            "trackingNumber": "",
            "total": 1,
            "totalItems": product_quantity,
            "totalShipping": 0,
            "customData": []
        }
        print(order_form)
        order_forms.append(order_form)

    for order_form in order_forms:
        response = requests.post(JANIS_ORDER_URL, json=order_form, headers=headers)
        if response.status_code == 200:
            print(f"Order {order_form['seqId']} created successfully.")
        else:
            print(f"Failed to create order {order_form['seqId']} with status code {response.status_code}.")
            print(f"Error Message: {response.text}")

    return

def reposicion_to_slack():
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    import io
    import pandas as pd

    df = reposicion()

    with io.BytesIO() as buffer:
        df.to_csv(buffer, index=False, encoding='utf-8')
        buffer.seek(0)
        
        token = Variable.get("token_slack")
        
        client = WebClient(token=token)
        
        try:
            response = client.files_upload(
                channels="alertas-reposiciones-mfc",
                file=buffer,
                filename="reporte_reposicion.csv",
                title="Reporte de Reposición",
                initial_comment="Aquí está el reporte de reposición actualizado."
            )
        except SlackApiError as e:
            print(f"Error al subir archivo: {e}")

    return


default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_reposicion_mfc',
    default_args=default_args,
    description="consulta de datos de Stock MFC, maestra reposicion desde postgres para logica de reposicion.",
    schedule_interval="0 7,19 * * *",
    start_date=pendulum.datetime(2022, 8, 25, tz="America/Santiago"),
    catchup=False,
    max_active_runs = 1,
    tags=["DATA", "MFC", "ecommdata","SLACK" ,"PATRICIO"],
) as dag:

    dag.doc_md = """
    genera unidades solicitadas para mfc en picking tienda.
    """ 

    t0 = PythonOperator(
        task_id = "reposicion_to_s3",
        python_callable = reposicion_to_s3
    )
    t1 = PythonOperator(
        task_id = "reposicion_to_postgres",
        python_callable = reposicion_to_postgres
    )
    t2 = PythonOperator(
        task_id = "picking_order_janis",
        python_callable = picking_order_janis
    )
    t3 = PythonOperator(
        task_id = "reposicion_to_slack",
        python_callable = reposicion_to_slack
    )
    t4 = PostgresOperator(
        task_id = "update_contador",
        postgres_conn_id = "postgresql_conn",
        sql = """BEGIN;
            UPDATE ecommdata.maestra_reposicion_mfc
            SET contador = contador - 0.5;
            UPDATE ecommdata.maestra_reposicion_mfc
            SET contador = lead_time
            WHERE contador < 0;
            COMMIT;"""
    )
    t0 >> t1 >> t2 >> t3 >> t4