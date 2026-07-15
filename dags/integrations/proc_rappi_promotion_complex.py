from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import pendulum
import io
import pandas as pd
import csv
import pysftp
from utils.slack_utils import dag_success_slack, dag_failure_slack

def get_rappi_active_stores():
    """
    Tarea 1: Obtener la lista de tiendas activas de Rappi desde PostgreSQL.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    query = """
        SELECT id, id_rappi
        FROM integraciones.tiendas_last_millers
        WHERE id_rappi IS NOT NULL AND id_rappi <> '';
    """
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

def extract_promotions(ds, store_ids):
    """
    Función desacoplada para extraer promociones complejas crudas (tipo 2, 7) para tiendas específicas.
    Usa la fecha lógica de Airflow (ds) para mantener la idempotencia.
    """
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn")
    
    # Formatear los IDs de tienda para la cláusula SQL IN
    store_ids_str = ", ".join([f"'{s}'" for s in store_ids])
    
    query = f"""
    SELECT DISTINCT
        tlm.id_rappi AS store_id,
        tlm.id AS smu_store_id,
        wp.fecha_inicio_de_promocion AS start_date_raw,
        wp.fecha_fin_de_promocion AS end_date_raw,
        wp.desc_promocion,
        wp.cantidad_n,
        wp.cantidad_m,
        wp.precio_total_promocional,
        wp.descripcion_material AS name,
        lspp.material AS id
    FROM integraciones.lm_stock_precio_promo lspp
    INNER JOIN integraciones.tiendas_last_millers tlm 
        ON tlm.id = lspp.id_tienda 
    INNER JOIN ecommdata.workflow_promociones wp 
        ON wp.material = lspp.material 
        AND (CASE WHEN wp.umv = 'ST' THEN 'UN' ELSE wp.umv END) = lspp.unidad_de_medida
    WHERE wp.fecha_inicio_de_promocion <= '{ds}'
      AND wp.fecha_fin_de_promocion >= '{ds}'
      AND tlm.id IN ({store_ids_str})
      AND wp.tipo_promocion IN (2, 7)
      AND wp.registro_valido = TRUE
      AND wp.organizacion_ventas = '1000'
      AND wp.canal_distribucion = '10'
      AND wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123, 124)
      AND wp.nombre_promocion::text !~~ '%MFC%'::text
      AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
      AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
      AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
      AND wp.nombre_promocion::text !~~ '%917%'::text
      AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
      AND wp.nombre_promocion::text !~~ '% LOC%'::text
      AND wp.nombre_promocion::text !~~ '%LIQ%'::text
      AND wp.nombre_promocion::text !~~ '%CYBER%'::text
      AND wp.nombre_promocion::text !~~ '%REGIO%'::text
      AND wp.n_promocion NOT IN (
          '5552392024','1120012024','1120022024','1120032024','1120042024',
          '1120052024','1120062024','1120082024','1120092024','1120102024',
          '1120112024','1120122024','4000512024','5552792024','5552852024'
      );
    """
    return pg_hook.get_pandas_df(query)

def build_type_format(row):
    """
    Construye el formato del tipo de promoción (type_format):
    - LX_PY para promociones NXM
    - N{quantity}_M{bundle_price} para promociones NX$
    """
    desc_promo = str(row.get('desc_promocion', '')).strip().upper()
    try:
        if desc_promo == 'COMBINACION NXM':
            buy = int(float(row.get('cantidad_n', 0)))
            pay = int(float(row.get('cantidad_m', 0)))
            return f"L{buy}_P{pay}"
        elif desc_promo == 'COMBINACION NX$':
            qty = int(float(row.get('cantidad_n', 0)))
            price = int(float(row.get('precio_total_promocional', 0)))
            return f"N{qty}_M{price}"
    except (ValueError, TypeError):
        pass
    return None

def build_description(row):
    """
    Construye la descripción en español por defecto:
    - "Compra X y paga Y" para LX_PY
    - "X unidades por $Y.YYY" para N_M
    """
    desc_promo = str(row.get('desc_promocion', '')).strip().upper()
    try:
        if desc_promo == 'COMBINACION NXM':
            buy = int(float(row.get('cantidad_n', 0)))
            pay = int(float(row.get('cantidad_m', 0)))
            return f"Compra {buy} y paga {pay}"
        elif desc_promo == 'COMBINACION NX$':
            qty = int(float(row.get('cantidad_n', 0)))
            price = int(float(row.get('precio_total_promocional', 0)))
            price_formatted = f"{price:,}".replace(",", ".")
            return f"{qty} unidades por ${price_formatted}"
    except (ValueError, TypeError):
        pass
    return None

def validate_promotion(row):
    """
    Realiza validaciones sobre las filas de promociones crudas.
    Retorna (True, None) si es válida, o (False, error_reason) si es inválida.
    """
    required_raw = ['store_id', 'id', 'name', 'desc_promocion']
    for field in required_raw:
        val = row.get(field)
        if pd.isnull(val) or str(val).strip() == '':
            return False, f"Falta campo obligatorio crudo: {field}"
            
    desc_promo = str(row.get('desc_promocion', '')).strip().upper()
    if desc_promo not in ['COMBINACION NXM', 'COMBINACION NX$']:
        return False, f"Tipo de promoción no soportado: {desc_promo}"
        
    start_val = row.get('start_date_raw')
    end_val = row.get('end_date_raw')
    if pd.isnull(start_val) or pd.isnull(end_val):
        return False, "Falta start_date o end_date"
        
    try:
        if isinstance(start_val, str):
            start_dt = pd.to_datetime(start_val).to_pydatetime()
        else:
            start_dt = start_val
        if isinstance(end_val, str):
            end_dt = pd.to_datetime(end_val).to_pydatetime()
        else:
            end_dt = end_val
    except Exception as e:
        return False, f"Formato de fecha inválido: {e}"
        
    if end_dt < start_dt:
        return False, f"La fecha de fin ({end_dt}) es anterior a la fecha de inicio ({start_dt})"
        
    try:
        qty_n_val = row.get('cantidad_n')
        if pd.isnull(qty_n_val):
            return False, "Falta cantidad_n"
            
        qty_n = float(qty_n_val)
        if qty_n <= 0 or not qty_n.is_integer():
            return False, f"cantidad_n inválida (debe ser entero > 0): {qty_n_val}"
        qty_n = int(qty_n)
        
        if desc_promo == 'COMBINACION NXM':
            qty_m_val = row.get('cantidad_m')
            if pd.isnull(qty_m_val):
                return False, "Falta cantidad_m"
            qty_m = float(qty_m_val)
            if qty_m <= 0 or not qty_m.is_integer():
                return False, f"cantidad_m inválida (debe ser entero > 0): {qty_m_val}"
            qty_m = int(qty_m)
            
            if qty_n <= 1:
                return False, f"Para LX_PY, la cantidad comprada (buy_quantity) debe ser > 1: {qty_n}"
            if qty_m >= qty_n:
                return False, f"Para LX_PY, la cantidad pagada ({qty_m}) debe ser menor que la cantidad comprada ({qty_n})"
                
        elif desc_promo == 'COMBINACION NX$':
            price_val = row.get('precio_total_promocional')
            if pd.isnull(price_val):
                return False, "Falta precio_total_promocional"
            price = float(price_val)
            if price <= 0 or not price.is_integer():
                return False, f"Precio de paquete inválido (debe ser entero > 0): {price_val}"
            if qty_n <= 1:
                return False, f"Para N_M, la cantidad (quantity) debe ser > 1: {qty_n}"
                
    except (ValueError, TypeError) as e:
        return False, f"Error al validar los valores numéricos: {e}"
        
    return True, None

def transform_promotion(row):
    """
    Transforma la fila cruda al esquema exacto esperado por Rappi.
    - Formatea las fechas como YYYY/MM/DD
    - Construye descripciones y formatos con funciones auxiliares
    - El ID (SKU) se mantiene como string sin decimales ni ceros a la izquierda
    """
    store_id = str(row['store_id']).strip()
    
    start_dt = pd.to_datetime(row['start_date_raw'])
    end_dt = pd.to_datetime(row['end_date_raw'])
    start_date = start_dt.strftime('%Y/%m/%d')
    end_date = end_dt.strftime('%Y/%m/%d')
    
    description = build_description(row)
    type_format = build_type_format(row)
    name = str(row['name']).strip()
    sku_id = str(int(float(row['id']))) if str(row['id']).replace('.','').isdigit() else str(row['id']).strip()
    
    return {
        'store_id': store_id,
        'start_date': start_date,
        'end_date': end_date,
        'description': description,
        'type_format': type_format,
        'name': name,
        'id': sku_id
    }

def write_csv(df, buffer):
    """
    Escribe el DataFrame en el buffer StringIO con el orden correcto de columnas.
    """
    columns = ['store_id', 'start_date', 'end_date', 'description', 'type_format', 'name', 'id']
    df_out = df[columns]
    df_out.to_csv(buffer, header=True, index=False, encoding="utf-8")

def validate_csv(csv_content):
    """
    Asegura que el contenido del CSV tenga las cabeceras, columnas correctas y no contenga valores vacíos.
    """
    reader = csv.reader(io.StringIO(csv_content))
    rows = list(reader)
    if not rows:
        raise ValueError("El CSV está vacío")
        
    header = rows[0]
    expected_header = ['store_id', 'start_date', 'end_date', 'description', 'type_format', 'name', 'id']
    if header != expected_header:
        raise ValueError(f"Diferencia en cabecera del CSV. Esperado: {expected_header}, Encontrado: {header}")
        
    for idx, r in enumerate(rows[1:], start=2):
        if len(r) != len(expected_header):
            raise ValueError(f"La línea {idx} no tiene {len(expected_header)} columnas. Columnas: {len(r)}")
        for col_idx, val in enumerate(r):
            if not val or val.strip() == '':
                raise ValueError(f"La línea {idx}, columna '{expected_header}[col_idx]' está vacía")
    return True

def upload_sftp(sftp_connection, local_file_buffer, remote_folder, filename):
    """
    Sube el buffer del archivo al directorio remoto.
    Primero sube con extensión .tmp, luego renombra al nombre de archivo final.
    """
    remote_path = f"{remote_folder}{filename}"
    temp_path = f"{remote_path}.tmp"
    
    local_file_buffer.seek(0)
    sftp_connection.putfo(local_file_buffer, temp_path)
    sftp_connection.rename(temp_path, remote_path)
    print(f"Subido y renombrado: {remote_path}")

def extract_and_process_promotions(ds, ti, **kwargs):
    """
    Tarea 2: Extracción, Validación, Transformación, Generación de CSV y Validación.
    Almacena archivos CSV válidos en S3 para la Tarea 3.
    """
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    exec_date = ds.replace("-", "/")
    
    ts = kwargs.get('ts')
    if ts:
        aux_time = pendulum.parse(ts).strftime("%H%M")
    else:
        aux_time = pendulum.now("America/Santiago").strftime("%H%M")
    
    # Recuperar tiendas activas de la Tarea 1 vía XCom
    rappi_stores = ti.xcom_pull(task_ids="get_rappi_active_stores")
    if not rappi_stores:
        print("No se retornaron tiendas activas de get_rappi_active_stores. Omitiendo ejecución.")
        return
        
    smu_store_ids = [store[0] for store in rappi_stores]
    print(f"Tiendas activas SMU obtenidas: {smu_store_ids}")
    
    print(f"Extrayendo promociones para la fecha lógica: {ds}")
    raw_df = extract_promotions(ds, smu_store_ids)
    print(f"Se extrajeron {len(raw_df)} registros crudos.")
    
    if raw_df.empty:
        print("No se encontraron promociones para la extracción. Omitiendo...")
        return
        
    grouped = raw_df.groupby('store_id')
    
    for store_id, group in grouped:
        print(f"Procesando Tienda ID: {store_id}")
        
        valid_records = []
        
        for idx, row in group.iterrows():
            is_valid, reason = validate_promotion(row)
            if not is_valid:
                print(f"SKU {row.get('id')} omitido: {reason}")
                continue
                
            transformed = transform_promotion(row)
            valid_records.append(transformed)
            
        if not valid_records:
            print(f"No hay registros válidos para la tienda {store_id}. Omitiendo generación de archivo.")
            continue
            
        valid_df = pd.DataFrame(valid_records)
        
        # Validación de duplicados
        initial_len = len(valid_df)
        valid_df.drop_duplicates(subset=['store_id', 'id', 'start_date', 'end_date', 'type_format'], keep='first', inplace=True)
        if len(valid_df) < initial_len:
            print(f"Se eliminaron {initial_len - len(valid_df)} promociones duplicadas.")
            
        csv_buffer = io.StringIO()
        write_csv(valid_df, csv_buffer)
        csv_content = csv_buffer.getvalue()
        
        try:
            validate_csv(csv_content)
            print(f"Validación de CSV exitosa para tienda {store_id}.")
        except Exception as e:
            print(f"Validación de CSV falló para tienda {store_id}: {e}. Omitiendo archivo.")
            csv_buffer.close()
            continue
            
        filename = f"{store_id}_{ds}_{aux_time}.csv"
        s3_key = f"integraciones/last_millers/promotions/out/rappi/Complex/{exec_date}/{filename}"
        s3_hook.load_string(csv_content,
                            key=s3_key,
                            bucket_name=s3_bucket,
                            replace=True,
                            encrypt=False)
        print(f"Archivo cargado en S3: {s3_key}")
        
        csv_buffer.close()

def send_joined_data_to_stfp(ds, ti, **kwargs):
    """
    Tarea 3: Lee los CSV generados desde S3, los sube a Rappi SFTP usando .tmp y renombrado,
    y registra resultados y limpia buffers de memoria.
    """
    ftp_host = Variable.get("SFTP_RAPPI_HOST")
    ftp_port = 22
    ftp_user = Variable.get("SFTP_RAPPI_USER")
    ftp_rsa_key = Variable.get("SFTP_RAPPI_PASSWORD")
    
    exec_date = ds.replace("-", "/")
    prefix = f"integraciones/last_millers/promotions/out/rappi/Complex/{exec_date}/"
    
    s3_bucket = Variable.get("AWS_S3_BUCKET_NAME")
    s3_hook = S3Hook(aws_conn_id="aws_s3_connection")
    
    s3_file_list = s3_hook.list_keys(s3_bucket, prefix=prefix)
    if not s3_file_list:
        print("No se encontraron archivos en S3 para subir al SFTP.")
        return
        
    print(f"Cantidad de archivos encontrados: {len(s3_file_list)}")
    
    remote_folder = Variable.get("SFTP_RAPPI_COMPLEX_FOLDER", "/discounts")
    if remote_folder and not remote_folder.endswith("/"):
        remote_folder += "/"
        
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    
    with pysftp.Connection(host=ftp_host,
                            username=ftp_user,
                            port=ftp_port,
                            password=ftp_rsa_key,
                            cnopts=cnopts) as sftp:
                            
        try:
            sftp.makedirs(remote_folder)
        except Exception as e:
            print(f"Error al chequear o crear directorio remoto: {e}")
            
        for s3_key in s3_file_list:
            filename = s3_key.split("/")[-1]
            print(f"Procesando subida SFTP para el archivo: {filename}")
            
            s3_obj = s3_hook.get_key(s3_key, bucket_name=s3_bucket)
            csv_content = s3_obj.get()["Body"].read().decode('utf-8')
            
            local_file_buffer = io.BytesIO(csv_content.encode('utf-8'))
            try:
                upload_sftp(sftp, local_file_buffer, remote_folder, filename)
                print(f"Subido y registrado exitosamente: {filename}")
            except Exception as e:
                print(f"Error al subir {filename}: {e}")
                local_file_buffer.close()
                raise e
            finally:
                local_file_buffer.close()
            
    print("Todas las subidas fueron completadas exitosamente.")

default_args = {
    "owner": "ecommerce_ops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "proc_rappi_promotion_complex",
    default_args=default_args,
    description="Cruce de stock, precios y precios promocionales complejas para integracion Rappi",
    schedule_interval=None, 
    start_date=pendulum.datetime(2023, 2, 21, tz="America/Santiago"),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
    tags=["OPS", "last_millers", "dw", "stock", "precios", "RODRIGO","PROMOTIONS", "RAPPI", "COMPLEX"],
    on_success_callback=dag_success_slack,
    on_failure_callback=dag_failure_slack,
) as dag:

    dag.doc_md = """
        DAG creado para la generación y envío de promociones complejas a Rappi por SFTP.
    """ 

    t0 = PythonOperator(
        task_id = "get_rappi_active_stores",
        python_callable = get_rappi_active_stores
    )

    t1 = PythonOperator(
        task_id = "extract_and_process_promotions",
        python_callable = extract_and_process_promotions
    )

    #t2 = PythonOperator(
    #    task_id = "send_joined_data_to_stfp",
    #    python_callable = send_joined_data_to_stfp
    #)

    t0 >> t1 #>> t2
