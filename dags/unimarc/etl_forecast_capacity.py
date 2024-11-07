from airflow import DAG
from airflow import macros
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from prophet import Prophet

import pendulum

from datetime import datetime, timedelta

def query_to_df(query):
    import pandas as pd
    print(query)
    pg_hook = PostgresHook(postgres_conn_id="postgresql_conn_prod")
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

def es_dia_paga(fecha):
    import calendar
    # Obtener el último día hábil del mes
    ultimo_dia_habil = fecha.replace(day=calendar.monthrange(fecha.year, fecha.month)[1])
    while ultimo_dia_habil.weekday() >= 5:  # Si es sábado o domingo, retroceder al viernes
        ultimo_dia_habil -= timedelta(days=1)
    
    # Obtener el primer fin de semana del mes
    primer_dia_mes = fecha.replace(day=1)
    primer_fin_de_semana = primer_dia_mes + timedelta(days=(5 - primer_dia_mes.weekday()) % 7)

    # Verificar si la fecha coincide con el último día hábil o el primer fin de semana
    return fecha == ultimo_dia_habil or fecha == primer_fin_de_semana

def carga_capacidades_to_s3(ds):
    import pandas as pd
    from prophet import Prophet

    def simplificar_nombre_promocion(nombre):
        import re
        # Eliminar rangos de fechas (ej: "17,08 A 23.08" o "22.02 A 25.02")
        nombre_simplificado = re.sub(r'\d{2},\d{2} (AL|A) \d{2}.\d{2}', '', nombre)

        # Eliminar números al final de las frases que estén separados por comas o puntos (ej: "DIA DEL GIN TONIC 19,10" -> "DIA DEL GIN TONIC")
        nombre_simplificado = re.sub(r'\s\d{2}[,.]\d{2}', '', nombre_simplificado)
        
        # Eliminar palabras o frases específicas: "ESP", "3X2", "SEM 22"
        nombre_simplificado = re.sub(r'ESP(,)?|3X2|SEM \d{2}', '', nombre_simplificado)
        
        # Eliminar números después de "ASAITO" (ej: "ASAITO FUTBOL IV")
        nombre_simplificado = re.sub(r'ASAITO [A-Z]+ \d+', 'ASAITO', nombre_simplificado)
        
        # Mantener "PUNTAS DE PRECIO" y solo eliminar el número al final (ej: "PUNTAS DE PRECIO 9" -> "PUNTAS DE PRECIO")
        nombre_simplificado = re.sub(r'PUNTAS DE PRECIO \d+', 'PUNTAS DE PRECIO', nombre_simplificado)
        
        # Eliminar las demás frases previas: "APO", "APOTEOSICO", "VTA", "UNI", "NF", "VENTA ESP", "ESPECIAL"
        nombre_simplificado = re.sub(r'APO(TEOSICO)?|VTA(,? ES HALLOWEN 25%)?|VTA ESP(ACIALES)?|UNIMARC|UNI|NF|VENTA ESP(ECIAL)?|VENTA ASISTIDA|ESPECIAL', '', nombre_simplificado)

        # Casos especiales
        nombre_simplificado = re.sub(r'VENTA ECIAL|ECIAL|14,06|1 LT|S ECIALES|TOP 25|20.08 A 20.08|18.10 A 20.10|VENTA|ES, 22.02 A 25.02|1,25 20.04 A 15.05', '', nombre_simplificado)

        # Mantener "hallowen")
        nombre_simplificado = re.sub(r',  HALLOWEEN 25%  01.10 A 30.11', 'HALLOWEEN', nombre_simplificado)

        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO FUTBOL IV + LECHE', 'ASAITO', nombre_simplificado)
        
        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO FUTBOL III + DES', 'ASAITO', nombre_simplificado)
        
        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO FUTBOL II', 'ASAITO', nombre_simplificado)
        
        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO FUTBOL I + LECHE', 'ASAITO', nombre_simplificado)
        
        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO DIA DEL PADRE', 'ASAITO', nombre_simplificado)
        
        # Mantener "asaito")
        nombre_simplificado = re.sub(r'ASAITO (18 CHICO)', 'ASAITO', nombre_simplificado)

        # Filtrar nombres de Banco Estado y unificarlos
        nombre_simplificado = re.sub(r'BANCO ESTADO( [\dA-Z/]+)?( [A-Z]+)?', 'BANCO ESTADO', nombre_simplificado)
        
        # Eliminar espacios en exceso
        nombre_simplificado = re.sub(r'\s+', ' ', nombre_simplificado).strip()

        # Eliminar frases exclusivas (ej: "EXCLUSIVO", "EX U,CL", etc.)
        nombre_simplificado = re.sub(r'EXCLUSIVO( U,CL)?|EX U,CL|ECOMMERCE|SOLO X HOY|EXCLUSIVA', '', nombre_simplificado)

        # Eliminar cualquier número seguido de un porcentaje (ej: "20%", "50/70/80%")
        nombre_simplificado = re.sub(r'\d+([/]\d+)*%', '', nombre_simplificado)

        # Quedarse solo con "CYBER" y eliminar el resto en frases como "EXC U,CL CYBERAZO SXH 02.10"
        nombre_simplificado = re.sub(r'EXC( U,CL)? CYBER[A-Z]* .*', 'CYBER', nombre_simplificado)
        
        # Eliminar espacios en exceso
        nombre_simplificado = nombre_simplificado.strip()
    
        return nombre_simplificado

    query_promos = """
                SELECT DISTINCT
            wp.n_promocion,
            wp.nombre_promocion,
            wp.descripcion_evento_promocional,
            wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion
        FROM
            ecommdata.workflow_promociones wp
        where wp.descripcion_evento_promocional IN ('UNI APOTEOSICO', 'UNI VENTA ESPECIAL')--,'ESPECIAL UNIMARC,CL'
            AND wp.fecha_inicio_de_promocion >= '2023-01-01'
            and wp.tipo_promocion IN (1,4)
            and wp.registro_valido = True
            and wp.organizacion_ventas = '1000'
            and wp.canal_distribucion in ('10','70')
            and wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123,124)
            and wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
            AND wp.nombre_promocion::text !~~ '%ZONA%'::text
            AND wp.nombre_promocion::text !~~ '%MFC%'::text
            AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
            AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
            AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
            AND wp.nombre_promocion::text !~~ '%917%'::text
            AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
            and wp.nombre_promocion::text !~~ '% LOC%'::text
            and wp.nombre_promocion::text !~~ '%LIQ%'::text
            and wp.n_promocion  not in  ('5640502024','5552392024','1120012024',
                    '1120022024',
                    '1120032024',
                    '1120042024',
                    '1120052024',
                    '1120062024',
                    '1120082024',
                    '1120092024',
                    '1120102024',
                    '1120112024',
                    '1120122024',
                    '4000512024','5552792024','5552852024'
                    ,'4000662024','4000942024','4000962024','4000972024','4000952024')
        union
        SELECT DISTINCT
            wp.n_promocion,
            wp.nombre_promocion,
            wp.descripcion_evento_promocional,
            wp.fecha_inicio_de_promocion,
            wp.fecha_fin_de_promocion
        FROM
            ecommdata.workflow_promociones wp
        where wp.descripcion_evento_promocional IN ('ESPECIAL UNIMARC,CL')
            AND wp.fecha_inicio_de_promocion >= '2023-01-01'
            and wp.tipo_promocion IN (1,4)
            and wp.registro_valido = True
            and wp.organizacion_ventas = '1000'
            and wp.canal_distribucion in ('10','70')
            and wp.id_mecanica NOT IN (25, 27, 36, 37, 50, 51, 53, 67, 72, 77, 93, 99, 123,124)
            and wp.nombre_promocion::text !~ 'L(0[0-9]{2}|[1-9][0-9]{0,2})'
            AND wp.nombre_promocion::text !~~ '%ZONA%'::text
            AND wp.nombre_promocion::text !~~ '%MFC%'::text
            AND wp.nombre_promocion::text !~~ '%BANCO%'::text 
            AND wp.nombre_promocion::text !~~ '%UNIPAY%'::text
            AND wp.nombre_promocion::text !~~ '%TERCERA%'::text 
            AND wp.nombre_promocion::text !~~ '%917%'::text
            AND wp.nombre_promocion::text !~~ '%ESTADO%'::text
            and wp.nombre_promocion::text !~~ '% LOC%'::text
            and wp.nombre_promocion::text !~~ '%LIQ%'::text
            and wp.n_promocion  not in  ('5640502024','5552392024','1120012024',
                    '1120022024',
                    '1120032024',
                    '1120042024',
                    '1120052024',
                    '1120062024',
                    '1120082024',
                    '1120092024',
                    '1120102024',
                    '1120112024',
                    '1120122024',
                    '4000512024','5552792024','5552852024'
                    ,'4000662024','4000942024','4000962024','4000972024','4000952024')
            and wp.nombre_promocion like '%CYBER%'"""
    df_promos = query_to_df(query_promos)

    # Aplicar la función a la columna 'nombre_promocion' para generar más traducciones automáticas
    df_promos['nombre_promocion_corta_auto'] = df_promos['nombre_promocion'].apply(simplificar_nombre_promocion)

    # Crear un diccionario inicial basado en las promociones similares
    promociones_dict = {
        'REDFRIDAY': 'RED FRIDAY',
        'RED FRIDAY': 'RED FRIDAY',
        'RF': 'RED FRIDAY',
        'LIMPIAZO': 'LIMPIAZO',
        'ASADITO': 'ASAITO',
        'ASAITO (18 CHICO)': 'ASAITO',
        'ASAITOI + DES': 'ASAITO',
        'ASAITO': 'ASAITO',
        'ASAITO FUTBOL I': 'ASAITO',
        'ASAITO FUTBOL IV + LECHE': 'ASAITO',
        'LECHERAZO': 'LECHERAZO',
        'GUAGUAZO+LECHERAZO': 'LECHERAZO',
        'LECHERAZO + GUAGUAZO': 'LECHERAZO',
        'DEN+CATEG LIMPIAZO': 'LIMPIAZO',
        'DENSAZO+LIMPIAZO': 'LIMPIAZO',
        'FFPP - CHELAZO': 'CHELAZO',
        'GRAN PASCUERAZO': 'PASCUERAZO',
        'CHELAZO': 'CHELAZO',
        'PUNTA DE PRECIO': 'PUNTAS DE PRECIO',
        'PUNTA DE PRECIOS': 'PUNTAS DE PRECIO',
        'PUNTA PRECIO': 'PUNTAS DE PRECIO',
        'PUNTAS DE PRECIO': 'PUNTAS DE PRECIO',
        'FINDE AL ROJO': 'AL ROJO',
        'FANATICOS DEL FINDE F': 'FANATICOS DEL FINDE',
        'FANATICOS DEL FINDE': 'FANATICOS DEL FINDE',
        'LECH+GUAGUAZO': 'LECHERAZO',
        'GUAGUAZO + LECHERAZO': 'LECHERAZO',
        'CYBER WEEK CARNES': 'CYBER',
        'BEB ENER MOSTER': 'BEBIDAS ENERGETICAS MONSTER',
        'PESCADER PREPARADA': 'PESCADERIA',
        'ACEITE MERKAT': 'ACEITE',
        'QUESO CHANCO': 'QUESO',
        'QUESO FRESCO': 'QUESO',
        'QUESO GOUDA LAM': 'QUESO',
        'QUESO HUILCO': 'QUESO',
        'QUESO SOPROLE': 'QUESO',
        'COCA COLA': 'BEBIDAS COCA COLA',
        'PACK COCA COLA': 'BEBIDAS COCA COLA',
        'FFPP (SAP)': 'FONDAZO',
        'NAVIDAD': 'NAVIDAD',
        'COSTANUSS': 'COSTANUSS',
        'DIA DEL QUESO': 'QUESO',
        'EMPANADAS': 'EMPANADAS',
        '14 FEBRERO': 'DIA ENAMORADOS',
        '48 HRS ALIM BASICOS 03-02 AL 04-02': 'ELIMINAR',
        'CAMPANA  LOS LEONES 03,10': 'ELIMINAR',
        'FFPP': 'FONDAZO',
        'FFPP - FONDAZO': 'FONDAZO',
        'PERECIBLES S/CARNES Y FFVV': 'PERECIBLE',
        'YOGUR': 'YOGHURT',
        'LACTEOS 0743': 'ELIMINAR',
        'PTA PRECIOS': 'PUNTAS DE PRECIO',
        'DD COMPLETO': 'DIA DEL COMPLETO',
        'DENSAZO': 'DESPENSAZO',
        'DENZASO': 'DESPENSAZO',
        'DDM': 'DIA DE LA MADRE',
        'GALLETA D MADRE': 'DIA DE LA MADRE',
        'FIESTA FDA SMU 469 13,12.2023': 'ELIMINAR',
        'FDA - FIN DE ANO': 'FIN DE ANO',
        'FDA-PASCUERAZO': 'FIN DE ANO',
        'FDS LIMPIEZA': 'ELIMINAR',
        'FDA-PASCUERAZO': 'FIN DE ANO',
        'BTS': 'BACK TO SCHOOL',
        'LEVER': 'UNILEVER',
        'PACK UMANTE + HEL PINA DIC':'ESPUMANTE',
        'ACEITE VGT LOCALES': 'ELIMINAR',
        'ACUARIO L469': 'ELIMINAR',
        'AL ROJO FESTIVA': 'AL ROJO',
        'CYBERAZO, 29.05 A 31.05': 'CYBER',
        'CYBERAZO ,01.06 A 04.06': 'CYBER',
        'CYBERAZO': 'CYBER',
        'CYBERAZO SOLOXHOY': 'CYBER',
        'AMADA MASA L895': 'ELIMINAR',
        '04.05 A 15.05': 'ELIMINAR', 
        '08.08 A 21.08': 'ELIMINAR', 
        '11.07 A 24.07': 'ELIMINAR',
        '13.06 A 26.06': 'ELIMINAR', 
        '16.05 A 02.06': 'ELIMINAR', 
        '22.08 A 04.09': 'ELIMINAR', 
        '25.07 A 07.08': 'ELIMINAR',
        '27.06 A 10.07': 'ELIMINAR',
        '29.05 A 31.05': 'ELIMINAR', 
        '30.05 A 12.06': 'ELIMINAR',
        '48 HRS 10-02 AL 11-02': 'ELIMINAR', 
        '48 HRS ASEO 09-02 A 10-02': 'ELIMINAR',
        '48 HRS ONCE 05-02 AL 06-02': 'ELIMINAR', 
        '48 HRS PERFUMERIA 11-02 A 12-02': 'ELIMINAR',
        'CAMPANA LOS LEONES 03,10':'ELIMINAR',
        'DIA DEL GIN TONIC': 'DIA DEL GIN',
        'APERTURA 895 PGC': 'ELIMINAR',
        'A': 'ELIMINAR',
        'COITES NAVIDAD': 'NAVIDAD',
        'CYBER CARNES':'CYBER',
        'CYBERAZO': 'CYBER',
        'BTS LIBRERIA': 'BACK TO SCHOOL',
        'AL ROJO FESTIVAL': 'AL ROJO',
        'CYBERAZO ,01.06 A' : 'CYBER',
        'CYBERAZO' : 'CYBER',
        'CYBER CARNES': 'CYBER',
        'ASIST COSTANUSS': 'COSTANUSS',
        'HALLOWEEN  A': 'HALLOWEEN',
        'FANATICOS DEL FINDE FES': 'FANATICOS DEL FINDE'
    }

    # Aplicar el diccionario para estandarizar las promociones
    df_promos['nombre_promocion_corta_auto'] = df_promos['nombre_promocion_corta_auto'].replace(promociones_dict)

    promociones_dict_2 = {
        'HALLOWEEN  A': 'HALLOWEEN',
        'FANATICOS DEL FINDE FES': 'FANATICOS DEL FINDE',
        'LIBRERIA S06': 'LIBRERIA',
        'LIBRERIA S07': 'LIBRERIA',
        'LIBRERIA S08': 'LIBRERIA', 
        'LIBRERIA S09': 'LIBRERIA', 
        'LIBRERIA S10': 'LIBRERIA',
        'LIBRERIA S11': 'LIBRERIA',
        'LECHES LOS PEUMOS': 'LECHE LOS PEUMOS',
        'LOS FRESQUITOS DE LA SEM': 'FRESQUITOS',
        'PROF FRES DE LA SEM CECINA': 'FRESQUITOS',
        'PROF FRES DE LA SEM QUESOS': 'FRESQUITOS',
    }

    # Aplicar el segundo diccionario con expresiones regulares
    df_promos['nombre_promocion_corta_auto'] = df_promos['nombre_promocion_corta_auto'].replace(promociones_dict_2, regex=True)

    # Crear un rango de fechas desde el 1 de enero de 2023 hasta el 31 de diciembre de 2024
    fecha_inicio = datetime(2023, 1, 1)
    fecha_fin = datetime(2024, 12, 31)
    fechas = pd.date_range(start=fecha_inicio, end=fecha_fin)

    # Crear DataFrame con las fechas
    df_calendario = pd.DataFrame({'fecha': fechas})

    # Agregar columna de días de la semana (lunes a domingo)
    #df_calendario['dia_semana'] = df_calendario['fecha'].dt.day_name()

    Feriados = {
        "fecha": [
            "23-01-01", "23-01-02", "23-04-07", "23-04-08", "23-05-01",
            "23-05-21", "23-06-26", "23-06-29", "23-07-16", "23-08-15",
            "23-09-18", "23-09-19", "23-10-09", "23-10-27", "23-11-01",
            "23-12-08", "23-12-25", "24-01-01", "24-03-29", "24-03-30",
            "24-05-01", "24-05-21", "24-06-07", "24-06-16", "24-06-24",
            "24-06-29", "24-08-15", "24-09-18", "24-09-19", "24-10-12",
            "24-11-01", "24-11-24", "24-12-08", "24-12-25"
        ],
        "irrenunciable": [
            1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 1
        ],
        "religioso": [
            0, 0, 1, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0, 1, 1,
            0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1
        ]
    }

    
    feriados = pd.DataFrame(Feriados)

    #cambiar tipo de dato
    df_calendario['fecha'] = pd.to_datetime(df_calendario['fecha'])
    feriados['fecha'] = pd.to_datetime(feriados['fecha'])

    df_calendario = df_calendario.merge(feriados, how="left", on = ["fecha"])
    df_calendario = df_calendario.fillna(0)

    #agregar pay_day
    df_calendario['es_dia_paga'] = df_calendario['fecha'].apply(es_dia_paga)

    query_ventas = """
        select t.id ,d1.id_transportadora, oj.fecha_creacion::date, 
        count(distinct oj.id) as cant_ordenes, sum(venta_creada_neta) as venta_creada_neta
        from 
            ecommdata.ordenes_janis oj 
        left join 
            (select id_orden, id_transportadora, max(fecha_despacho::date) 
                    from ecommdata.despachos d group by 1,2) d1 
                on d1.id_orden = oj.id
        left join ecommdata.tiendas t on t.id_janis  = oj.id_tienda_janis 
        where oj.fecha_creacion::date >= '2023-01-01'
        group by  t.id ,d1.id_transportadora, oj.fecha_creacion::date"""
    
    df_venta_transportadora = query_to_df(query_ventas)
    
    # Primero, aseguramos que la columna 'fecha_creacion' en las ventas esté en formato datetime
    df_venta_transportadora['fecha_creacion'] = pd.to_datetime(df_venta_transportadora['fecha_creacion'])

    # 1. Unimos el calendario con las ventas usando la fecha
    df_ventas_con_calendario = pd.merge(
        df_venta_transportadora,
        df_calendario,
        how='left',
        left_on='fecha_creacion',
        right_on='fecha'
    )

    # 2. Asociamos las promociones activas a las ventas
    # Convertimos las columnas de fechas en promociones a datetime
    df_promos['fecha_inicio_de_promocion'] = pd.to_datetime(df_promos['fecha_inicio_de_promocion'])
    df_promos['fecha_fin_de_promocion'] = pd.to_datetime(df_promos['fecha_fin_de_promocion'])

    # Ahora necesitamos una función para verificar si la fecha de venta está dentro del rango de las promociones
    def obtener_promociones_activas(fecha):
        promociones_activas = df_promos[
            (df_promos['fecha_inicio_de_promocion'] <= fecha) & 
            (df_promos['fecha_fin_de_promocion'] >= fecha)
        ]['nombre_promocion_corta_auto'].tolist()
        
        # Si hay más de una promoción, las unimos en un solo string
        return ', '.join(promociones_activas) if promociones_activas else 'Sin promoción'
    
    # Aplicamos la función para obtener las promociones activas por cada fecha de venta
    df_ventas_con_calendario['promociones_activas'] = df_ventas_con_calendario['fecha_creacion'].apply(obtener_promociones_activas)

    # Hacemos el One-Hot Encoding de la columna de promociones activas
    # Primero, necesitamos separar las diferentes promociones dentro de la columna 'promociones_activas'
    df_ventas_con_calendario['promociones_lista'] = df_ventas_con_calendario['promociones_activas'].str.split(', ')

    # Creamos columnas binarias para cada promoción con get_dummies
    promociones_encoded = df_ventas_con_calendario['promociones_lista'].str.join('|').str.get_dummies()

    # Unimos las columnas codificadas binariamente con el dataframe original
    df_ventas_con_calendario_encoded = pd.concat([df_ventas_con_calendario, promociones_encoded], axis=1)

    # Eliminamos las filas donde las ventas y órdenes no tienen registros (NaN originalmente)
    df_ventas_con_calendario_encoded = df_ventas_con_calendario_encoded.dropna(subset=['cant_ordenes', 'venta_creada_neta'])

    # Eliminamos las filas donde id_transportadora no tienen registros (NaN originalmente)
    df_ventas_con_calendario_encoded = df_ventas_con_calendario_encoded.dropna(subset=['id_transportadora'])

    # Definir las columnas que quieres eliminar
    columnas_a_eliminar = ['ELIMINAR','id_transportadora','04.05 A 15.05', '08.08 A 21.08', '11.07 A 24.07', '13.06 A 26.06',
       '16.05 A 02.06', '22.08 A 04.09', '25.07 A 07.08', '27.06 A 10.07', 'A','CAMPANA LOS LEONES','E','EXHIBICIONES HUACHALALUME',
       '30.05 A 12.06','FFPP NON FOOD A', 'FIESTA FDA SMU 469.2023','IN&OUT NON FOOD', 'IN&OUT NON FOOD 1',
       'INSERTO PERECIBLES', 'INV24 NON FOOD 10 ', 'INV24 NON FOOD 11 ',
       'INV24 NON FOOD 12 ', 'INV24 NON FOOD 13 ', 'INV24 NON FOOD 14 ',
       'INV24 NON FOOD 15 ', 'INV24 NON FOOD 16 ', 'INV24 NON FOOD 17 ',
       'INV24 NON FOOD 18 ','NON FOOD', 'NON FOOD 10', 'NON FOOD 11', 'NON FOOD 12',
       'NON FOOD 13', 'NON FOOD 14', 'NON FOOD 15', 'NON FOOD 16',
       'NON FOOD 17', 'NON FOOD 18', 'NON FOOD 19', 'NON FOOD 2',
       'NON FOOD 20', 'NON FOOD 21', 'NON FOOD 22', 'NON FOOD 23',
       'NON FOOD 3', 'NON FOOD 4', 'NON FOOD 5', 'NON FOOD 6', 'NON FOOD 7',
       'NON FOOD 8', 'NON FOOD 9', 'NON FOOD SEM 1', 'NON FOOD SEM 2',
       'NON FOOD SEM 3', 'NON FOOD SEM 4', 'NON FOOD SEM 5', 'NON FOOD SEM 6','VERANO NON FOOD',
       'NON FOOD SEM 7', 'NON FOOD SEM 8', 'NON FOOD SEM 9','PRODUCTOS INOUT GALLETAS','TOP26 PERECIBLE', 'TOP26 PGC']

    # Eliminar las columnas del DataFrame `df_futuro_calendario_encoded`
    df_ventas_con_calendario_encoded.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')
    print("\ninfo de df_ventas_con_calendario_encoded")
    df_ventas_con_calendario_encoded.info()

    #todo esto fue preparacion de datos :)
    lista_tiendas = df_ventas_con_calendario_encoded['id'].unique()

    print(lista_tiendas)

    ################################
    ##### Preparamos DF futuro #####
    ################################

    fecha_filtro = pd.to_datetime(ds, format='%Y-%m-%d')
    fecha_limite = fecha_filtro + timedelta(days=12)
    df_futuro = df_calendario[(df_calendario['fecha'] >= fecha_filtro) & (df_calendario['fecha'] <= fecha_limite)]

    print("\ndf_futuro primeras 20 filas")
    print(df_futuro.head(20))

    # Obtener las promociones activas para las fechas futuras
    df_futuro['promociones_activas'] = df_futuro['fecha'].apply(obtener_promociones_activas)

    # Crear una lista de promociones activas
    df_futuro['promociones_lista'] = df_futuro['promociones_activas'].str.split(', ')

    # Realizar One-Hot Encoding de las promociones activas
    promociones_encoded_futuro = df_futuro['promociones_lista'].str.join('|').str.get_dummies()

    # Unir las promociones codificadas con el DataFrame de fechas futuras
    df_futuro_enconded = pd.concat([df_futuro, promociones_encoded_futuro], axis=1)

    # Renombrar la columna 'fecha' a 'ds' para que coincida con Prophet
    df_futuro_enconded.rename(columns={'fecha': 'ds'}, inplace=True)

    # Binarizar las promociones activas, si está activa 1, si no, 0
    for col in promociones_encoded_futuro.columns:
        df_futuro_enconded[col] = df_futuro_enconded[col].apply(lambda x: 1 if x > 0 else 0)

    # Filtrar las columnas útiles (incluyendo 'ds', 'pay_day', 'holiday' y las promociones)
    columnas_utiles = ['ds'] + list(promociones_encoded_futuro.columns)
    df_futuro_enconded = df_futuro_enconded[columnas_utiles]

    df_futuro_enconded.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')

    print(f"veamos las primeras 21 columnas aers si está funcionando: {df_futuro_enconded.columns[0:20]}")
    print(f"veamos los primeros registros: {df_futuro_enconded.tail(20)}")


    for tienda in lista_tiendas:

        df_aux = df_ventas_con_calendario_encoded[df_ventas_con_calendario_encoded["id"] == tienda].copy()
        # Crear una nueva columna 'pay_day' que binarice la columna 'es_dia_paga'
        df_aux['pay_day'] = df_aux['es_dia_paga'].apply(lambda x: 1 if x else 0)

        # Definir las columnas que quieres eliminar
        columnas_a_eliminar = ['es_dia_paga','fecha','promociones_activas', 'promociones_lista']

        # Eliminar las columnas del DataFrame
        df_aux.drop(columns=columnas_a_eliminar, inplace=True, errors='ignore')

        # Identificar las columnas desde la columna "S" en adelante
        columnas_desde_S = df_aux.columns[6:]

        # Agrupar por id y fecha, sumando las órdenes y conservando las columnas desde S
        df_grouped = df_aux.groupby(['id', 'fecha_creacion'], as_index=False).agg({
            'cant_ordenes': 'sum',
            **{col: 'first' for col in columnas_desde_S}
        })
        df_grouped.drop(columns=['id'], inplace=True, errors='ignore')
        df_grouped.rename(columns={'fecha_creacion': 'ds', 'cant_ordenes': 'y'}, inplace=True)

        # Verificar los regresores que están en los datos históricos (df_grouped)
        columnas_regresores_historicos = df_grouped.columns.tolist()

        # Verificar los regresores que están en los datos futuros
        columnas_regresores_futuros = df_futuro_enconded.columns.tolist()

        # Encontrar los regresores faltantes en los datos futuros
        regresores_faltantes = [col for col in columnas_regresores_historicos if col not in columnas_regresores_futuros and col not in ['ds', 'y']]

        # Añadir los regresores faltantes a df_futuro_calendario_encoded y llenarlos con ceros
        for col in regresores_faltantes:
            df_futuro_enconded[col] = 0

        df_grouped.fillna(0, inplace=True)
        df_futuro_calendario_encoded.fillna(0, inplace=True)

        # Obtener las columnas de regresores del df_grouped y df_futuro_calendario_encoded
        columnas_grouped = set(df_grouped.columns)
        columnas_futuro = set(df_futuro_calendario_encoded.columns)

        # Encontrar las columnas que faltan en cada uno
        columnas_faltantes_en_grouped = columnas_futuro - columnas_grouped
        print(f"faltan estas columnas en df gruped {columnas_faltantes_en_grouped}")
        columnas_faltantes_en_futuro = columnas_grouped - columnas_futuro
        print(f"faltan estas columnas en df futuro {columnas_faltantes_en_futuro}")

        # Añadir las columnas faltantes en df_grouped y rellenarlas con ceros
        for col in columnas_faltantes_en_grouped:
            df_grouped[col] = 0

        # Añadir las columnas faltantes en df_futuro_calendario_encoded y rellenarlas con ceros
        for col in columnas_faltantes_en_futuro:
            df_futuro_calendario_encoded[col] = 0

        # Asegurarnos de que ambas tablas tengan las mismas columnas y en el mismo orden
        df_futuro_calendario_encoded = df_futuro_calendario_encoded[df_grouped.columns]

        # Crear el modelo Prophet
        modelo_prophet = Prophet()

        # Añadir los regresores (pay_day, holiday, y promociones) al modelo
        for col in df_grouped.columns:
            if col not in ['ds', 'y']:  # Excluir las columnas 'ds' y 'y'
                modelo_prophet.add_regressor(col)

        # Entrenar el modelo con los datos históricos (df_grouped)
        modelo_prophet.fit(df_grouped)

        # Concatenar df_grouped con las fechas futuras (df_futuro_calendario_encoded)
        future = pd.concat([df_grouped, df_futuro_calendario_encoded], ignore_index=True)

        # Realizar la predicción con Prophet
        forecast = modelo_prophet.predict(future)

        # Mostrar las predicciones
        print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])
        forecast_7_days = forecast[['ds', 'yhat']].tail(7)
        print(f"Pronosticos sgtes dias para las tienda:{tienda} {forecast_7_days}")



    print("\ntodo listo")

    return

def carga_capacidades_to_postgres():
    print("todo bien de momento")
    return



default_args = {
    "owner": "ecommerce_data",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_forecast_capacity',
    default_args=default_args,
    description="cargar tabla capacity",
    schedule_interval="20 8 * * *",
    start_date=pendulum.datetime(2024, 10, 1, tz="America/Santiago"),
    catchup=False,
    tags=["DATA", "ordenes", "forecast_and_plannig", "unimarc", "PATRICIO"],
) as dag:
    

    dag.doc_md = """
    Carga tabla capacidades de forecast and planning\n
    guardar en S3.
    """ 
    t0 = PythonOperator(
        task_id='carga_capacidades_to_s3',
        python_callable=carga_capacidades_to_s3,
    )

    t1 = PythonOperator(
        task_id = "carga_capacidades_to_postgres",
        python_callable = carga_capacidades_to_postgres
    )

    t0 >> t1