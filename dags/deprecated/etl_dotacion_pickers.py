from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum


def credenciales():
    import gspread
    import json

    diccionario_keys = Variable.get('KEYS_GOOGLE_SHEET',deserialize_json=True)
    with open('temp_keys.json', 'w', encoding='utf-8') as f:
        json.dump(diccionario_keys, f, ensure_ascii=False)
    
    keys = 'temp_keys.json'
    return (keys)

def fecha_ejecucion(ts):
    from datetime import timedelta, datetime
    import pytz

    today = ((datetime.strptime(ts[:19], '%Y-%m-%dT%H:%M:%S')) + timedelta(hours=1))
    localtimezone = pytz.timezone("America/Santiago")
    today = today.replace(tzinfo = pytz.utc).astimezone(localtimezone)
    today = today.strftime('%Y-%m-%dT%H:%M:%S')
    print (today)
    return (today)



#################### INGESTA GSHEETS ######################
# FUNCIONES
# funcion para buscar ls indices que contienen algun valor
def busca_valor(a,valor):
    a = a.reset_index()
    for row in range(len(a)-1):
        for col in range(len(a.columns)-1):
            if a.iloc[row][col] == valor:
                cell = [row,col]
    return cell
# funcion fecha
def fecha_y_m_d(x):
    from datetime import timedelta, datetime
    import pandas as pd 
    try:
        str(x)
        if str(x)[0:3]=='202':
            fecha = pd.to_datetime(str(x).replace("/","-").split(' ')[0], format='%Y-%m-%d').strftime('%Y-%m-%d')
        elif str(x)[2]=='-' or str(x)[1]=='-':
            fecha = pd.to_datetime(str(x).replace("/","-").split(' ')[0], format='%d-%m-%Y').strftime('%Y-%m-%d')
        else:
            fecha = pd.to_datetime(str(x).replace("/","-").split(' ')[0]).strftime('%Y-%m-%d')
    except:
        fecha = ""
    return fecha
# funcion cod_tienda
def cod_tienda(tienda):
    cod = tienda.split(' ')[0].lstrip('0')
    if len(cod.split('-')[0])<4:
        cod = '0'*(4-len(cod.split('-')[0]))+cod
    return cod

def gsheets_to_sql(keys,today):
    import gspread
    import pandas as pd 
    import sqlalchemy
    from sqlalchemy import text
    from google.oauth2 import service_account

    gc = gspread.service_account(filename='temp_keys.json')
    # parámetros de palabra clave:
    keyword1 = "Dotacíon  Forecast"
    keyword2 = "Dotacíon Diaria Operador"
    keyword3 = "Ordenes  Forecast"
    ## conección a GSheets
    # otros parámetros de conexión 
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    # Log prueba de conexión
    try:
        #service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service_account.Credentials.from_service_account_file('temp_keys.json', scopes=SCOPES)
        print("conectado a Gsheet...")
    except Exception as e:
        print (str(e))
        print("no se pudo establecer conexión con Gsheets")
        raise Exception('No se puede establecer conexion con Google Sheets')
            
    # The ID and range of a sample spreadsheet.
    BOOSMAP_SPREADSHEET_ID = Variable.get('GOOGLE_SHEET_KEY_DOTACION_BOOSMAP') ### boosmap # GOOGLE_SHEET_KEY_DOTACION_BOOSMAP
    TIMEJOBS_SPREADSHEET_ID = Variable.get('GOOGLE_SHEET_KEY_DOTACION_TIMEJOBS') ### timejobs # GOOGLE_SHEET_KEY_DOTACION_TIMEJOBS
    TOUCH_SPREADSHEET_ID = Variable.get('GOOGLE_SHEET_KEY_DOTACION_TOUCH') ### touch # GOOGLE_SHEET_KEY_DOTACION_TOUCH
    # TODO : Credentials check

    ################# Hoja Dotación #####################
    # lee excels de google sheet de dotación
    df_dotacion_gsheets_fr = pd.DataFrame()
    df_dotacion_gsheets_oper = pd.DataFrame()
    for hoja in [BOOSMAP_SPREADSHEET_ID, TIMEJOBS_SPREADSHEET_ID, TOUCH_SPREADSHEET_ID]:
        gsheet_dotacion = gc.open_by_url('https://docs.google.com/spreadsheet/ccc?key='+hoja)
        gsheet_dotacion_values = gsheet_dotacion.worksheet('Resumen Ejecutivo Dotacion').get_all_values()
        df_dotacion_gsheet = pd.DataFrame.from_records(gsheet_dotacion_values).drop_duplicates()
        a = df_dotacion_gsheet
        desde = busca_valor(a,keyword1)
        hasta = busca_valor(a,keyword2)
        operador = str(gsheet_dotacion.title).split(' ')[-1]
        # dataframe de dotacion diaria operador
        a1 = a.reset_index().loc[desde[0]:hasta[0]-1]

        a1.columns = ["","0","Tienda"]+list(a.reset_index().loc[desde[0]+1][3:])
        a1 = a1[~a1['Tienda'].isin(["",keyword1])]
        print(a1.head())
        a1_melt = a1.melt(id_vars=["","0","Tienda"],var_name='fecha',value_name='dotacion_fr')
        a1_melt['fecha'] = a1_melt['fecha'].apply(lambda x: fecha_y_m_d(x))
        a1_melt['cod_tienda'] = a1_melt['Tienda'].apply(lambda x: cod_tienda(x))
        a1_melt = a1_melt[a1_melt['Tienda'].apply(lambda x: 1 if 'unimarc' in str(x).lower() else 0)==1]
        a1_melt['operador'] = operador
        df_dotacion_gsheets_fr = df_dotacion_gsheets_fr.append(a1_melt)
        # dataframe de dotacion diaria operador
        a2 = a.reset_index().loc[hasta[0]+1:]
        a2.columns = ["","0","Tienda"]+list(a.reset_index().loc[desde[0]+1][3:])
        a2 = a2[~a2['Tienda'].isin(["",keyword2])]
        a2_melt = a2.melt(id_vars=["","0","Tienda"],var_name='fecha',value_name='dotacion_operador')
        a2_melt = a2_melt[a2_melt['dotacion_operador']!='']
        a2_melt['fecha'] = a2_melt['fecha'].apply(lambda x: fecha_y_m_d(x))
        a2_melt['cod_tienda'] = a2_melt['Tienda'].apply(lambda x: cod_tienda(x))
        a2_melt = a2_melt[a2_melt['Tienda'].apply(lambda x: 1 if 'unimarc' in str(x).lower() else 0)==1]
        a2_melt['operador'] = operador
        df_dotacion_gsheets_oper = df_dotacion_gsheets_oper.append(a2_melt)

    ################# Hoja Forecast #####################
    # ingesta de datos
    df_forecast_gsheets_ordenes = pd.DataFrame()
    for hoja in [BOOSMAP_SPREADSHEET_ID, TIMEJOBS_SPREADSHEET_ID, TOUCH_SPREADSHEET_ID]:
        gsheet_dotacion = gc.open_by_url('https://docs.google.com/spreadsheet/ccc?key='+hoja)
        gsheet_dotacion_values = gsheet_dotacion.worksheet('Resumen Ejecutivo Forecast').get_all_values()
        df_dotacion_gsheet = pd.DataFrame.from_records(gsheet_dotacion_values).drop_duplicates()
        a = df_dotacion_gsheet
        hasta = busca_valor(a,keyword3)
        operador = str(gsheet_dotacion.title).split(' ')[-1]
        # dataframe de forecast ordenes
        a3 = a.reset_index().loc[hasta[0]:]
        a3.columns = ["","0","Tienda"]+list(a.reset_index().loc[desde[0]][3:])
        a3 = a3[~a3['Tienda'].isin(["",keyword3])]
        a3_melt = a3.melt(id_vars=["","0","Tienda"],var_name='fecha',value_name='ordenes_forecast')
        a3_melt = a3_melt[a3_melt['ordenes_forecast']!='']
        a3_melt['fecha'] = a3_melt['fecha'].apply(lambda x: fecha_y_m_d(x))
        a3_melt['cod_tienda'] = a3_melt['Tienda'].apply(lambda x: cod_tienda(x))
        a3_melt = a3_melt[a3_melt['Tienda'].apply(lambda x: 1 if 'unimarc' in str(x).lower() else 0)==1]
        a3_melt['operador'] = operador
        df_forecast_gsheets_ordenes = df_forecast_gsheets_ordenes.append(a3_melt)

    # dataframe de forecast
    df_forecast = df_dotacion_gsheets_fr[['cod_tienda','Tienda', 'fecha', 'dotacion_fr','operador']].merge(df_forecast_gsheets_ordenes[['cod_tienda','Tienda', 'fecha', 'ordenes_forecast','operador']],
                                                                                                            on=['cod_tienda','Tienda', 'fecha','operador'],
                                                                                                            how='outer')
    
    
    # agrupa df_forecast en las tiendas con apertura camión excepto mirador
    df_forecast['modelo'] = df_forecast['cod_tienda'].apply(lambda x: 'Picker' if '-' in x else 'Shopper')
    df_forecast['cod_tienda'] = df_forecast['cod_tienda'].apply(lambda x: str(x).split('-')[0] if '-' in x else x)
    df_forecast = df_forecast[['cod_tienda', 'Tienda', 'fecha', 'ordenes_forecast', 'operador', 'dotacion_fr', 'modelo']]
    df_forecast.columns = ['id_tienda', 'Tienda', 'fecha', 'ordenes', 'operador', 'dotacion', 'modelo']
    df_forecast = df_forecast[['fecha', 'id_tienda', 'modelo', 'ordenes', 'dotacion','operador']]
    
    df_forecast['fecha_carga'] = today

    print(df_forecast)

    print(df_dotacion_gsheets_oper)
    # dataframe de dotación
    df_dotacion = df_dotacion_gsheets_oper[['Tienda', 'fecha', 'dotacion_operador', 'cod_tienda','operador']]
    # agrupa df_dotacion en las tiendas con apertura camión excepto mirador
    df_dotacion['modelo'] = df_dotacion['cod_tienda'].apply(lambda x: 'Picker' if '-' in x else 'Shopper')
    df_dotacion['cod_tienda'] = df_dotacion['cod_tienda'].apply(lambda x: str(x).split('-')[0] if '-' in x else x)
    
    df_dotacion = df_dotacion[['Tienda', 'fecha', 'dotacion_operador', 'cod_tienda','operador','modelo']]
    df_dotacion.columns = ['Tienda', 'fecha', 'dotacion', 'id_tienda', 'operador','modelo']
    print(df_dotacion)
    df_dotacion = df_dotacion[['fecha', 'id_tienda', 'modelo', 'dotacion','operador']]
    df_dotacion['fecha_carga'] = today
    
    #################### AMAZON ###############################

    df_real = df_dotacion
    df_fr = df_forecast

    
    df_fr = df_fr.fillna(0).replace('',0)
    df_fr['dotacion'] = df_fr['dotacion'].astype(int)
    df_fr['ordenes'] = df_fr['ordenes'].astype(int)
    df_real = df_real.fillna(0).replace('',0)
    print(df_real.loc[df_real['dotacion'] == 'Cambio'])
    df_real['dotacion'] = df_real['dotacion'].astype(int)
    print(df_real)
    print(df_real.columns)
    print(df_real.dtypes)
    print(df_fr)
    print(df_fr.columns)
    print(df_fr.dtypes)


    ################### Corrige los duplicados ####################

    df_real_max = df_real.groupby(['fecha', 'id_tienda', 'modelo'])['dotacion'].max().reset_index()
    df_fr_max = df_fr.groupby(['fecha', 'id_tienda', 'modelo'])['dotacion'].max().reset_index()
    df_real = df_real.merge(df_real_max, on=['fecha', 'id_tienda', 'modelo','dotacion']).drop_duplicates()
    df_fr = df_fr.merge(df_fr_max, on=['fecha', 'id_tienda', 'modelo','dotacion']).drop_duplicates()
    
    ############## CARGA DE DATOS #######################

    host = Variable.get('POSTGRESQL_HOST')
    database = Variable.get('POSTGRESQL_DB')
    username = Variable.get('POSTGRESQL_USER')
    password = Variable.get('POSTGRESQL_PASSWORD')
    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
    engine = sqlalchemy.create_engine(conn_url)

    ##### FORECAST
    

    df_fr['fecha'] = df_fr['fecha'].apply(lambda x : pd.to_datetime(x))
    df_fr['fecha_carga'] = df_fr['fecha_carga'].apply(lambda x : pd.to_datetime(x))

    df_real['fecha_carga'] = df_real['fecha_carga'].apply(lambda x : pd.to_datetime(x))
    df_real['fecha'] = df_real['fecha'].apply(lambda x : pd.to_datetime(x))
    
    print(df_fr.info())
    print(df_real.info())
    #DELETE
    connection = engine.connect()
    delete_query = "TRUNCATE TABLE forecast_and_planning.forecast"
    connection.execute(text(delete_query))
    connection.close()

    # INSERT
    df_fr.to_sql(name="forecast",
    con=engine,
    schema="forecast_and_planning",
    if_exists='append',
    index=False,
    chunksize=20000,
    method='multi')

    #### DOTACION

    # DELETE
    connection = engine.connect()
    delete_query = "TRUNCATE TABLE forecast_and_planning.dotacion_real"
    connection.execute(text(delete_query))
    connection.close()

    # INSERT
    df_real.to_sql(name="dotacion_real",
    con=engine,
    schema="forecast_and_planning",
    if_exists='append',
    index=False,
    chunksize=20000,
    method='multi')

    #### VALIDACION DE LA CARGA DE DATOS

    connection = engine.connect()

    engine = sqlalchemy.create_engine(conn_url)
    df_real_base = pd.read_sql("SELECT * FROM forecast_and_planning.dotacion_real", con=engine)

    engine = sqlalchemy.create_engine(conn_url)
    df_fr_base = pd.read_sql("SELECT * FROM forecast_and_planning.forecast", con=engine)

    ################ LOGS ###########################
    if df_real_base.shape[0] == df_real.shape[0]:
        print('EXITOSO: Se ha cargado exitosamente la base dotacion')
        print('Datos SQL dotacion: {}'.format(df_real_base.shape[0]))
        print('Datos GoogleSheet dotacion: {}'.format(df_real.shape[0]))
    else:
        print('ERROR: No se ha cargado exitosamente la base dotacion')
        print('Datos SQL dotacion: {}'.format(df_real_base.shape[0]))
        print('Datos GoogleSheet dotacion: {}'.format(df_real.shape[0]))


    if df_fr_base.shape[0] == df_fr.shape[0]:
        print('EXITOSO: Se ha cargado exitosamente la base forecast')
        print('Datos SQL dotacion: {}'.format(df_fr_base.shape[0]))
        print('Datos GoogleSheet dotacion: {}'.format(df_fr.shape[0]))
    else:
        print('ERROR: No se ha cargado exitosamente la base forecast')
        print('Datos SQL forecast: {}'.format(df_fr_base.shape[0]))
        print('Datos GoogleSheet forecast: {}'.format(df_fr.shape[0]))
    connection.close()

def main_execution(ts):
    import os
    import time

    keys = credenciales()
    today = fecha_ejecucion(ts)
    gsheets_to_sql(keys,today)

    
    time.sleep (10)
    os.remove('temp_keys.json')


default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'etl_dot_pickers_a_bdd',
    default_args=default_args,
    description="Automatización de dotación de pickers a Base de datos",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2022, 11, 22, tz="America/Santiago"),
    catchup=False,
    tags=["OPS","GOOGLE","GOOGLE_SHEET"],
) as dag:

    dag.doc_md = """
    Obtención de dotación y carga automática \n
    a bases de datos.
    """ 

    t0 = PythonOperator(
        task_id = "ejecucion_principal",
        python_callable = main_execution,
    )
