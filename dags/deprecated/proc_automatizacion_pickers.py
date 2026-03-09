from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def get_user(df_formato, formato_RUT_get, largo, headersQA, myfile):
    import pandas as pd
    import requests
    import time

    contador_fallo_RUT = 0
    df_get_request = pd.DataFrame()

    for x in range(largo):
        r_get = requests.get(formato_RUT_get.format(df_formato['RUT'].iat[x]), headers=headersQA)
        if r_get.text == '[]':
            print (r_get.content)
            print (r_get.status_code)
            print ('Usuario con el siguiente RUT no encontrado dentro de la API')
            print (df_formato['RUT'].iat[x])
            myfile.write (str(r_get.content)+ "\n")
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write('Usuario con el siguiente RUT no encontrado dentro de la API\n')
            myfile.write (str(df_formato['RUT'].iat[x])+ "\n")
            contador_fallo_RUT = contador_fallo_RUT + 1
        elif r_get.status_code == 200:
            df = pd.DataFrame(r_get.json())
            try:
                json_carga = df[['id','firstname','lastname','email','employeeId','locations','profileId','status']]
                print ('Respuesta 200 en GET')
                myfile.write (str(r_get.status_code)+ '\n')
            except Exception as e: # pylint: disable=bare-except
                print (str(e))
                print (r_get.content)
                continue
            df_get_request = pd.concat([df_get_request, json_carga], axis = 0)
        elif r_get.status_code == 400:
            print (r_get.status_code)
            print ('Parámetros inválidos')
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write ('Parámetros inválidos\n')
            break
        elif r_get.status_code == 401:
            print (r_get.status_code)
            print ('El header no existe o es inválido\n')
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write('El header no existe o es inválido\n')
            break
        elif r_get.status_code == 403:
            print (r_get.status_code)
            print ('Permiso denegado para las credenciales utilizadas')
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write ('Permiso denegado para las credenciales utilizadas\n')
            break
        elif r_get.status_code == 429:
            print (r_get.status_code)
            print ('Muchas requests simultáneas, porfavor espere 30 segundos') 
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write('Muchas requests simultáneas, porfavor espere 30 segundos\n')
            time.sleep(30)
            print ('Reintentando...')
            myfile.write  ('Reintentando...\n')
            try:
                r_get = requests.get(formato_RUT_get.format(df_formato['RUT'].iat[x]), headers=headersQA)
                if r_get.text == '[]':
                    print (r_get.content)
                    print (r_get.status_code)
                    print ('Usuario con el siguiente RUT no encontrado dentro de la API')
                    print (df_formato['RUT'].iat[x])
                    myfile.write (str(r_get.content)+ "\n")
                    myfile.write (str(r_get.status_code)+ "\n")
                    myfile.write('Usuario con el siguiente RUT no encontrado dentro de la API\n')
                    myfile.write (str(df_formato['RUT'].iat[x])+ "\n")
                    contador_fallo_RUT = contador_fallo_RUT + 1
                else:
                    df = pd.DataFrame(r_get.json())
                    try:
                        json_carga = df[['id','firstname','lastname','email','employeeId','locations','profileId','status']]
                        print ('Respuesta 200 en GET')
                        myfile.write (str(r_get.status_code)+ '\n')
                    except: # pylint: disable=bare-except
                        continue
                    df_get_request = pd.concat([df_get_request, json_carga], axis = 0)
            except Exception as e: # pylint: disable=bare-except
                print (str(e))
                print ('Intento fallido, intentando nuevamente en 1 minuto')
                myfile.write ('Intento fallido, intentando nuevamente en 1 minuto\n')
                time.sleep(60)
                print ('Reintentando tras 1 minuto...')
                myfile.write ('Reintentando tras 1 minuto...\n')
                try:
                    r_get = requests.get(formato_RUT_get.format(df_formato['RUT'].iat[x]), headers=headersQA)
                    if r_get.text == '[]':
                        print (r_get.content)
                        print (r_get.status_code)
                        print ('Usuario con el siguiente RUT no encontrado dentro de la API')
                        print (df_formato['RUT'].iat[x])
                        myfile.write (str(r_get.content)+ "\n")
                        myfile.write (str(r_get.status_code)+ "\n")
                        myfile.write('Usuario con el siguiente RUT no encontrado dentro de la API\n')
                        myfile.write (str(df_formato['RUT'].iat[x])+ "\n")
                        contador_fallo_RUT = contador_fallo_RUT + 1
                    else:
                        df = pd.DataFrame(r_get.json())
                        try:
                            json_carga = df[['id','firstname','lastname','email','employeeId','locations','profileId','status']]
                            print ('Respuesta 200 en GET')
                            myfile.write (str(r_get.status_code) + '\n')
                        except:
                            continue
                        df_get_request = pd.concat([df_get_request, json_carga], axis = 0)
                except Exception as e: # pylint: disable=bare-except
                    print (str(e))
                    print (r_get.status_code)
                    print ('Error por tercera vez, se realiza break')
                    myfile.write (str(r_get.status_code)+ "\n")
                    myfile.write('Error por tercera vez, se realiza break\n')
                    break
        elif r_get.status_code  == 503:
            print (r_get.status_code)
            print ('No se puede alcanzar el servidor, reintentando en 10 minutos')
            myfile.write (str(r_get.status_code)+ "\n")
            myfile.write('No se puede alcanzar el servidor, reintentando en 10 minutos\n')
            time.sleep(600)
            print ('Reintentando...')
            myfile.write ('Reintentando...\n')
            try:
                r_get = requests.get(formato_RUT_get.format(df_formato['RUT'].iat[x]), headers=headersQA)
                if r_get.text == '[]':
                    print (r_get.content)
                    print (r_get.status_code)
                    print ('Usuario con el siguiente RUT no encontrado dentro de la API')
                    print (df_formato['RUT'].iat[x])
                    myfile.write (str(r_get.content)+ "\n")
                    myfile.write (str(r_get.status_code)+ "\n")
                    myfile.write('Usuario con el siguiente RUT no encontrado dentro de la API\n')
                    myfile.write (str(df_formato['RUT'].iat[x])+ "\n")
                    contador_fallo_RUT = contador_fallo_RUT + 1
                else:
                    df = pd.DataFrame(r_get.json())
                    try:
                        json_carga = df[['id','firstname','lastname','email','employeeId','locations','profileId','status']]
                        print ('Respuesta 200 en GET')
                        myfile.write (str(r_get.status_code)+ '\n')
                    except: # pylint: disable=bare-except
                        continue
                    df_get_request = pd.concat([df_get_request, json_carga], axis = 0)
            except Exception as e: # pylint: disable=bare-except
                print (str(e))
                print ('Intento fallido, intentando nuevamente en 30 minutos')
                myfile.write ('Intento fallido, intentando nuevamente en 30 minutos\n')
                time.sleep(1800)
                print ('Reintando...')
                myfile.write ('Reintentando...\n')
                try:
                    r_get = requests.get(formato_RUT_get.format(df_formato['RUT'].iat[x]), headers=headersQA)
                    if r_get.text == '[]':
                        print (r_get.content)
                        print (r_get.status_code)
                        print ('Usuario con el siguiente RUT no encontrado dentro de la API')
                        print (df_formato['RUT'].iat[x])
                        myfile.write (str(r_get.content)+ "\n")
                        myfile.write (str(r_get.status_code)+ "\n")
                        myfile.write('Usuario con el siguiente RUT no encontrado dentro de la API\n')
                        myfile.write (str(df_formato['RUT'].iat[x])+ "\n")
                        contador_fallo_RUT = contador_fallo_RUT + 1
                    else:
                        df = pd.DataFrame(r_get.json())
                        try:
                            json_carga = df[['id','firstname','lastname','email','employeeId','locations','profileId','status']]
                            print ('Respuesta 200 en GET')
                            myfile.write (str(r_get.status_code)+ '\n')
                        except: # pylint: disable=bare-except
                            continue
                        df_get_request = pd.concat([df_get_request, json_carga], axis = 0)
                except Exception as e: # pylint: disable=bare-except
                    print (str(e))
                    print (r_get.status_code)
                    print ('Error por tercera vez, se realiza break')
                    myfile.write (str(r_get.status_code)+ "\n")
                    myfile.write('Error por tercera vez, se realiza break\n')
                    break
        #else: #si la api no responde ni 200, 400, 401, 403, 429, 503, o tampoco está vacío el df
            #myfile.write ('Error desconocido')
            #break
    #myfile.write (str(df_get_request))
    df_out = pd.merge(df_formato[['RUT','Tienda']], df_get_request, how = 'left', left_on = 'RUT', right_on = 'employeeId')
    print (df_out)
    myfile.write (str(df_out) + '\n')

    if contador_fallo_RUT > 0:
        print ('Error, no todos los RUT son válidos, se cierra el programa')
        myfile.write ('Error, no todos los RUT son válidos, se cierra el programa')
        myfile.close()
        raise Exception('Error, no todos los RUT son válidos, se cierra el programa')

    return (df_out)
#get_user()

def put_user(dfx, myfile, formato_PUT_user, headersQA):
    import pandas as pd
    import requests
    import time

    for x in range(len(dfx)):
        if pd.isna(dfx['employeeId'].iat[x]) is True:
            print ('Error, usuario con rut ' + dfx['RUT'].iat[x] + ' sin datos')
            myfile.write ('Error, usuario con rut ' + str(dfx['RUT'].iat[x]) + ' sin datos' + '\n')
            continue
        json_carga_put = (dfx[['id','firstname','lastname','email','locations','profileId','status']].iloc[[x]]).to_dict('records')
        print (json_carga_put)
        myfile.write (str(json_carga_put)+ '\n')
        carga = json_carga_put[0]
        #print (carga)
        id_unic = carga['id']
        try:
            del carga['id']
        except KeyError:
            break
        tiendax = []
        dfx['Tienda'].iat[x] = dfx['Tienda'].iat[x][:4]
        tiendax.append(dfx['Tienda'].iat[x])
        tiendainput = tiendax
        carga['locations'] = tiendainput
        print (carga)
        myfile.write (str(carga) + '\n')
        r = requests.put(formato_PUT_user.format(id_unic), json = carga, headers=headersQA)
        if r.status_code  == 200:
            print ('Datos Actualizados correctamente')
            print (r.status_code)
            myfile.write ('Datos Actualizados correctamente\n')
            myfile.write (str(r.status_code)+ "\n")
        elif r.status_code == 400:
            print (r.status_code)
            print ('Parámetros inválidos')
            myfile.write (str(r.status_code)+ "\n")
            myfile.write ('Parámetros inválidos\n')
            break
        elif r.status_code == 401:
            print (r.status_code)
            print ('El header no existe o es inválido')
            myfile.write (str(r.status_code)+ "\n")
            myfile.write('El header no existe o es inválido\n')
            break
        elif r.status_code == 403:
            print (r.status_code)
            print ('Permiso denegado para las credenciales utilizadas')
            myfile.write (str(r.status_code)+ "\n")
            myfile.write ('Permiso denegado para las credenciales utilizadas\n')
            break
        elif r.status_code == 429:
            print (r.status_code)
            print ('Muchas requests simultáneas, porfavor espere 30 segundos')
            myfile.write (str(r.status_code)+ "\n")
            myfile.write('Muchas requests simultáneas, porfavor espere 30 segundos\n')
            time.sleep(30)
            print ('Reintentando...')
            myfile.write  ('Reintentando...\n')
            try:
                r = requests.put(formato_PUT_user.format(id_unic), json = carga, headers=headersQA)
            except Exception as e: # pylint: disable=bare-except
                print (str(e))
                print ('Intento fallido, intentando nuevamente en 1 minuto')
                myfile.write ('Intento fallido, intentando nuevamente en 1 minuto\n')
                time.sleep(60)
                print ('Reintentando...')
                myfile.write ('Reintentando...\n')
                try:
             
                    r = requests.put(formato_PUT_user.format(id_unic), json = carga, headers=headersQA)
                except Exception as e: # pylint: disable=bare-except
                    print (str(e))
                    print (r.status_code)
                    print ('Error por tercera vez, se realiza break')
                    myfile.write (str(r.status_code)+ "\n")
                    myfile.write('Error por tercera vez, se realiza break\n')
                    break
        elif r.status_code  == 503:
            print (r.status_code)
            print ('No se puede alcanzar el servidor, reintentando en 10 minutos')
            myfile.write (str(r.status_code)+ "\n")
            myfile.write('No se puede alcanzar el servidor, reintentando en 10 minutos\n')
            time.sleep(600)
            print ('Reintentando...')
            myfile.write ('Reintentando...\n')
            try:
                r = requests.put(formato_PUT_user.format(id_unic), json = carga, headers=headersQA)
            except Exception as e: # pylint: disable=bare-except
                print (str(e))
                print ('Intento fallido, intentando nuevamente en 30 minutos')
                myfile.write ('Intento fallido, intentando nuevamente en 30 minutos\n')
                time.sleep(1800)
                print ('Reintentando...')
                myfile.write ('Reintentando...\n')
                try:
                    r = requests.put(formato_PUT_user.format(id_unic), json = carga, headers=headersQA)
                except Exception as e: # pylint: disable=bare-except
                    print (str(e))
                    print (r.status_code)
                    print ('Error por tercera vez, se realiza break')
                    myfile.write (str(r.status_code)+ "\n")
                    myfile.write('Error por tercera vez, se realiza break\n')
                    break
    myfile.close()

def automa_pickers(id_drive, ds):
    import pandas as pd
    from datetime import datetime, timedelta
    import json
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive
    from oauth2client.service_account import ServiceAccountCredentials
    import os

    headersQA = {
        "Content-Type": "application/json",
        "janis-api-key": Variable.get('JANIS_API_KEY'),
        "janis-api-secret" : Variable.get('JANIS_API_SECRET'),
        "janis-client" : Variable.get("JANIS_CLIENT")
    }

    myfile = open('log.txt', 'w')

    #sys.stdout=open("logtest.txt","w")

    diccionario_keys = Variable.get('keys_drive',deserialize_json=True)
    
    #variable = json.dumps(diccionario_keys)

    with open('temp_keys.json', 'w', encoding='utf-8') as f:
        json.dump(diccionario_keys, f, ensure_ascii=False)

    #Día de ejecución
    #fecha = datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y')
    #fecha = datetime.now()
    #fechayhora = fecha.strftime("%d/%m/%Y %H:%M:%S")

    gauth = GoogleAuth()
    scope = ['https://www.googleapis.com/auth/drive.readonly']
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name('temp_keys.json', scope)
    drive = GoogleDrive(gauth)

    query = "'{}' in parents and trashed=false"
    query=query.format(id_drive)
    file_list = drive.ListFile({'q': query}).GetList()
    largo= len(file_list)
    llaves = ['id','title','createdDate','modifiedDate']
    lista_info = []
    df_info = pd.DataFrame(columns = ['id', 'nombre_archivo', 'fecha_creacion','fecha_modif'])
    for z in range(largo):
        lista_info = ([file_list[z].get(llave) for llave in llaves])
        df_info.loc[len(df_info)] = lista_info
    df_info['fecha_creacion'] = pd.to_datetime(df_info['fecha_creacion'])
    df_info['fecha_creacion'] = pd.to_datetime(df_info['fecha_modif'])

    regex_fecha = r'([0-9]{2}\-[0-9]{2}\-[0-9]{4})'
    df_info['fecha'] = df_info['nombre_archivo'].str.extract(regex_fecha, expand = False)
    df_info['fecha'] = pd.to_datetime(df_info['fecha'])

    fecha_ayer = datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y')
    fecha_hoy = (datetime.strptime(ds, '%Y-%m-%d') + timedelta(days=1)).strftime('%d-%m-%Y')

    df_last= (df_info.loc[df_info['fecha'] == (fecha_hoy)]) #validar si el día de hoy es el mismo que el se quiere cargar

    if len (df_last.index) != 0:
        pass
    else:
        print ('Error, no hay archivos con la fecha de hoy')
        raise Exception('Error, no hay archivos con la fecha de hoy')
    
    #df_last= (df_info.loc[df_info['fecha'] == (fecha_tomorrow)]) #validar si mañana es el día que se quiere cambiar

    id_last = df_last.iloc[0]['id']
    name_last = df_last.iloc[0]['nombre_archivo']

    file_id = id_last
    # store the output file name
    output_file_name = name_last
    # create an instance of Google Drive file with auth of this instance
    f = drive.CreateFile({'id': file_id})
    #content = f.GetContentFile()

    # Guarda el archivo ÚNICO necesario, se borra posteriormente.
    f.GetContentFile(output_file_name)

    if name_last.endswith('.xlsx'):
        df_formato = pd.read_excel(output_file_name, index_col=False)
    elif name_last.endswith('.csv'):
        df_formato = pd.read_csv(output_file_name, index_col=False, names=['RUT','Tienda','Nombre','Apellido','Operador'])
    else:
        print ('error, tipo de archivo incorrecto')
        myfile.write('error, tipo de archivo incorrecto')
        myfile.close()
        raise Exception('Error, tipo de archivo incorrecto')

    formato_RUT_get = Variable.get("JANIS_API_URL") + 'user?employeeId={}'
    formato_PUT_user = Variable.get("JANIS_API_URL") + 'user/{}'
    #df_formato = pd.read_excel(output_file_name, index_col=False)
    df_formato['RUT'] = df_formato['RUT'].values.astype('str')

    #print (fechayhora)
    print (df_formato)

    #myfile.write (str(fechayhora) + '\n')
    largo = len(list(df_formato['RUT']))

    df_out = get_user(df_formato, formato_RUT_get, largo, headersQA, myfile)
    put_user(df_out, myfile, formato_PUT_user, headersQA)


    os.remove(output_file_name) #Borra el archivo que se genera localmente para hacer dfs; to do: guardarlo en S3 antes de borrarlo.
    

def borrar_archivo():
    import os
    os.remove('temp_keys.json')
    os.remove('log.txt')

default_args = {
    "owner": "capacity_and_planning",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
with DAG(
    'proc_automatizacion_pickers',
    default_args=default_args,
    description="Carga automática de pickers en la API",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2022, 11, 3, tz="America/Santiago"),
    catchup=False,
    tags=["OPS","Janis","API","GET","PUT"],
) as dag:

    dag.doc_md = """
    Extracción y carga de datos dado formato de operadores \n
    Para ingresar mediante API a Janis.
    """ 
    t0 = PythonOperator(
        task_id = "automa_pickers_Touch_Shoppers",
        python_callable = automa_pickers,
        op_kwargs = {
            "id_drive": Variable.get('OPERADOR_TIMEJOBS_SHOPPERS'),
        }
    )

    t1 = PythonOperator(
        task_id = "automa_pickers_Touch_Pickers",
        python_callable = automa_pickers,
        op_kwargs = {
            "id_drive": Variable.get('OPERADOR_TIMEJOBS_PICKERS'),
        }
    )

    t2 = PythonOperator(
        task_id = "automa_pickers_Timejobs",
        python_callable = automa_pickers,
        op_kwargs = {
            "id_drive": Variable.get('OPERADOR_TOUCH'),
        }
    )

    t3 = PythonOperator(
        task_id = "automa_pickers_Boosmap",
        python_callable = automa_pickers,
        op_kwargs = {
            "id_drive": Variable.get('OPERADOR_BOOSMAP'),
        }
    )

    t4 = PythonOperator(
        task_id = "borrar_archivos",
        python_callable = borrar_archivo,
    )

t0 >> t4
t1 >> t4
t2 >> t4
t3 >> t4
