import requests
from datetime import timedelta, date, datetime
import pandas as pd
from io import StringIO
import boto3
import smtplib
import pytz
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(email_recipient,
               email_subject,
               email_message,
               attachment_location = ''):
    
    host = "smtprelay.unimarc.local"
    port = 25
    sender = "reportes_ecommerce@smu.cl"
    receiver = email_recipient
    mail_body = email_message

    message = MIMEMultipart()
    message['From'] = sender
    message['To'] = ", ".join(receiver)
    message['Subject'] = email_subject
    body = MIMEText(mail_body, 'plain')
    #The body and the attachments for the mail
    message.attach(body)

    #server = smtplib.SMTP(host, port=port)
    server = smtplib.SMTP_SSL(host, port=port)
    server.send_message(message, from_addr=sender, to_addrs=receiver)   
    print("Se envio correctamente el mail de fallas")
    
    return True

def janis_query(janis_api_secret, janis_api_client, janis_api_key, aws_access_key, aws_secret_key, aws_bucket_name):
    
    headers = {
            'Janis-Api-Secret': janis_api_secret,
            'Janis-Client': janis_api_client,
            'Janis-Api-Key': janis_api_key,
            'Content-Type': 'application/json' }

    #parametros
    id_transportadora = '0469'
    lista_enviar = ['fmacaya@smu.cl','djimenezg@smu.cl']
    fecha_mañana = (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=1)).strftime('%Y-%m-%d')
    fecha_hoy = (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0)).strftime('%Y-%m-%d')
    shipping_date = str(fecha_mañana) +'T00:01:00-03:00'
    capacidad = 25
    numero_camiones = 2

    lista_ordenes = []

    indicador = True
    contador = 1

    lista_error_nulo = []
    lista_error_ruta = []

    while indicador == True: 
        
        url = "https://janisqa.in/api/order/get?carrierId={}&status=ready_for_shipping&perPage=30&page={}&shippingDate={}".format(id_transportadora,str(contador),shipping_date)
        
        payload={}
        response = requests.request("GET", url, headers=headers, data=payload)
        response = response.json()
        
        try:
            for x in response['data']:
                
                if x['route'] == None:

                    for reg in range(len(x['shipping'])):

                        if x['shipping'][reg]['main'] == True:

                            #print(x['shipping'][reg]['main'])
                            id_orden = x['id']
                            lat = x['shipping'][reg]['address']['lat']
                            lng = x['shipping'][reg]['address']['lng']
                            lista_ordenes.append([id_transportadora, id_orden, lat, lng])

                        else:
                            pass
                else:
                    lista_error_ruta.append(x['id'])
                    pass

            if response['data'] == list():
                indicador = False
            else:
                indicador = True
            
            contador = contador + 1
            
            
            #x = json.dumps(x, indent=4)
            #print(x)

        # import pickle
        # with open('saved_dictionary.', 'wb') as f:
            #    pickle.dump(x, f)

        except Exception as e:
            print(f'Error: {e}')
            return False

    df2 = pd.DataFrame(lista_ordenes, columns=['transportadora','Orden','lat','lng'])

    print(f'Etapa 1. Input Janis: Se extrajeron {len(df2)} ordenes')

    if len(df2) > (capacidad * numero_camiones):
        print('La Capacidad maxima de la transportadora ha sido excedida. Se generara un error en el proceso')
    else:
        print('La Capacidad maxima de la transportadora esta de acuardo a los parametros de entrada del proceso.')

    buffer = StringIO()
    

    for row in df2.itertuples():
        if (pd.isnull(row.lat) or row.lat == '') or (pd.isnull(row.lat) or row.lng == ''):
        #if (row.lat is None or row.lat == '') or (row.lat is None or row.lng == '') == True:
            lista_error_nulo.append(row.Orden)

    total_lista_error = lista_error_ruta + lista_error_nulo

    if len(total_lista_error) != 0:
        print(f'Etapa 1. Se excluyeron {len(total_lista_error)} ordenes por falta de coordenadas')
        body = 'Estimados: \n\n Debido a que algunas ordenes se encuentran con problemas, se han excluido del proceso automático de generación de rutas las siguientes ordenes: \n\n                        Ordenes con Ruta: {}  \n\n                                         Ordenes sin Latitud y Longitud  : {}      \n\n Quedamos atento a cualquier consulta. \n\n Saludos'.format(lista_error_ruta, lista_error_nulo)
        #send_email(lista_enviar,'Optimizacion de Ruta: Mensaje de Error por Ordenes No Asignadas', body)
        df2 = df2[~df2['Orden'].isin(total_lista_error)]

        print(f'Las ordenes que NO se ejecutaron, fueron: {total_lista_error}')

        df2_error = df2[df2['Orden'].isin(total_lista_error)]
        df2_error.to_csv(buffer, header=True, index=False, encoding="utf-8")
        buffer.seek(0)

        prefix = "ecommops/capacity/rutas/" + fecha_hoy + '/'
        name = 'Etapa_1_' + id_transportadora + '_ERRORS' + '.csv'
        file_name = prefix+name

        s3_client = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name = "us-east-1")
        response = s3_client.put_object(Bucket=aws_bucket_name, Key=file_name, Body=buffer.getvalue())
    
    df2.to_csv(buffer, header=True, index=False, encoding="utf-8")
    buffer.seek(0)

    prefix = "ecommops/capacity/rutas/" + fecha_hoy + '/'
    name = 'Etapa_1_' + id_transportadora + '.csv'
    file_name = prefix+name

    s3_client = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name = "us-east-1")
    response = s3_client.put_object(Bucket=aws_bucket_name, Key=file_name, Body=buffer.getvalue())

    if len(df2) != 0:
        print('Etapa 1. Se ha finalizado exitosamente la ejecucion de la primera etapa.')
    else:
        print('Etapa 1. El dataframe no tiene registros, repetir operacion')
     
    return True