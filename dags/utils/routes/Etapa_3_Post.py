import requests
import json
import pandas as pd
from datetime import timedelta, date, datetime
import pytz
import random
import boto3
import pymongo


def inyeccion(janis_api_secret, janis_api_client, janis_api_key, aws_access_key, aws_secret_key, aws_bucket_name, mongo_user, mongo_pass, cluster_name, db):

    s3_resource = boto3.resource("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name="us-east-1")

    bucket = s3_resource.Bucket(aws_bucket_name)
    fecha_hoy = (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0)).strftime('%Y-%m-%d')

    #parametros
    id_transportadora = '0581-3'
    dicc_vehiculo = 27
    dicc_choferes = "17334430-90"

    prefix = "ecommops/capacity/rutas/" + fecha_hoy + "/"
    name = 'Etapa_2_' + id_transportadora + '.csv'
    file_name = prefix + name
    csv_file = bucket.Object(file_name)
    df = pd.read_csv(csv_file.get()["Body"])

    print(f'Etapa 3. Se importaron {len(df)} ordenes desde la etapa 2.')

    headers = {
        'Janis-Api-Secret': janis_api_secret ,
        'Janis-Client': janis_api_client,
        'Janis-Api-Key': janis_api_key,
        'Content-Type': 'application/json' }

    df['RutaID'] = df['Ruta'].apply(lambda x: int(x.split(' ')[1]))
    
    resp_list = []

    try:
        for x in list(df['RutaID'].unique()):
            
            df_json = {}
            df_json['refId'] = id_transportadora + (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0)).strftime("%Y%m%d%H%M%S")
            df_json['vehicleId'] = dicc_vehiculo
            df_json['initialCash'] = 0
            df_json['orders'] = [{'orderId': int(x)} for x in df.loc[df['RutaID'] == x]['Orden'].values]
            df_json['deliveryAssistantsEmployeeIds'] = [ dicc_choferes] 
            df_json['driversEmployeeIds'] = [dicc_choferes] 
            df_json['logisticCompanyId'] = 5 #Traer
            df_json = json.dumps(df_json, indent=4)

            url = "https://logistics.janis.in/api/routes"

            response = requests.request("POST", url, headers=headers, data=df_json)
            response = response.json()
            resp_list.append(response)
            print(f'Etapa 3. La ruta creada fue: {response}.')

            respuesta_response = {}
            respuesta_response['response'] = response
            
            respuesta_response["timestamp"] = (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0))
            respuesta_response["pedidos"] = len(df.loc[df['RutaID'] == x])
            respuesta_response["transportadora"] = id_transportadora
            respuesta_response['ruta'] = int(x)
            respuesta_response['refId'] = id_transportadora + (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0)).strftime("%Y%m%d%H%M%S")

            mongo_client = pymongo.MongoClient("mongodb+srv://"+mongo_user+":"+mongo_pass+"@"+cluster_name+".lppxi.mongodb.net/"+db+"?retryWrites=true&w=majority&authSource=admin")
            mongo_collection = mongo_client[db]["routes"]
            #mongo_metadata = mongo_client.get_colletion("routes")
            mongo_collection.insert_one(respuesta_response)

    except Exception as e:
        print(f"ERROR: {e}")
        return False

    if len(resp_list) != 0:
        print('Etapa 3. Se ha finalizado exitosamente la ejecucion de la tercera etapa.')
    else:
        print('Etapa 3. No se ha inyectado ninguna ruta al sistema.')

    return True
