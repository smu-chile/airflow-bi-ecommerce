import requests
import json
import pandas as pd
from datetime import timedelta, date
import random
import boto3


def inyeccion(janis_api_secret, janis_api_client, janis_api_key, aws_access_key, aws_secret_key, aws_bucket_name):

    s3_resource = boto3.resource("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name="us-east-1")

    bucket = s3_resource.Bucket(aws_bucket_name)
    fecha_hoy = date.today().strftime('%Y-%m-%d')

    #parametros
    id_transportadora = '0469'
    dicc_vehiculo = 27
    dicc_choferes = "17334430-90"

    prefix = "ecommops/capacity/rutas/" + fecha_hoy + "/"
    name = 'Etapa_2_' + id_transportadora + '.csv'
    file_name = prefix + name
    csv_file = bucket.Object(file_name)
    df = pd.read_csv(csv_file.get()["Body"])

    fecha_mañana = (date.today() + timedelta(days=1))
    mes = int(fecha_mañana.month)
    dia = int(fecha_mañana.day)
    ano = int(fecha_mañana.year)

    headers = {
        'Janis-Api-Secret': janis_api_secret ,
        'Janis-Client': janis_api_client,
        'Janis-Api-Key': janis_api_key,
        'Content-Type': 'application/json' }

    df['RutaID'] = df['Ruta'].apply(lambda x: int(x.split(' ')[1]))
    
    try:
        for x in list(df['RutaID'].unique()):
            
            df.loc[df['RutaID'] == x]
            random_numero = random.randint(0,1000)
            id_pedido = str(id_transportadora) + str(ano) + str(mes) + str(dia) + str(random_numero)
            df_json = {}
            df_json['refId'] = id_pedido
            df_json['vehicleId'] = dicc_vehiculo
            df_json['initialCash'] = 0
            df_json['orders'] = [{'orderId': int(x)} for x in df.loc[df['RutaID'] == x]['Orden'].values]
            df_json['deliveryAssistantsEmployeeIds'] = [ dicc_choferes] 
            df_json['driversEmployeeIds'] = [dicc_choferes] 
            df_json['logisticCompanyId'] = 5 #Traer
            df_json = json.dumps(df_json, indent=4)

            url = "https://logistics.janisqa.in/api/routes"

            
            response = requests.request("POST", url, headers=headers, data=df_json)
            response = response.json()
            print(response)

    except Exception as e:
        print(f"ERROR: {e}")
        return False

    return True

