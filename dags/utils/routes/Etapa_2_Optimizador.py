import pandas as pd
import numpy as np
from vincenty import vincenty
from datetime import date, timedelta, datetime
import pytz
import math
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
import boto3
from io import StringIO
from airflow.models import Variable

def report_generator(aws_access_key, aws_secret_key, aws_bucket_name):

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
    search_parameters.time_limit.seconds = 30
    search_parameters.log_search = True

    s3_resource = boto3.resource("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name="us-east-1")

    bucket = s3_resource.Bucket(aws_bucket_name)
    
    fecha_hoy = (datetime.now(pytz.timezone('Chile/Continental')) + timedelta(days=0)).strftime('%Y-%m-%d')
    
    #parametros
    id_transportadora = Variable.get("CAPACITY_ID_TRANSPORTADORA")
    lng_tienda = -70.6068642
    lat_tienda = -33.5138181

    prefix = "ecommops/capacity/rutas/" + fecha_hoy + "/"
    name = 'Etapa_1_' + id_transportadora + '.csv'
    file_name = prefix + name

    csv_file = bucket.Object(file_name)
   
    df2 = pd.read_csv(csv_file.get()["Body"])
  
    df2['lat'] = df2['lat'].astype(float)
    df2['lng'] = df2['lng'].astype(float)

    print(f'Etapa 2. Ingresaron {len(df2)} ordenes a la etapa 2 desde la etapa 1.')

    ### DISTANCIAS ###

    def vincenty_algo(lon1, lat1, lon2, lat2):
        place1 = (float(lon1), float(lat1))
        place2 = (float(lon2), float(lat2))
        dist = vincenty(place1, place2)
        
        return dist

    def get_distance_matrix(data4matrix):
        dist_matrix = []

        for row in range(len(data4matrix)):
            temp_list = []
            for col in range(len(data4matrix)):
                dist = vincenty_algo(data4matrix.loc[row, 'lat'], data4matrix.loc[row, 'lng'], 
                                    data4matrix.loc[col, 'lat'], data4matrix.loc[col, 'lng'])
                temp_list.append(dist)
            dist_matrix.append(temp_list)
        
        return np.array(dist_matrix)

    def create_data_model():

        data = {}
        distance_mtrx = get_distance_matrix(data_4_matrix)
        data['distance_matrix'] = distance_mtrx
        demand = [1 for x in range(len(distance_mtrx))]
        demand[0] = 0
        data['demands'] = demand
        data['vehicle_capacities'] = [capacity for x in range(trucks_needed)]
        data['num_vehicles'] = trucks_needed
        data['depot'] = 0
        return data

    def print_solution(data, manager, routing, solution):

        #print(f'Objective: {solution.ObjectiveValue()}')
        total_distance = 0
        total_load = 0
        lista_rutas = []
        lista_carga = []
        dista_list = []
        for vehicle_id in range(data['num_vehicles']):
            stops_list = []
            index = routing.Start(vehicle_id)
            plan_output = 'Route for vehicle {}:\n'.format(vehicle_id)
            route_distance = 0
            route_load = 0
            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                route_load += data['demands'][node_index]
                plan_output += ' Cliente {0} -> '.format(node_index)
                stops_list.append(node_index)
                previous_index = index
                index = solution.Value(routing.NextVar(index))
                route_distance += routing.GetArcCostForVehicle(
                    previous_index, index, vehicle_id)
            dista_list.append(route_distance)
            lista_carga.append(route_load)
            plan_output += ' Cliente {0}\n'.format(manager.IndexToNode(index))
            stops_list.append(manager.IndexToNode(index))
            plan_output += 'Distance of the route: {}km\n'.format(route_distance)
            plan_output += 'Load of the route: {}\n'.format(route_load)
            #print(plan_output)
            lista_rutas.append(stops_list)
            total_distance += route_distance
            total_load += route_load
        
        #print('Total distance of all routes: {}km'.format(total_distance))
        #print('Total load of all routes: {}'.format(total_load))
        return lista_rutas, dista_list, lista_carga


    def main():

        data = create_data_model()

        manager = pywrapcp.RoutingIndexManager(len(data['distance_matrix']),
                                            data['num_vehicles'], data['depot'])

        routing = pywrapcp.RoutingModel(manager)

        def distance_callback(from_index, to_index):

            from_node = manager.IndexToNode(from_index)
            to_node = manager.IndexToNode(to_index)
            return data['distance_matrix'][from_node][to_node]

        transit_callback_index = routing.RegisterTransitCallback(distance_callback)

        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        def demand_callback(from_index):

            from_node = manager.IndexToNode(from_index)
            return data['demands'][from_node]

        demand_callback_index = routing.RegisterUnaryTransitCallback(
            demand_callback)
        routing.AddDimensionWithVehicleCapacity(
            demand_callback_index,
            0,  # null capacity slack
            data['vehicle_capacities'],  # vehicle maximum capacities
            True,  # start cumul to zero
            'Capacity')

        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC)
        search_parameters.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH)
        search_parameters.time_limit.FromSeconds(1)

        solution = routing.SolveWithParameters(search_parameters)
        
        if solution:
            lista_rutas, dista_list, lista_carga = print_solution(data, manager, routing, solution)

        return lista_rutas, dista_list, lista_carga

    df_resultado_transportadora = pd.DataFrame()

    try:

        df_transportadoras2 = df2.copy()
        capacity = 25
        centro_distribucion = []
        centro_distribucion.insert(0, {'orden': 'Origen', 'lat': -33.5138181,'lng': -70.6068642})
        data_4_matrix = pd.concat([pd.DataFrame(centro_distribucion), df_transportadoras2], ignore_index=True)
        data_4_matrix.reset_index(inplace=True, drop=True)

        ordenes_totales = len(df_transportadoras2)

        trucks_needed = int(math.ceil(ordenes_totales / capacity))
        remanent = ordenes_totales % capacity

        if remanent < (1 * trucks_needed):
            trucks_needed = int(math.ceil(ordenes_totales / (1 + capacity)))
            capacity = capacity + 1

        elif remanent < (2 * trucks_needed):
            trucks_needed = int(math.ceil(ordenes_totales / (2 + capacity)))
            capacity = capacity + 2

        else:
            pass

        #print(f'Ordenes Totales: {ordenes_totales}, Camiones Necesarios: {ordenes_totales / TRUCK_CAPACITY}, Redondeado: {trucks_needed}')
        lista_de_rutas, dista_list, lista_carga = main()

        df_resultado_rutas = pd.DataFrame()
        df_resultado_transportadora = pd.DataFrame()

            #df_remanentes = pd.DataFrame()
            #print(lista_de_rutas)

        for ruta in range(len(lista_de_rutas)):

            df_resultado_transportadora_parcial = data_4_matrix[(data_4_matrix.index.isin(lista_de_rutas[ruta]))]
            df_resultado_transportadora_parcial = df_resultado_transportadora_parcial.reindex(lista_de_rutas[ruta])
            df_resultado_transportadora_parcial = df_resultado_transportadora_parcial.fillna('Mirador')
            df_resultado_transportadora_parcial['Ruta'] = 'Camion ' + str(ruta)

            df_resultado_rutas = pd.concat([df_resultado_rutas, df_resultado_transportadora_parcial])
            #df_resultado_transportadora_parcial.to_csv(f'df_resultado_parcial_de_ruta_{ruta}.csv', sep=';')

            # if (len(df_resultado_transportadora_parcial) <= 14) and (i != '0581-2 Mirador Norte'):
            #     remanente = True
            #     df_remanentes = pd.concat([df_remanentes, df_resultado_transportadora_parcial])

            # else:
            #     df_resultado_rutas = pd.concat([df_resultado_rutas, df_resultado_transportadora_parcial])
            #     cycle =+ 1

        df_resultado_transportadora = pd.concat([df_resultado_transportadora, df_resultado_rutas])

            # if remanente == True:
            #     df_remanente_transportadora = pd.concat([df_remanente_transportadora, df_remanentes])
        df_resultado_transportadora = df_resultado_transportadora[df_resultado_transportadora['orden'] != 'Origen']

        buffer = StringIO()
        df_resultado_transportadora.to_csv(buffer, header=True, index=True, encoding="utf-8")
        buffer.seek(0)

        name2 = 'Etapa_2_' + id_transportadora + '.csv'
        file_name2 = prefix+name2

        s3_client = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name = "us-east-1")
        response = s3_client.put_object(Bucket=aws_bucket_name, Key=file_name2, Body=buffer.getvalue())
        print(f'Etapa 2. El numero de ordenes procesadas fue de {len(df_resultado_transportadora)}')
    
    except Exception as e:
        print(f"Etapa 2 Error: Se excedio el numero maximo de camiones permitido.")
        return False

    if len(df_resultado_transportadora) != 0:
        print('Etapa 2. Se ha finalizado exitosamente la ejecucion de la segunda etapa.')
    else:
        print('Etapa 2. El dataframe no tiene registros, repetir operacion')

    return True
