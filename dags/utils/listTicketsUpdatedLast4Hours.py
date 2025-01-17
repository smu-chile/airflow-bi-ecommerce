import requests

def get_tickets_updated_last_4_hours(desde, hasta, nombre_formato, numero_pagina, base_url, API_KEY):
    def brand_id(a):
        if a == 'unimarc':
            return 1500000283081
        elif a == 'alvi':
            return 1500003492021
        elif a == 'salute':
            return 28946849342359
        else:
            return ''

    query = f'type:ticket updated>{desde}Z updated<={hasta}Z brand_id:{brand_id(nombre_formato)} custom_field_360053290513:*'
    url = f'{base_url}search.json?page={numero_pagina}&query={query}'

    data = {}

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return data
