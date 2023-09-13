import requests

def get_ticket_form_tipificacion(base_url, API_KEY):
    url = f'{base_url}ticket_forms/360005744174'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        data = data['ticket_form']
        return data
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return None
