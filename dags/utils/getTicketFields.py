import requests

def get_ticket_fields(base_url, API_KEY):
    url = f'{base_url}ticket_fields'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        ticket_fields = data.get('ticket_fields', [])
        return ticket_fields
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return []
