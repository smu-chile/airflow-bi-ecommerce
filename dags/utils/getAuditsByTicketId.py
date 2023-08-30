import requests

def get_audits_by_ticket_id(ticket_id, base_url, API_KEY):
    url = f'{base_url}tickets/{ticket_id}/audits'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    comentario = ''

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        comentario = data.get('audits', '')
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return comentario
