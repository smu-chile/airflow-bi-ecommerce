import requests

def get_metrics_by_ticket_id(ticket_id, base_url, API_KEY):
    url = f'{base_url}tickets/{ticket_id}/metrics'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        metrics = data.get('ticket_metric')
        return metrics
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return None
