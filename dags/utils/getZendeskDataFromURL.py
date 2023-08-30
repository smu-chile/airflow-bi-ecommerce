import requests

def get_zendesk_data_from_url(url, API_KEY):
    data = {}
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return data
