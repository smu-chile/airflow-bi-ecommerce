import requests

def get_user_by_id(user_id, base_url, API_KEY):
    url = f'{base_url}users/{user_id}'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
        data = response.json()
        user = data.get('user', {})
        return user
    except requests.exceptions.RequestException as e:
        print('zendesk response error:', e)

    return {}
