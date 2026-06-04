import requests
import time

def get_metrics_by_ticket_id(ticket_id, base_url, API_KEY):
    url = f'{base_url}tickets/{ticket_id}/metrics'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {API_KEY}'
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 5))
                print(f"Rate limited (429). Retrying after {retry_after} seconds (Attempt {attempt+1}/{max_retries})")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()  # Raises an exception for 4xx and 5xx status codes
            data = response.json()
            metrics = data.get('ticket_metric')
            return metrics
        except requests.exceptions.RequestException as e:
            print('zendesk response error:', e)
            if attempt == max_retries - 1:
                break
            time.sleep(2)

    return None
