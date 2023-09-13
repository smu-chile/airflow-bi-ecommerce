def get_estado(a):
    status_map = {
        'solved': 'Solved',
        'closed': 'Closed',
        'hold': 'Hold',
        'pending': 'Pending',
        'open': 'Open'
    }
    return status_map.get(a, 'New')
