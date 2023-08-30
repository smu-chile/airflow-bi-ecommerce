def get_id_tienda(a):
    if a in ['', 'Sin Tienda']:
        return None
    if a is None:
        return None
    return a[:4]
