def get_value_from_key(FIELD_ID, custom_fields_del_ticket, ticket_fields_endpoint):
    obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == FIELD_ID), None)
    if obj_of_ticket is None:
        return None

    field_value_key = obj_of_ticket['value']
    if field_value_key is None or field_value_key == '':
        return None

    buscar_id_en_fields = next((e for e in ticket_fields_endpoint if e['id'] == FIELD_ID), None)
    if buscar_id_en_fields is None:
        return None

    custom_field_options = buscar_id_en_fields.get('custom_field_options')
    if not custom_field_options:
        print(f'FIELD_ID: {FIELD_ID}     field_value_key: {field_value_key}')
        return None

    VALOR_DEL_CAMPO_DEL_TICKET = next((e for e in custom_field_options if e['value'] == field_value_key), None)
    return VALOR_DEL_CAMPO_DEL_TICKET['name'] if VALOR_DEL_CAMPO_DEL_TICKET else None
