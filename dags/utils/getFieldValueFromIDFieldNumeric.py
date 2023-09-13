def get_value_from_id_field_numeric(FIELD_ID, custom_fields_del_ticket):
    obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == FIELD_ID), None)
    if obj_of_ticket is None:
        return None

    field_value_key = obj_of_ticket.get('value')
    return field_value_key
