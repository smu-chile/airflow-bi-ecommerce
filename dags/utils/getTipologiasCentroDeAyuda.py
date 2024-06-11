def get_tipologias_centro_de_ayuda(custom_fields_del_ticket, ticket_fields_endpoint, object_of_helpers):
    array_motivo_childs = object_of_helpers['array_motivo_childs']

    array_submotivos_fields = object_of_helpers['array_submotivos_fields']
    array_subtipo_field_ids = object_of_helpers['array_subtipo_field_ids']

    tipologias = {}
    FIELD_ID_MOTIVO = 8485295910039
    motivo_types = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == FIELD_ID_MOTIVO), [])
    motivo_obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == FIELD_ID_MOTIVO), {})
    motivo_key = motivo_obj_of_ticket.get('value', None)
    MOTIVO_DEL_TICKET = next((e['name'] for e in motivo_types if e['value'] == motivo_key), None)
    tipologias["motivo"] = MOTIVO_DEL_TICKET

    inheritance_motivo = next((e for e in array_motivo_childs if e.get('value', None) == motivo_key), {})

    if inheritance_motivo and isinstance(inheritance_motivo, dict):
        ID_FIELD_SUBTIPO1 = inheritance_motivo.get('child_fields_subtipo', {}).get('id', None)
    else:
        ID_FIELD_SUBTIPO1 = None



    subtipo_1_obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == ID_FIELD_SUBTIPO1), {})
    subtipo1_key = subtipo_1_obj_of_ticket.get('value', None)

    if subtipo1_key == "calidad_":
        tipologias["tipo1"] = 'Calidad'
        subtipo_2_calidad_deprecado = next((e for e in custom_fields_del_ticket if e['id'] == 6077463343255), {})
        subtipo2_key_calidad_deprecado = subtipo_2_calidad_deprecado.get('value', None)
        subtipo2_types_calidad_deprecado = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == 6077463343255), [])
        campo_tipo2_calidad_deprecado = next((e for e in subtipo2_types_calidad_deprecado if e['value'] == subtipo2_key_calidad_deprecado), {})
        tipo2_calidad_deprecado = campo_tipo2_calidad_deprecado.get('name', None)
        tipologias["tipo2"] = tipo2_calidad_deprecado
        tipologias["tipo3"] = None
        return tipologias

    if subtipo1_key is None:
        tipologias["tipo1"] = None
        tipologias["tipo2"] = None
        tipologias["tipo3"] = None
        return tipologias

    subtipo1_types = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == ID_FIELD_SUBTIPO1), [])
    campo_tipo1_del_ticket = next((e for e in subtipo1_types if e['value'] == subtipo1_key), {})
    TIPO1_DEL_TICKET = campo_tipo1_del_ticket.get('name', None)
    tipologias["tipo1"] = TIPO1_DEL_TICKET

    caracteristicas_de_los_tipos1 = next((e for e in array_submotivos_fields if e['id'] == ID_FIELD_SUBTIPO1), {})

    tipo1_buscado = next((e for e in caracteristicas_de_los_tipos1.get('subtipos', []) if e.get('value', None) == subtipo1_key), None)


    if tipo1_buscado is None:
        tipologias["tipo2"] = None
        tipologias["tipo3"] = None
        return tipologias

    array_de_hijos = tipo1_buscado.get('child_fields', [])

    if not (array_de_hijos and isinstance(array_de_hijos, list)):
        tipologias["tipo2"] = None
        tipologias["tipo3"] = None
        return tipologias

    ids_de_hijos = [e['id'] for e in array_de_hijos]
    id_de_hijo_subtipo = [e for e in ids_de_hijos if e in array_subtipo_field_ids and e != 6651184142999]

    if not id_de_hijo_subtipo:
        tipologias["tipo2"] = None
        tipologias["tipo3"] = None
        return tipologias

    ID_FIELD_TIPO2 = id_de_hijo_subtipo[0]
    subtipo_2_obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == ID_FIELD_TIPO2), {})
    subtipo2_key = subtipo_2_obj_of_ticket.get('value', None)


    if subtipo2_key == "demora_en_devolucion":
        tipologias["tipo2"] = 'Demora en Devolución'
        subtipo_3_demora = next((e for e in custom_fields_del_ticket if e['id'] == 7243378072471), {})
        subtipo3_key_demora = subtipo_3_demora.get('value', None)
        subtipo3_types_demora = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == 7243378072471), [])
        campo_tipo3_demora = next((e for e in subtipo3_types_demora if e['value'] == subtipo3_key_demora), {})
        tipo3_demora = campo_tipo3_demora.get('name', None)
        tipologias["tipo3"] = tipo3_demora
        return tipologias

    subtipo2_types = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == ID_FIELD_TIPO2), [])
    campo_tipo2_del_ticket = next((e for e in subtipo2_types if e['value'] == subtipo2_key), {})
    TIPO2_DEL_TICKET = campo_tipo2_del_ticket.get('name', None)
    tipologias["tipo2"] = TIPO2_DEL_TICKET

    caracteristicas_de_los_tipos2 = next((e for e in array_submotivos_fields if e['id'] == ID_FIELD_TIPO2), {})
    tipo2_buscado = next((e for e in caracteristicas_de_los_tipos2.get('subtipos', []) if e.get('value', None) == subtipo2_key), None)

    array_de_hijos_2 = []  

    if tipo2_buscado is not None and 'child_fields' in tipo2_buscado:
        array_de_hijos_2 = tipo2_buscado['child_fields']

    if not (len(array_de_hijos_2) >0 and isinstance(array_de_hijos_2, list)):
        tipologias["tipo3"] = None
        return tipologias

    ids_de_hijos_2 = [e['id'] for e in array_de_hijos_2]
    id_de_hijo_subtipo_2 = [e for e in ids_de_hijos_2 if e in array_subtipo_field_ids]

    if not id_de_hijo_subtipo_2:
        tipologias["tipo3"] = None
        return tipologias

    ID_FIELD_TIPO3 = id_de_hijo_subtipo_2[0]
    subtipo_3_obj_of_ticket = next((e for e in custom_fields_del_ticket if e['id'] == ID_FIELD_TIPO3), {})
    subtipo3_key = subtipo_3_obj_of_ticket.get('value', None)
    subtipo3_types = next((e['custom_field_options'] for e in ticket_fields_endpoint if e['id'] == ID_FIELD_TIPO3), [])
    campo_tipo3_del_ticket = next((e for e in subtipo3_types if e['value'] == subtipo3_key), {})
    TIPO3_DEL_TICKET = campo_tipo3_del_ticket.get('name', None)
    tipologias["tipo3"] = TIPO3_DEL_TICKET

    return tipologias
