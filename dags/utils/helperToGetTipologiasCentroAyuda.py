import re

def helpers_to_get_tipologias_centro_ayuda(formulario_tipificacion, ticket_fields_endpoint):
    pattern = re.compile(r'[tT]ipo')
    herencias_tipificacion = formulario_tipificacion['agent_conditions']
    FIELD_ID_MOTIVO = 8485295910039
    types_of_motivo_objects = [e for e in herencias_tipificacion if e['parent_field_id'] == FIELD_ID_MOTIVO]

    for e in types_of_motivo_objects:
        subtipo_motivo = None
        for u in e['child_fields']:
            field_id = u['id']
            field = next((item for item in ticket_fields_endpoint if item['id'] == field_id), None)
            if field and pattern.search(field['title']):
                subtipo_motivo = u
                break
        e["child_fields_subtipo"] = subtipo_motivo

    subtipo_fields = [e for e in ticket_fields_endpoint if pattern.search(e['title'])]
    subtipo_field_id_title = [{'id': e['id'], 'title': e['title']} for e in subtipo_fields]
    subtipo_field_ids = [e['id'] for e in subtipo_field_id_title]

    inheritance_subtipos_array = []
    for e in subtipo_field_id_title:
        types_of_subtipo_objects = [a for a in herencias_tipificacion if a['parent_field_id'] == e['id']]
        inheritance_subtipos_array.append({'id': e['id'], 'title': e['title'], 'subtipos': types_of_subtipo_objects})

    return {
        'array_motivo_childs': types_of_motivo_objects,
        'array_submotivos_fields': inheritance_subtipos_array,
        'array_subtipo_field_ids': subtipo_field_ids
    }
