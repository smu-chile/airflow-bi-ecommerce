select p.ref_id as refId,
    att.valor as category,
    p.nombre as name
from ecommdata.lista8 l
    inner join ecommdata.productos p on p.ref_id = l.material || '-' || l.umv
    inner join ecommdata.atributos_producto att on att.ref_id = l.material || '-' || l.umv
where p.id_categoria = { id_sustitutive_category_id }
    and att.id_atributo = { id_atributo_idcategory }
    and l.material || '-' || l.umv not in (
        '000000000000761296-KG',
        '000000000000752499-KG',
        '000000000000542749-KG',
        '000000000000761299-KG',
        '000000000000752492-KG',
        '000000000000752501-KG',
        '000000000000752528-KG',
        '000000000000761291-KG',
        '000000000000761281-KG',
        '000000000000761292-KG',
        '000000000000761279-KG',
        '000000000000752510-KG',
        '000000000000761276-KG',
        '000000000000542743-KG',
        '000000000000752519-KG',
        '000000000000542758-KG',
        '000000000000761285-KG',
        '000000000000761294-KG',
        '000000000000752496-KG',
        '000000000000752531-KG',
        '000000000000761287-KG',
        '000000000000752486-KG',
        '000000000000542755-KG',
        '000000000000542752-KG',
        '000000000000752507-KG'
    )
    and p.id_categoria not in ({ id_category_static })
group by p.ref_id,
    p.id_categoria,
    att.valor,
    att.id_atributo,
    p.nombre
having not bool_and(l.sustituto);