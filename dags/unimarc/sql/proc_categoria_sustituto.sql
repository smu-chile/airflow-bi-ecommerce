select LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv as ref_id,
    bool_and(l8.sustituto) as sustituto_total,
    case
        when bool_and(l8.sustituto) = false then pro.id_categoria
        else 48312581
    end as id_category
from ecommdata.lista8 l8
    left join ecommdata.productos pro on LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv = pro.ref_id
group by LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv,
    pro.id_categoria,
    l8.sustituto
having (
        bool_and(l8.sustituto) = true
        and LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv not in (
            select distinct ref_id
            from ecommdata.productos pro
            where pro.id_categoria = 48312581
        )
    )
    or (
        bool_and(l8.sustituto) = false
        and (
            LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv in (
                select distinct ref_id
                from ecommdata.productos pro
                where pro.id_categoria = 48312581
            )
        )
        and LPAD(cast(l8.material as text), 18, '0') || '-' || l8.umv not in (
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
    );