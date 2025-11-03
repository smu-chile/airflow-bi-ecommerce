SELECT du.user_profile_id, du.email, du.nombre, du.apellido, du.documento 
FROM analytics_and_growth.perfil_usuario pu 
INNER JOIN analytics_and_growth.detalle_usuario du ON pu.user_profile_id = du.user_profile_id
WHERE du.documento in (select document from catalogo.usuarios_rugby)
GROUP BY 1,2,3,4,5;