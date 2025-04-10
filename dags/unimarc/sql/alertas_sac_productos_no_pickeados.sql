with tablatemporal as (
SELECT
  CAST("source"."fecha_facturacion" AS date) AS "fecha_facturacion",
  "source"."fecha_picking" as "fecha_picking",
  "source"."ref_id" AS "ref_id",
  "source"."Material SAP" AS "Material SAP",
  "source"."descripcion" AS "descripcion",
  "source"."Marcas__nombre" AS "Marcas__nombre",
  "source"."categoria_n1" AS "categoria_n1",
  "source"."glosa" AS "glosa",
  SUM("source"."unidades_solicitadas") AS "Unidades Solicitadas",
  SUM("source"."unidades_pickeadas") AS "Unidades Pickeadas",
  SUM("source"."unidades_solicitadas") - SUM("source"."unidades_pickeadas") AS "Unidades No Pickeadas",
  count(distinct "source"."orden") AS "Pedidos Afectados"
FROM
  (
    SELECT
      "operaciones_unimarc"."found_rate_productos"."orden" AS "orden",
      "operaciones_unimarc"."found_rate_productos"."fecha_facturacion" AS "fecha_facturacion",
      "operaciones_unimarc"."found_rate_productos"."fecha_picking" as "fecha_picking",
      "operaciones_unimarc"."found_rate_productos"."ref_id" AS "ref_id",
      "operaciones_unimarc"."found_rate_productos"."descripcion" AS "descripcion",
      "operaciones_unimarc"."found_rate_productos"."categoria_n1" AS "categoria_n1",
      "operaciones_unimarc"."found_rate_productos"."unidades_solicitadas" AS "unidades_solicitadas",
      "operaciones_unimarc"."found_rate_productos"."unidades_pickeadas" AS "unidades_pickeadas",
      "operaciones_unimarc"."found_rate_productos"."estado_foundrate" AS "estado_foundrate",
      "operaciones_unimarc"."found_rate_productos"."glosa" AS "glosa",
      SUBSTRING("Productos"."ref_id", 1, 19) AS "Material SAP",
      "Marcas"."nombre" AS "Marcas__nombre",
      "Productos"."ref_id" AS "Productos__ref_id",
      "Productos"."id_marca" AS "Productos__id_marca",
      "Marcas"."id" AS "Marcas__id"
    FROM
      "operaciones_unimarc"."found_rate_productos"
LEFT JOIN "ecommdata"."productos" AS "Productos" ON "operaciones_unimarc"."found_rate_productos"."ref_id" = "Productos"."ref_id"
      LEFT JOIN "ecommdata"."marcas" AS "Marcas" ON "Productos"."id_marca" = "Marcas"."id" 
WHERE
      (
        (
          "operaciones_unimarc"."found_rate_productos"."estado_foundrate" = 1
        ) 
    OR(
          "operaciones_unimarc"."found_rate_productos"."estado_foundrate" = 2
        )
      )
   AND (
        "operaciones_unimarc"."found_rate_productos"."fecha_facturacion" >= CAST(NOW() AT TIME ZONE 'America/Santiago' AS date)
      )
      AND (
        "operaciones_unimarc"."found_rate_productos"."fecha_facturacion" < CAST((NOW() AT TIME ZONE 'America/Santiago'+ INTERVAL '1 day') AS date)
      )
      AND (
        "operaciones_unimarc"."found_rate_productos"."glosa" = '0442 - UNI LAUTARO COYHAIQUE'
      )
  ) AS "source"
GROUP BY
  CAST("source"."fecha_facturacion" AS date),
  "source"."fecha_picking",
  "source"."ref_id",
  "source"."Material SAP",
  "source"."descripcion",
  "source"."Marcas__nombre",
  "source"."categoria_n1",
  "source"."glosa"
ORDER BY
  CAST("source"."fecha_facturacion" AS date) DESC,
  "source"."ref_id" ASC,
  "source"."Material SAP" ASC,
  "source"."descripcion" ASC,
  "source"."Marcas__nombre" ASC,
  "source"."categoria_n1" ASC,
  "source"."glosa" ASC
)
select "fecha_picking",
descripcion,
sum("Unidades No Pickeadas") as "Unidades No Pickeadas",
sum("Pedidos Afectados") as "Pedidos Afectados"
from tablatemporal
where "fecha_picking" >= NOW() AT TIME ZONE 'America/Santiago' - INTERVAL '30 minutes' 
group by "fecha_picking", descripcion
order by "fecha_picking" desc, "Unidades No Pickeadas" desc,
"Pedidos Afectados" desc;