#!/usr/bin/env python
# coding: utf-8

# ## Código compilado



import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import text

# postgresql AWS connections
# Janis products-store
conn = psycopg2.connect(
    host="bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com",
    database="postgres",
    user="msegura",
    password="DB2971-FlV")

query = """
select ref_id, id_tienda 
from ecommdata.productos_tienda pt;
"""

df_productos_janis_tienda = pd.read_sql(query, con=conn)

# lista 8 products that doesn't exist in Janis

query = """
select distinct LPAD(l.material::text, 18, '0')||'-'||l.umv as ref_id
from ecommdata.lista8 l
left join ecommdata.productos p 
    on p.ref_id = LPAD(l.material::text, 18, '0')||'-'||l.umv
where (p.ref_id is null) and "fecha" = (select max("fecha") from ecommdata.lista8)
;
"""

df_not_in_janis = pd.read_sql(query, con=conn)

# lista 8

query = """
select l.material||'-'||l.umv as ref_id, l.id_tienda 
from ecommdata.lista8 l
left join catalogo.productos_excluidos pe
    on pe.material||'-'||pe.umv = l.material||'-'||l.umv
where pe.material is null
union
select pc.ref_id, pc.id_tienda
from ecommdata.publicacion_catalogo pc
where pc.mfc is true and pc.fecha_hora = ''
;
"""

df_lista8 = pd.read_sql(query, con=conn)

# exclusions

query = """
select pe.material, pe.umv
from catalogo.productos_excluidos pe
left join ecommdata.skus s
    on s.ref_id = pe.material||'-'||pe.umv
where s.ref_id is not null
;
"""

df_exclusions = pd.read_sql(query, con=conn)

# Janis active stores

query = """
select id 
from ecommdata.tiendas
where status = 1
;
"""

series_active_stores = pd.read_sql(query, con=conn).iloc[:,0]

conn.close()

# df_lista8['ref_id'] = df_lista8['material']+'-'+df_lista8['umv']
# df_lista8.drop(columns=['material','umv','fecha'],inplace=True)
df_lista8 = df_lista8[df_lista8['id_tienda'].isin(series_active_stores)]
del(series_active_stores)

df_exclusions['ref_id'] = df_exclusions['material']+'-'+df_exclusions['umv']
df_exclusions.drop(columns=['material','umv'],inplace=True)

# this ref_id's must be deactivated on Janis (NaNs in the right side of the merged DF)
df_deact = df_productos_janis_tienda.merge(df_lista8,how='left',on='ref_id')
df_deact = df_deact[df_deact['id_tienda_y'].isna()]
df_deact = pd.concat([df_exclusions, df_deact])
del(df_exclusions)

# products to deactivate
series_deact = pd.Series(df_deact.loc[:,'ref_id'].unique())
del(df_deact)

# products from Janis that have suffered any modification in lista 8
df = pd.concat([df_productos_janis_tienda, df_lista8])
del(df_productos_janis_tienda)

#exclude products that doesn't exist in Janis
df = df.merge(df_not_in_janis,how='left',on='ref_id',indicator=True)
df = df[df['_merge']!='both'][['ref_id','id_tienda']].reset_index(drop=True)
del(df_not_in_janis)

# gb for comparing if a product-store pair changes
df_gpby = df.groupby(list(df.columns))
# product-store pairs that have changed (only 1 occurence considering both tables)
idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
df_changes = df.reindex(idx)
del(df_gpby)
del(idx)
del(df)
# create DF with ref-id's that have changed and exclude the rest
df_changes = df_changes.loc[~df_changes['ref_id'].isin(series_deact)]
series_changes = pd.Series(df_changes['ref_id'].unique())
del(df_changes)

# refId's to change stores
df_lista8_changes = df_lista8.loc[df_lista8['ref_id'].isin(series_changes)]
del(series_changes)
del(df_lista8)
# refId-stores long_to_wide
df_lista8_changes.loc[:,'idx'] = df_lista8_changes.groupby(['ref_id']).cumcount()
df_changes_final = df_lista8_changes.pivot_table(index=['ref_id'], columns='idx', 
                    values=['id_tienda'], aggfunc='first')
# rename columns in the format id_tienda_x
df_changes_final = df_changes_final.sort_index(axis=1, level=1)
df_changes_final.columns = [f'{x}_{y}' for x,y in df_changes_final.columns]
df_changes_final = df_changes_final.reset_index()
del(df_lista8_changes)

#create new column 'tiendas' that contains all the stores per refId separated by comma
cols = df_changes_final.filter(like='id_tienda_').columns

df_changes_final['tiendas'] = df_changes_final[cols].agg(lambda s: s.dropna().str.cat(sep=','), axis=1)
df_changes_final.drop(columns=cols, inplace=True)
del(cols)

# formatting output
df_changes_final["excludedStores"] = ''
df_changes_final["publish"] = 1
df_changes_final["updatePending"] = 1
df_changes_final.rename(columns={"ref_id":"refId","tiendas":"stores"}, inplace=True)
df_changes_final["date"] = pd.to_datetime('today')

# generate file
#df_changes_final.to_excel('cambio_tiendas_Janis_{}.xlsx'.format(time.strftime("%d-%m-%Y_%H-%M")),index=False)
#del(df_changes_final)

#create DF with products to be deactivated
df_deact_prods = pd.DataFrame(series_deact,columns=["refId"])
del(series_deact)

# formatting output
df_deact_prods["updatePending"] = 1
df_deact_prods["visible"] = 0
df_deact_prods["active"] = 0
df_deact_prods["date"] = pd.to_datetime('today')

# generate file
#df_deact_prods.to_excel('productos_desactivar_{}.xlsx'.format(time.strftime("%d-%m-%Y_%H-%M")),index=False)
#del(df_deact_prods)

# write tables into DB
host = "bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com"
database = "postgres"
username = "msegura"
password = "DB2971-FlV"

conn_url = "postgresql+psycopg2://"+username+":"+password+"@"+host+":5432/"+database
engine = sqlalchemy.create_engine(conn_url)

df_list = [df_changes_final, df_deact_prods]
names = ['actualizacion_tiendas_por_producto_janis','desactivacion_productos_janis']

for i in [0,1]:
    # Save into PostgreSQL:
    
    connection = engine.connect()
    delete_query = "DELETE FROM catalogo.{}".format(names[i])
    connection.execute(text(delete_query))
    connection.close()
    
    df_list[i].to_sql(name=names[i],
                con=engine,         
                schema="catalogo",         
                if_exists='append',         
                index=False,         
                chunksize=20000,         
                method='multi')

    print("Data saved to PostgreSQL.")

engine.dispose()
del(df_changes_final)
del(df_deact_prods)
del(df_list)
del(names)


