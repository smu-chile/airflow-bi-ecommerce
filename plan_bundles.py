# Verify table existence
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
pg = PostgresHook(postgres_conn_id="postgresql_conn")
df = pg.get_pandas_df("SELECT * FROM ecommdata.sku_bundles_retornables LIMIT 5")
print(df)
