import sqlalchemy
from airflow.models import Variable
import pandas as pd

def check_stores():
    try:
        host = Variable.get("POSTGRESQL_HOST")
        database = Variable.get("POSTGRESQL_DB")
        username = Variable.get("POSTGRESQL_USER")
        password = Variable.get("POSTGRESQL_PASSWORD")
        
        conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:5432/{database}"
        engine = sqlalchemy.create_engine(conn_url)
        
        query = "SELECT id, glosa, status FROM ecommdata_alvi.tiendas WHERE status = 1 ORDER BY id"
        df = pd.read_sql(query, engine)
        print("Stores in ecommdata_alvi.tiendas (status = 1):")
        print(df.to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_stores()
