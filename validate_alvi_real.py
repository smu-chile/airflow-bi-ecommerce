
import pandas as pd
import numpy as np
import sqlalchemy

# 1. Conexión a DB Real
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgresql:U0rPPKwqrVPjICuU@bi-ecommerce-postgres-prod-master.cuuchupawrpt.us-east-1.rds.amazonaws.com:5432/postgres')

def validate_real_data():
    print("--- VALIDACIÓN REAL ALVI SS ---")
    
    # Simular extracción de ventas (Paso 1 del DAG)
    # Tomamos un SKU real: 10005-UN
    avg_sales = 13.16
    id_tienda = "3092"
    ref_id = "10005-UN"

    # Lógica del DAG (Paso a Paso)
    print(f"Tienda: {id_tienda}, SKU: {ref_id}")
    print(f"Venta Promedio: {avg_sales}")
    
    base = avg_sales * 0.5
    print(f"1. Base (venta * 0.5): {base}")
    
    # Piso de 2
    nuevo_ss = max(base, 2)
    print(f"2. Con Piso de 2 (max(base, 2)): {nuevo_ss}")

    # Redondeo previo (lo que hace el DAG en la linea 156/295 aprox)
    nuevo_ss_rounded = round(nuevo_ss, 0)
    print(f"3. Redondeo Inicial (round(2,0)): {nuevo_ss_rounded}")

    # Cargar Matriz REAL desde la DB
    df_matriz = pd.read_sql(f"SELECT id_tienda, peso FROM catalogo.matriz_ss_alvi WHERE id_tienda = '{id_tienda}'", engine)
    print(f"4. Registro Matriz encontrado: \n{df_matriz}")

    # El Cruce (Lo que arreglamos con astype(str))
    df_final = pd.DataFrame({'id_tienda': [id_tienda], 'nuevo_ss': [nuevo_ss_rounded]})
    
    # Forzar tipos como hace el DAG corregido
    df_final["id_tienda"] = df_final["id_tienda"].astype(str)
    df_matriz["id_tienda"] = df_matriz["id_tienda"].astype(str)
    
    df_merge = df_final.merge(df_matriz, how='left', on='id_tienda')
    print(f"5. Resultado del Cruce:\n{df_merge}")

    # Aplicación del Peso
    df_merge["final"] = round(df_merge["nuevo_ss"] * df_merge["peso"], 0)
    print(f"6. Cálculo Final (nuevo_ss * peso y redondeo): {df_merge['final'].values[0]}")
    
    # Comprobar si quedó NaN o un Valor
    if pd.isna(df_merge["final"].values[0]):
        print("ERROR: El cálculo falló (NaN)")
    else:
        print(f"ÉXITO: El sistema calculó {df_merge['final'].values[0]} usando peso {df_merge['peso'].values[0]}")

if __name__ == "__main__":
    validate_real_data()
