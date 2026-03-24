
import pandas as pd
import numpy as np

def simulate_old_failure():
    print("--- SIMULACIÓN DEL FALLO ORIGINAL (ANTES DEL FIX) ---")
    
    # 1. Simulación de los tipos de datos en el DAG original:
    # id_tienda de ventas solía venir como INT (desde productos_tienda o similar)
    id_tienda_ventas = 3092 
    
    # id_tienda de la matriz viene como STR (character varying de Postgres)
    id_tienda_matriz = "3092"
    
    print(f"Tienda en ventas: {id_tienda_ventas} (Tipo: {type(id_tienda_ventas)})")
    print(f"Tienda en matriz: '{id_tienda_matriz}' (Tipo: {type(id_tienda_matriz)})")
    
    # Crear DataFrames
    df_final = pd.DataFrame({'id_tienda': [id_tienda_ventas], 'nuevo_ss': [7.0]})
    df_matriz = pd.DataFrame({'id_tienda': [id_tienda_matriz], 'peso': [0.1]})

    print("\n--- Intento de Cruce (SIN el fix de astype(str)) ---")
    
    # Cruce original (FALLIDO)
    df_merge = df_final.merge(df_matriz, how='left', on='id_tienda')
    print(f"Resultado del Cruce:\n{df_merge}")

    # Aplicación del Peso sobre el NaN
    df_merge["final"] = round(df_merge["nuevo_ss"] * df_merge["peso"], 0)
    print(f"\n--- Resultado Final en Janis ---")
    
    peso_aplicado = df_merge["peso"].values[0]
    resultado_final = df_merge["final"].values[0]
    
    if pd.isna(peso_aplicado):
        print(f"PESO NO ENCONTRADO (NaN). El 0.1 de la matriz fue ignorado.")
        print(f"Cálculo: 7.0 * NaN = {resultado_final}")
    else:
        print(f"ÉXITO: Se aplicó el peso {peso_aplicado}")

if __name__ == "__main__":
    simulate_old_failure()
