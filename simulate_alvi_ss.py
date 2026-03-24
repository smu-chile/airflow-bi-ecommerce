
import pandas as pd
import numpy as np

def simulate_ss(avg_sales, weight):
    print(f"--- Simulación Alvi SS (Peso: {weight}) ---")
    print(f"Promedio Ventas (30d): {avg_sales}")
    
    # Lógica de etl_stock_seguridad_alvi.py
    # 1. cantidad = avg * 0.5
    cantidad = avg_sales * 0.5
    print(f"Paso 1 (cantidad * 0.5): {cantidad}")
    
    # 2. Piso de 2
    nuevo_ss_base = max(cantidad, 2)
    print(f"Paso 2 (Piso de 2): {nuevo_ss_base}")
    
    # 3. Aplicar Peso
    result = round(nuevo_ss_base * weight, 0)
    print(f"Paso 3 (Multiplicar por Peso {weight} y redondear): {result}")
    
    # 4. Tope de 200
    final = min(result, 200)
    print(f"Resultado Final Janis: {final}")
    return final

# Casos de prueba
simulate_ss(10, 0.1)
print("\n")
simulate_ss(10, 2.0)
print("\n")
simulate_ss(100, 0.1)
