import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Carga las variables de entorno del archivo .env
load_dotenv()

# --- 1. EXTRACCIÓN ---
print("Leyendo archivo transacciones.csv...")
df_transacciones = pd.read_csv('../data/transacciones.csv')
print("Datos de transacciones cargados:")
print(df_transacciones)
print("-" * 30)

# Leemos la contraseña de forma segura desde las variables de entorno
db_password = os.getenv("DB_PASSWORD")
# Usamos un f-string para construir la cadena de conexión de forma segura
cadena_conexion = f'postgresql://postgres:{db_password}@localhost:5432/bdb_challenge'

try:
    engine = create_engine(cadena_conexion)
    print("Conectando a la base de datos...")
    df_clientes = pd.read_sql('SELECT * FROM clientes', engine)
    print("Datos de clientes cargados:")
    print(df_clientes)

except Exception as e:
    print(f"Error al conectar o leer la base de datos: {e}")

# --- 2. TRANSFORMACIÓN ---
print("\nIniciando transformación de datos...")

# Paso 2.1: Enriquecimiento de datos
df_merged = pd.merge(df_transacciones, df_clientes, on='cliente_id', how='left')
print("\n--- Datos después del merge inicial ---")
print(df_merged)

# Paso 2.2: Limpieza de datos
# Regla 1: Filtrar transacciones con montos inválidos.
df_limpio = df_merged[df_merged['monto'] > 0].copy()
print("\n--- Datos después de filtrar montos inválidos ---")
print(df_limpio)

# Regla 2: Filtrar transacciones de clientes que no existen.
df_limpio = df_limpio[df_limpio['nombre'].notna()].copy()
print("\n--- DataFrame final transformado y limpio ---")
print(df_limpio)

# --- 3. CARGA ---
print("\nIniciando fase de Carga...")
ruta_salida = '../movimientos_clientes.csv'
# Guardamos el resultado en un nuevo archivo CSV.
df_limpio.to_csv(ruta_salida, index=False)
print(f"Proceso completado. Los datos limpios se han guardado en: {ruta_salida}")