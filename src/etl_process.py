"""
Proceso ETL para la consolidación de movimientos de clientes.

Este script realiza un proceso de Extracción, Transformación y Carga (ETL) para
integrar datos de clientes desde una base de datos PostgreSQL y sus transacciones
financieras desde un archivo CSV.

El proceso consiste en:
1.  **Extracción**: Lee los datos de las dos fuentes (PostgreSQL y CSV).
2.  **Transformación**:
    - Cruza la información de clientes y transacciones.
    - Aplica reglas de negocio para limpiar los datos, descartando registros inválidos
      (ej. montos negativos, transacciones de clientes no existentes).
3.  **Carga**: Guarda el conjunto de datos consolidado y limpio en un nuevo archivo CSV.

Requiere un archivo .env en la raíz del proyecto para las credenciales de la BD.
"""
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Carga las variables de entorno para el manejo seguro de credenciales.
load_dotenv()

# --- CONFIGURACIÓN ---
RUTA_SALIDA = '../movimientos_clientes.csv'

# ============================================================================
# === FASE 1: EXTRACCIÓN (EXTRACT) ===
# ============================================================================

print("Iniciando la fase de Extracción...")

# Extracción desde el archivo CSV de transacciones.
df_transacciones = pd.read_csv('../data/transacciones.csv')
print("  - Datos de transacciones cargados.")

# Construcción de la cadena de conexión a la BD usando variables de entorno
# para no exponer credenciales en el código fuente.
db_password = os.getenv("DB_PASSWORD")
if not db_password:
    print("Error: La variable de entorno DB_PASSWORD no está definida.")
    sys.exit()

cadena_conexion = f'postgresql://postgres:{db_password}@localhost:5432/bdb_challenge'

# Extracción desde la base de datos PostgreSQL.
try:
    engine = create_engine(cadena_conexion)
    df_clientes = pd.read_sql('SELECT * FROM clientes', engine)
    print("  - Datos de clientes cargados desde la base de datos.")
except Exception as e:
    print(f"Error fatal al conectar o leer la base de datos: {e}")
    sys.exit() # Detiene la ejecución si la conexión a la BD falla.

# ============================================================================
# === FASE 2: TRANSFORMACIÓN (TRANSFORM) ===
# ============================================================================

print("\nIniciando la fase de Transformación...")

# Enriquecimiento de datos: Se realiza un left join para cruzar transacciones
# con los datos de los clientes. Se usa 'left' para conservar todas las
# transacciones y poder identificar posteriormente aquellas sin un cliente válido.
df_merged = pd.merge(df_transacciones, df_clientes, on='cliente_id', how='left')
print("  - Enriquecimiento de datos completado (join).")

# Limpieza de datos aplicando reglas de negocio.
# Regla 1: Descartar transacciones con montos no positivos.
df_limpio = df_merged[df_merged['monto'] > 0].copy()

# Regla 2: Descartar transacciones de clientes que no existen en la BD.
# Se identifican porque las columnas del cliente ('nombre') son nulas tras el join.
df_limpio = df_limpio[df_limpio['nombre'].notna()].copy()
print("  - Limpieza de datos inválidos completada.")

# ============================================================================
# === FASE 3: CARGA (LOAD) ===
# ============================================================================

print("\nIniciando la fase de Carga...")

# Se guarda el DataFrame limpio en un archivo CSV.
# Se usa index=False para evitar que Pandas escriba una columna extra con el índice.
df_limpio.to_csv(RUTA_SALIDA, index=False)

print(f"--- PROCESO ETL COMPLETADO EXITOSAMENTE ---")
print(f"Los datos consolidados han sido guardados en: {RUTA_SALIDA}")