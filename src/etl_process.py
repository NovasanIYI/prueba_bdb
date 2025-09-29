"""
Módulo principal de la lógica ETL para la consolidación de movimientos de clientes.
"""
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def ejecutar_etl_completo(ruta_csv_transacciones):
    """
    Ejecuta el flujo ETL completo para un archivo de transacciones dado,
    mostrando los resultados de cada paso de la transformación.
    """
    # ============================================================================
    # === FASE 1: EXTRACCIÓN (EXTRACT) ===
    # ============================================================================
    print("\n--- Iniciando Proceso ETL ---")
    print(f"Procesando archivo de entrada: {ruta_csv_transacciones}")
    
    try:
        df_transacciones = pd.read_csv(ruta_csv_transacciones)
    except FileNotFoundError:
        print(f"Error fatal: No se encontró el archivo de transacciones en '{ruta_csv_transacciones}'")
        return

    db_password = os.getenv("DB_PASSWORD")
    if not db_password:
        print("Error fatal: La variable de entorno DB_PASSWORD no está definida.")
        return

    cadena_conexion = f'postgresql://postgres:{db_password}@localhost:5432/bdb_challenge'
    
    try:
        engine = create_engine(cadena_conexion)
        df_clientes = pd.read_sql('SELECT * FROM clientes', engine)
    except Exception as e:
        print(f"Error fatal al conectar o leer la base de datos: {e}")
        return

    # ============================================================================
    # === FASE 2: TRANSFORMACIÓN (TRANSFORM) ===
    # ============================================================================
    
    # Enriquecimiento de datos
    df_merged = pd.merge(df_transacciones, df_clientes, on='cliente_id', how='left')
    print("\n--- Datos después del merge inicial ---")
    print(df_merged)

    # Limpieza - Regla 1: Descartar transacciones con montos no positivos.
    df_limpio = df_merged[df_merged['monto'] > 0].copy()
    print("\n--- Datos después de filtrar montos inválidos ---")
    print(df_limpio)
    
    # Limpieza - Regla 2: Descartar transacciones de clientes que no existen.
    df_limpio = df_limpio[df_limpio['nombre'].notna()].copy()
    print("\n--- DataFrame final transformado y limpio ---")
    print(df_limpio)

    # ============================================================================
    # === FASE 3: CARGA (LOAD) ===
    # ============================================================================

    nombre_archivo_salida = os.path.basename(ruta_csv_transacciones).replace('.csv', '_procesado.csv')
    ruta_salida = f"../{nombre_archivo_salida}" 
    
    df_limpio.to_csv(ruta_salida, index=False)
    
    print(f"\n--- Proceso ETL Completado. Resultado guardado en: {ruta_salida} ---")


if __name__ == "__main__":
    ruta_prueba_ajustada = "../data/transacciones.csv"
    print("Ejecutando script en modo de prueba independiente...")
    ejecutar_etl_completo(ruta_prueba_ajustada)