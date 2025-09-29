"""
Módulo principal de la lógica ETL para la consolidación de movimientos de clientes.

Este script contiene la función principal que ejecuta el flujo de Extracción, 
Transformación y Carga. Puede ser invocado desde un orquestador o ejecutado 
directamente para pruebas.
"""
import pandas as pd
from sqlalchemy import create_engine
import os
import sys
from dotenv import load_dotenv

# Carga las variables de entorno del archivo .env que debe estar en la raíz del proyecto.
load_dotenv()

def ejecutar_etl_completo(ruta_csv_transacciones):
    """
    Ejecuta el flujo ETL completo para un archivo de transacciones dado.
    
    Args:
        ruta_csv_transacciones (str): La ruta al archivo CSV de transacciones a procesar.
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
        return # Termina la función si el archivo no existe.

    # Lee la contraseña de forma segura desde las variables de entorno.
    db_password = os.getenv("DB_PASSWORD")
    if not db_password:
        print("Error fatal: La variable de entorno DB_PASSWORD no está definida en el archivo .env.")
        return

    cadena_conexion = f'postgresql://postgres:{db_password}@localhost:5432/bdb_challenge'
    
    try:
        engine = create_engine(cadena_conexion)
        df_clientes = pd.read_sql('SELECT * FROM clientes', engine)
    except Exception as e:
        print(f"Error fatal al conectar o leer la base de datos: {e}")
        return # Termina la función si la conexión a la BD falla.

    # ============================================================================
    # === FASE 2: TRANSFORMACIÓN (TRANSFORM) ===
    # ============================================================================
    
    # Enriquecimiento de datos uniendo transacciones y clientes.
    df_merged = pd.merge(df_transacciones, df_clientes, on='cliente_id', how='left')
    
    # Limpieza - Regla 1: Descartar transacciones con montos no positivos.
    df_limpio = df_merged[df_merged['monto'] > 0].copy()
    
    # Limpieza - Regla 2: Descartar transacciones de clientes que no existen.
    df_limpio = df_limpio[df_limpio['nombre'].notna()].copy()

    # ============================================================================
    # === FASE 3: CARGA (LOAD) ===
    # ============================================================================

    # Genera un nombre de archivo de salida dinámico.
    nombre_archivo_salida = os.path.basename(ruta_csv_transacciones).replace('.csv', '_procesado.csv')
    # Guarda el archivo en la carpeta raíz del proyecto.
    ruta_salida = f"../{nombre_archivo_salida}" 
    
    df_limpio.to_csv(ruta_salida, index=False)
    
    print(f"--- Proceso ETL Completado. Resultado guardado en: {ruta_salida} ---")


# El bloque __main__ permite que este script siga siendo ejecutable por sí mismo.
# Es útil para realizar pruebas rápidas sin necesidad de usar el orquestador.
if __name__ == "__main__":
    
    # Define la ruta del archivo de prueba por defecto.
    ruta_prueba = 'data/transacciones.csv'
    # Ajusta la ruta para que funcione al ejecutar el script desde la carpeta 'src'.
    ruta_prueba_ajustada = f"../{ruta_prueba}"
    
    print("Ejecutando script en modo de prueba independiente...")
    ejecutar_etl_completo(ruta_prueba_ajustada)