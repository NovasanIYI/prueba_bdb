#!/bin/bash

# Este comando detiene el script si cualquier paso falla (set -e)
set -e

echo "--- [INICIO] Proceso ETL Batch ---"

# Paso 1: Navegar al directorio del script de Python.
# Es una buena práctica ejecutar los scripts desde su ubicación para
# asegurar que las rutas relativas (como '../data/transacciones.csv') funcionen siempre.
echo "[PASO 1/2] Navegando al directorio 'src'..."
cd src

# Paso 2: Ejecutar el script de ETL con Python.
echo "[PASO 2/2] Ejecutando el script etl_process.py..."
python etl_process.py

echo "--- [FIN] Proceso ETL Batch Finalizado Exitosamente ---"