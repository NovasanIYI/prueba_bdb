#!/bin/bash

# =============================================================================
# Script Orquestador del Proceso ETL
#
# Propósito:
#   Ejecuta el flujo completo de ETL para la consolidación de movimientos
#   de clientes. Simula un proceso batch automatizado.
#
# Uso:
#   Ejecutar desde la raíz del proyecto: ./run_etl.sh
# =============================================================================

# 'set -e' asegura que el script se detenga inmediatamente si cualquier
# comando falla, previniendo ejecuciones parciales o corruptas.
set -e

echo "--- [INICIO] Orquestador ETL Batch ---"

# Se establece el directorio 'src' como el directorio de trabajo.
# Es una buena práctica para asegurar que las rutas relativas dentro del script
# de Python se resuelvan correctamente.
echo "[PASO 1/2] Estableciendo directorio de trabajo..."
cd src

# Ejecuta el script principal de Python que contiene la lógica ETL.
echo "[PASO 2/2] Ejecutando script ETL principal..."
python etl_process.py

echo "--- [FIN] Orquestador ETL Batch Finalizado Exitosamente ---"