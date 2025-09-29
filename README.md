# Kata de Data Engineer: Integración y Carga de Datos

Solución al reto técnico para la vacante de Ingeniero de Datos. El proyecto consiste en un proceso ETL completo que ha sido extendido con una funcionalidad de procesamiento automático de archivos en tiempo real.

## Descripción del Proceso

El proyecto implementa un flujo de Extracción, Transformación y Carga (ETL) que integra datos de clientes desde una base de datos PostgreSQL y un archivo plano (`.csv`) de transacciones. El proceso enriquece los datos y aplica reglas de negocio para limpiar registros inválidos antes de generar un archivo de salida consolidado.

## Herramientas Utilizadas
* **Base de Datos**: PostgreSQL
* **Lenguaje de Programación**: Python 3
* **Librerías Principales**: Pandas (para manipulación de datos), SQLAlchemy (para conexión a BD), python-dotenv (para manejo de secretos).
* **Orquestación y Automatización**:
  * **Batch**: Script de Shell (`run_etl.sh`)
  * **Watcher**: Librería `watchdog` de Python

## Cómo Ejecutar el Proyecto

### Requisitos Previos
1.  Tener un servidor de PostgreSQL en ejecución.
2.  Clonar este repositorio.
3.  Crear la base de datos y la tabla `clientes` ejecutando el script en la carpeta `sql/setup.sql`.
4.  Crear un archivo `.env` en la raíz del proyecto y definir la variable `DB_PASSWORD` con la contraseña de la base de datos.
5.  Instalar las librerías de Python necesarias:
    ```bash
    pip install pandas sqlalchemy psycopg2-binary python-dotenv watchdog
    ```

### Modo 1: Ejecución Batch (para Pruebas 🧪)
Este modo ejecuta el proceso una sola vez utilizando el archivo de ejemplo estático `data/transacciones.csv`. Es ideal para pruebas rápidas y verificar la lógica del ETL.

```bash
# Ejecutar desde la raíz del proyecto
./run_etl.sh