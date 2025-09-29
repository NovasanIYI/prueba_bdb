"""
File Watcher para el Proceso ETL.

Este script monitorea el directorio 'landing_zone'. Cuando un nuevo archivo .csv
es creado, invoca el proceso ETL principal para procesarlo.
"""
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
from src.etl_process import ejecutar_etl_completo # Importamos la función

class MyEventHandler(FileSystemEventHandler):
    """Manejador de eventos para la creación de nuevos archivos."""
    def on_created(self, event):
        if event.is_directory:
            return

        # Solo procesamos archivos CSV
        if event.src_path.lower().endswith('.csv'):
            logging.info(f"Nuevo archivo CSV detectado: {event.src_path}")
            # Llamamos a la lógica ETL, pasando la ruta del nuevo archivo
            ejecutar_etl_completo(event.src_path)
        else:
            logging.warning(f"Archivo no soportado ignorado: {event.src_path}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    path = "landing_zone"
    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)

    print(f"✅ Iniciando monitor en la carpeta: '{path}'")
    print("   El servicio está esperando nuevos archivos CSV...")
    print("   Presiona Ctrl+C para detener.")
    
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\n🛑 Monitor detenido por el usuario.")
    observer.join()