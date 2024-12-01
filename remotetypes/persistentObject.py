import json
import os

"""
Este módulo contiene la clase PersistentObject, que sirve como clase base 
para estructuras de datos persistentes. Esta clase maneja la carga y 
almacenamiento de datos en un archivo y asegura que los datos se mantengan 
persistentes entre ejecuciones.
"""

class PersistentObject:
    """Clase base para estructuras de datos persistentes."""

    def __init__(self, identifier: str, storage_file: str):
        """
        Inicializa un objeto persistente con un identificador único y un archivo de almacenamiento.

        Args:
            identifier (str): Identificador único del objeto persistente.
            storage_file (str): Ruta del archivo donde se guardarán los datos persistentes.

        Inicializa el objeto cargando los datos del archivo de almacenamiento. Si no 
        existe un registro para el identificador, se crea uno nuevo e inicializa los datos.
        """
        
        self.id_ = identifier  # Identificador único
        self.storage_file = storage_file
        self._data = self._load_from_file()

        # Si no existe un registro para este identificador, lo inicializamos
        if self.id_ not in self._data:
            self._data[self.id_] = self._initialize_data()
            self._save_to_file()

    def _initialize_data(self):
        raise NotImplementedError("Este método debe ser implementado por la subclase.")

    def _load_from_file(self):
        """Carga datos desde el archivo JSON."""
        if os.path.exists(self.storage_file):
            with open(self.storage_file, 'r') as file:
                return json.load(file)
        return {}

    def _save_to_file(self):
        """Guarda datos en el archivo JSON."""
        with open(self.storage_file, 'w') as file:
            json.dump(self._data, file, indent=4)

    def _update_data(self):
        """Actualiza los datos en el archivo JSON."""
        self._data[self.id_] = self._storage_
        self._save_to_file()
