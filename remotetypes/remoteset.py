import json
import os
import Ice
import uuid
import RemoteTypes as rt


class RemoteSet(rt.RSet):
    """
    Representa un conjunto remoto con persistencia.

    Esta clase maneja un conjunto de elementos persistente, lo que permite
    realizar operaciones como agregar, eliminar, verificar la existencia y
    iterar sobre los elementos. Además, mantiene el estado persistente de
    los elementos en un archivo de almacenamiento global. Las modificaciones
    realizadas en el conjunto (como agregar o eliminar elementos) se reflejan
    en el archivo de almacenamiento para asegurar que los datos se mantengan
    consistentes entre sesiones.

    Atributos:
        identifier (str): Identificador único para el conjunto.
        data (set): Conjunto de datos que contiene los elementos del conjunto.
        _modification_count (int): Contador para rastrear el número de modificaciones realizadas.
        storage_file (str): Ruta al archivo que almacena los datos del conjunto de forma persistente.

    Métodos:
        __init__(identifier): Inicializa un conjunto remoto con un identificador único.
        _load_data(): Carga los datos del archivo de almacenamiento global.
        _save_global_storage(): Guarda los datos actuales en el archivo de almacenamiento global.
        add(item): Añade un elemento al conjunto si no existe.
        remove(item): Elimina un elemento del conjunto.
        contains(item): Verifica si un elemento está en el conjunto.
        length(): Devuelve la cantidad de elementos en el conjunto.
        iter(): Devuelve un iterador sobre los elementos del conjunto.
        hash(): Calcula un hash a partir del contenido del conjunto.
        pop(): Elimina y devuelve un elemento del conjunto.
    """

    GLOBAL_STORAGE_FILE = "remoteset_data.json"

    def __init__(self, identifier: str) -> None:
        """Inicializa un RemoteSet con un identificador único."""
        self.identifier = identifier
        self.data = set()
        self._modification_count = 0
        self.storage_file = self.GLOBAL_STORAGE_FILE

        # Cargar datos existentes del archivo global
        self._load_data()

    def _load_data(self) -> None:
        """Carga los datos desde el archivo global."""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, "r") as f:
                    storage = json.load(f)
                    # Cargar solo los datos para el identificador actual
                    if self.identifier in storage:
                        self.data = set(storage[self.identifier])
            except json.JSONDecodeError:
                pass  # Si el archivo está vacío o corrupto, ignorar

    def _save_global_storage(self) -> None:
        """Guarda los datos en el archivo global."""
        storage = {}
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, "r") as f:
                    storage = json.load(f)
            except json.JSONDecodeError:
                pass  # Si el archivo está vacío o corrupto, ignorar

        # Actualizar el almacenamiento con los datos actuales
        storage[self.identifier] = list(self.data)

        # Escribir el almacenamiento actualizado en el archivo
        with open(self.storage_file, "w") as f:
            json.dump(storage, f, indent=4)

    def add(self, item: str, current=None) -> None:
        """Añade un elemento al conjunto si no existe."""
        if item not in self.data:  # Evitar duplicados
            self.data.add(item)
            self._modification_count += 1  # Incrementar el contador de modificaciones
            self._save_global_storage()

    def remove(self, item: str, current=None) -> None:
        """Elimina un elemento del conjunto."""
        if item not in self.data:
            raise rt.KeyError(f"El elemento '{item}' no existe en el conjunto.")
        self.data.remove(item)
        self._modification_count += 1  # Incrementar el contador de modificaciones
        self._save_global_storage()

    def contains(self, item: str, current=None) -> bool:
        """Verifica si un elemento está en el conjunto."""
        return item in self.data

    def length(self, current=None) -> int:
        """Devuelve la cantidad de elementos en el conjunto."""
        return len(self.data)

    def iter(self, current=None):
        """Devuelve un iterador sobre los elementos del conjunto."""
        if current is None:
            raise RuntimeError("El objeto 'current' es necesario.")
        adapter = current.adapter
        iterator = RemoteSetIterator(self)
        identity = Ice.Identity(name=str(uuid.uuid4()))
        proxy = adapter.add(iterator, identity)
        return rt.IterablePrx.checkedCast(proxy)

    def hash(self, current=None) -> int:
        """Calcula un hash a partir del contenido del conjunto."""
        return hash(frozenset(self.data))

    def pop(self, current=None) -> str:
        """Elimina y devuelve un elemento del conjunto."""
        if not self.data:
            raise rt.KeyError("El conjunto está vacío.")
        item = self.data.pop()
        self._modification_count += 1  # Incrementar el contador de modificaciones
        self._save_global_storage()
        return item


class RemoteSetIterator(rt.Iterable):
    """
    Iterador para el conjunto remoto (RemoteSet).

    Esta clase proporciona la funcionalidad para iterar sobre los elementos
    de un conjunto remoto, gestionando el estado de la iteración y manejando
    excepciones como `StopIteration` y `CancelIteration` si se realizan modificaciones
    al conjunto durante la iteración.

    Atributos:
        _iterator (iter): Iterador que recorre los elementos del conjunto.
        _expected_mod_count (int): Contador de modificaciones esperado para
        verificar la consistencia durante la iteración.

    Métodos:
        __init__(remote_set): Inicializa el iterador con el conjunto remoto y el contador de modificaciones esperado.
        next(): Devuelve el siguiente elemento en la iteración.
                Lanza una excepción `StopIteration` cuando termina la iteración
                y `CancelIteration` si el conjunto fue modificado durante la iteración.
    """

    def __init__(self, remote_set):
        """Inicializa el iterador."""
        self._iterator = iter(remote_set.data)

    def next(self, current=None):
        """Devuelve el siguiente elemento o lanza una excepción al terminar."""
        try:
            next_item = next(self._iterator)
            return next_item
        except StopIteration:
            raise rt.StopIteration()
        except RuntimeError:
            raise rt.CancelIteration()
