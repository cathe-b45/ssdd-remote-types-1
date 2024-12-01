from typing import Optional
import Ice
import RemoteTypes as rt
import json
import os
import uuid


class RemoteDict(rt.RDict):
    """
    Implementación de la interfaz remota RDict con persistencia.

    Esta clase implementa la interfaz `RDict` de ZeroC Ice, proporcionando un diccionario
    persistente donde las claves y sus valores se almacenan de manera persistente en un
    archivo JSON. Los métodos permiten realizar operaciones comunes de diccionarios como
    agregar, eliminar, verificar la existencia de claves y obtener valores, así como iterar
    sobre las claves y valores. Las modificaciones en el diccionario son seguidas por un
    contador de modificaciones para asegurar la consistencia durante las iteraciones.

    Atributos:
        id_ (str): Identificador único del diccionario.
        storage_file (str): Ruta del archivo donde se almacenan los datos del diccionario.
        _storage_ (dict): Contenedor en memoria para las claves y valores del diccionario.
        _modification_count (int): Contador de modificaciones para verificar cambios durante la iteración.

    Métodos:
        __init__(identifier, storage_file): Inicializa un `RemoteDict` con persistencia.
        _load_data(): Carga los datos del archivo JSON.
        _save_data(): Guarda los datos en el archivo JSON.
        identifier(current): Devuelve el identificador del objeto.
        remove(key, current): Elimina una clave del diccionario.
        length(current): Devuelve el número de elementos en el diccionario.
        contains(key, current): Verifica si una clave está en el diccionario.
        hash(current): Calcula un hash a partir del contenido del diccionario.
        iter(current): Devuelve un iterador para el diccionario.
        setItem(key, item, current): Asigna un valor a una clave en el diccionario.
        getItem(key, current): Obtiene el valor asociado a una clave en el diccionario.
        pop(key, current): Elimina y devuelve el valor asociado a una clave en el diccionario.
    """

    def __init__(self, identifier: str, storage_file: str) -> None:
        """Inicializa un RemoteDict con un identificador y archivo de almacenamiento."""
        self.id_ = identifier
        self.storage_file = storage_file
        self._storage_ = self._load_data()
        self._modification_count = 0  # Contador para controlar las modificaciones

    def _load_data(self) -> dict:
        """Carga los datos del archivo JSON."""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as file:
                    data = json.load(file)
                return data  # Cargar directamente como diccionario
            except json.JSONDecodeError:
                # Manejar caso de archivo vacío o datos no válidos
                return {}
        return {}

    def _save_data(self) -> None:
        """Guarda los datos en el archivo JSON."""
        try:
            with open(self.storage_file, 'w') as file:
                json.dump(self._storage_, file, indent=4)
        except Exception as e:
            raise RuntimeError(f"Error al guardar los datos: {e}")

    def identifier(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el identificador del objeto."""
        return self.id_

    def remove(self, key: str, current: Optional[Ice.Current] = None) -> None:
        """Elimina una clave del diccionario si existe. De lo contrario, lanza una excepción remota."""
        try:
            del self._storage_[key]
            self._modification_count += 1  # Incrementa el contador de modificaciones
            self._save_data()
        except KeyError as error:
            raise rt.KeyError(key) from error

    def length(self, current: Optional[Ice.Current] = None) -> int:
        """Devuelve el número de elementos en el diccionario."""
        return len(self._storage_)

    def contains(self, key: str, current: Optional[Ice.Current] = None) -> bool:
        """Verifica si una clave está en el diccionario."""
        return key in self._storage_

    def hash(self, current: Optional[Ice.Current] = None) -> int:
        """Calcula un hash a partir del contenido del diccionario."""
        # Ordenamos las claves para asegurar la consistencia del hash
        items = tuple(sorted(self._storage_.items()))
        return hash(items)

    def iter(self, current: Optional[Ice.Current] = None) -> rt.IterablePrx:
        """Crea y devuelve un iterador para el diccionario."""
        if current is None:
            raise RuntimeError("El objeto 'current' es necesario.")
        adapter = current.adapter
        iterator = DictIterator(self, self._modification_count)  # Proporciona el contador esperado
        identity = Ice.Identity(name=str(uuid.uuid4()))
        proxy = adapter.add(iterator, identity)
        return rt.IterablePrx.checkedCast(proxy)

    def setItem(self, key: str, item: str, current: Optional[Ice.Current] = None) -> None:
        """Asigna un valor a una clave en el diccionario."""
        self._storage_[key] = item
        self._modification_count += 1  # Incrementa el contador de modificaciones
        self._save_data()

    def getItem(self, key: str, current: Optional[Ice.Current] = None) -> str:
        """Obtiene el valor asociado a una clave en el diccionario."""
        try:
            return self._storage_[key]
        except KeyError as error:
            raise rt.KeyError(key) from error

    def pop(self, key: str, current: Optional[Ice.Current] = None) -> str:
        """Elimina y devuelve el valor asociado a una clave en el diccionario."""
        try:
            value = self._storage_.pop(key)
            self._modification_count += 1  # Incrementa el contador de modificaciones
            self._save_data()
            return value
        except KeyError as error:
            raise rt.KeyError(key) from error


class DictIterator(rt.Iterable):
    """Implementación del iterador para RemoteDict."""

    def __init__(self, remote_dict: RemoteDict, expected_mod_count: int) -> None:
        """Inicializa el iterador con el diccionario y el contador de modificaciones esperado."""
        self._dict = remote_dict
        self._expected_mod_count = expected_mod_count
        # Iteramos sobre las claves del diccionario
        self._iterator = iter(remote_dict._storage_.items())

    def next(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el siguiente elemento en la iteración."""
        if self._expected_mod_count != self._dict._modification_count:
            raise rt.CancelIteration()

        try:
            key, value = next(self._iterator)
            return f"{key}: {value}"
        except StopIteration:
            raise rt.StopIteration()
