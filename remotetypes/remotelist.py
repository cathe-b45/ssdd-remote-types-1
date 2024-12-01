import json
import os
from typing import Optional
import Ice
import RemoteTypes as rt
import uuid


class RemoteList(rt.RList):
    """
    Implementación de la interfaz remota RList con persistencia.

    Esta clase implementa la interfaz `RList` de ZeroC Ice, proporcionando una
    estructura de datos de lista persistente. Los elementos de la lista se almacenan
    de manera persistente en un archivo JSON, lo que permite que los datos sean
    preservados entre sesiones. La clase soporta operaciones comunes de listas como
    agregar, eliminar, verificar existencia y obtener elementos, así como iteración
    sobre los elementos de la lista.

    Atributos:
        id_ (str): Identificador único de la lista.
        storage_file (str): Ruta del archivo donde se almacenan los datos de la lista.
        _modification_count (int): Contador para llevar el registro de las modificaciones
                                   realizadas en la lista.
        _storage_ (list): Contenedor en memoria para los elementos de la lista.

    Métodos:
        __init__(identifier, storage_file): Inicializa un `RemoteList` con persistencia.
        _load_data(): Carga los datos desde el archivo JSON.
        _save_data(): Guarda los datos en el archivo JSON.
        identifier(current): Devuelve el identificador del objeto.
        append(item, current): Añade un elemento al final de la lista.
        remove(item, current): Elimina un elemento de la lista.
        contains(item, current): Verifica si un elemento está en la lista.
        length(current): Devuelve el número de elementos en la lista.
        hash(current): Calcula un hash a partir del contenido de la lista.
        getItem(index, current): Devuelve el elemento en una posición específica.
        pop(index, current): Elimina y devuelve un elemento de la lista.
        iter(current): Devuelve un iterador para la lista.
    """

    def __init__(self, identifier: str, storage_file: str) -> None:
        """Inicializa un RemoteList con persistencia."""
        self.id_ = identifier
        self.storage_file = storage_file
        self._modification_count = 0  # Contador para controlar las modificaciones
        self._storage_ = self._load_data()

    def _load_data(self) -> list:
        """Carga los datos desde el archivo JSON."""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as file:
                    data = json.load(file)
                    return data.get(self.id_, [])
            except json.JSONDecodeError:
                # Si el archivo está vacío o corrupto, inicializar como lista vacía
                return []
        return []

    def _save_data(self) -> None:
        """Guarda los datos en el archivo JSON."""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as file:
                    try:
                        data = json.load(file)
                    except json.JSONDecodeError:
                        data = {}
            else:
                data = {}

            data[self.id_] = self._storage_

            with open(self.storage_file, 'w') as file:
                json.dump(data, file, indent=4)
        except Exception as e:
            raise RuntimeError(f"Error al guardar los datos: {e}")

    def identifier(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el identificador del objeto."""
        return self.id_

    def append(self, item: str, current: Optional[Ice.Current] = None) -> None:
        """Añade un elemento al final de la lista."""
        self._storage_.append(item)
        self._modification_count += 1
        self._save_data()

    def remove(self, item: str, current: Optional[Ice.Current] = None) -> None:
        """Elimina un elemento de la lista."""
        try:
            self._storage_.remove(item)
            self._modification_count += 1
            self._save_data()
        except ValueError as error:
            raise rt.KeyError(f"Item {item} not found in list") from error

    def contains(self, item: str, current: Optional[Ice.Current] = None) -> bool:
        """Verifica si un elemento está en la lista."""
        return item in self._storage_

    def length(self, current: Optional[Ice.Current] = None) -> int:
        """Devuelve el número de elementos en la lista."""
        return len(self._storage_)

    def hash(self, current: Optional[Ice.Current] = None) -> int:
        """Calcula un hash a partir del contenido de la lista."""
        return hash(tuple(self._storage_))

    def getItem(self, index: int, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el elemento en una posición específica."""
        try:
            return self._storage_[index]
        except IndexError as error:
            raise rt.IndexError(f"Index {index} is out of range") from error

    def pop(self, index: Optional[int] = None, current: Optional[Ice.Current] = None) -> str:
        """Elimina y devuelve un elemento de la lista."""
        try:
            if index is None or index is Ice.Unset:
                item = self._storage_.pop()
            else:
                item = self._storage_.pop(index)
            self._modification_count += 1
            self._save_data()
            return item
        except IndexError as error:
            raise rt.IndexError(f"Index {index} is out of range") from error

    def iter(self, current: Optional[Ice.Current] = None) -> rt.IterablePrx:
        """Devuelve un iterador para la lista."""
        if current is None:
            raise RuntimeError("El objeto 'current' es necesario.")
        adapter = current.adapter
        iterator = ListIterator(self, self._modification_count)
        identity = Ice.Identity(name=str(uuid.uuid4()))
        proxy = adapter.add(iterator, identity)
        return rt.IterablePrx.checkedCast(proxy)


class ListIterator(rt.Iterable):
    """Implementación del iterador para RemoteList."""

    def __init__(self, remote_list: RemoteList, expected_mod_count: int) -> None:
        """Inicializa el iterador con la lista y el contador de modificaciones esperado."""
        self._list = remote_list
        self._expected_mod_count = expected_mod_count
        self._iterator = iter(remote_list._storage_)

    def next(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el siguiente elemento en la iteración."""
        if self._expected_mod_count != self._list._modification_count:
            raise rt.CancelIteration()
        try:
            return next(self._iterator)
        except StopIteration:
            raise rt.StopIteration()
