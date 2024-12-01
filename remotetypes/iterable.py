import Ice
import RemoteTypes as rt
from RemoteTypes import StopIteration
from typing import Optional, Any

class Iterable(rt.Iterable):
    """Clase base para iteradores."""

    def __init__(self, data_source: Any, get_mod_count: callable, expected_mod_count: int) -> None:
        """Inicializa el iterador base.

        Args:
            data_source (Any): La fuente de datos a iterar.
            get_mod_count (callable): Función para obtener el contador de modificaciones actual.
            expected_mod_count (int): Contador de modificaciones esperado al momento de la creación.

        """
        self._data_source = data_source
        self._get_mod_count = get_mod_count
        self._expected_mod_count = expected_mod_count
        self._iterator = iter(data_source)

    def next(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el siguiente elemento en la iteración.

        Raises:
            CancelIteration: Si el objeto iterado ha sido modificado.
            StopIteration: Si se han iterado todos los elementos.

        """
        if self._expected_mod_count != self._get_mod_count():
            raise rt.CancelIteration("El objeto ha sido modificado durante la iteración.")

        try:
            return next(self._iterator)
        except StopIteration:
            raise rt.StopIteration("No hay más elementos.")

class ListIterator(BaseIterator):
    """Iterador para RemoteList."""

    def __init__(self, remote_list: 'RemoteList') -> None:
        super().__init__(
            data_source=remote_list._storage_,
            get_mod_count=lambda: remote_list._modification_count,
            expected_mod_count=remote_list._modification_count
        )


class SetIterator(Iterable):
    """Iterador para RemoteSet."""

    def __init__(self, remote_set: 'RemoteSet') -> None:
        """Inicializa el iterador para RemoteSet.

        Args:
            remote_set (RemoteSet): El conjunto remoto a iterar.

        """
        super().__init__(
            data_source=remote_set._storage_,
            get_mod_count=lambda: remote_set._modification_count,
            expected_mod_count=remote_set._modification_count
        )



class DictIterator(rt.Iterable):
    """Iterador para RemoteDict."""

    def __init__(self, remote_dict: 'RemoteDict') -> None:
        self._dict = remote_dict
        self._get_mod_count = lambda: remote_dict._modification_count
        self._expected_mod_count = remote_dict._modification_count
        self._iterator = iter(remote_dict._storage_.items())

    def next(self, current: Optional[Ice.Current] = None) -> str:
        """Devuelve el siguiente elemento en la iteración.

        Raises:
            CancelIteration: Si el diccionario ha sido modificado.
            StopIteration: Si se han iterado todos los elementos.

        """
        if self._expected_mod_count != self._get_mod_count():
            raise rt.CancelIteration("El diccionario ha sido modificado durante la iteración.")

        try:
            key, value = next(self._iterator)
            return f"{key}: {value}"
        except StopIteration:
            raise rt.StopIteration("No hay más elementos en el diccionario.")
