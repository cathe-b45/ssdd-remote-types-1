import unittest
import uuid
import socket
import Ice
import os
from RemoteTypes import StopIteration, CancelIteration
from remotetypes.remoteset import RemoteSet
from remotetypes.remotelist import RemoteList
from remotetypes.remotedict import RemoteDict


def _get_available_port():
    """Encuentra un puerto disponible para evitar conflictos."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))  # Bind al puerto 0 para que el sistema asigne uno disponible.
        return s.getsockname()[1]  # Devuelve el puerto asignado


class TestIterable(unittest.TestCase):
    def setUp(self):
        """Setup the test environment for all types."""
        self.communicator = Ice.initialize([])

        # Usamos el puerto disponible como lo pides
        port = _get_available_port()
        self.adapter = self.communicator.createObjectAdapterWithEndpoints("TestAdapter", f"default -p {port}")
        self.adapter.activate()

        # Inicializamos los objetos RemoteSet, RemoteList y RemoteDict
        self.rset_id = str(uuid.uuid4())
        self.rset = RemoteSet(self.rset_id)
        self.rset.add("element1")
        self.rset.add("element2")
        self.rset.add("element3")

        self.rlist_id = str(uuid.uuid4())
        self.rlist = RemoteList(self.rlist_id, "test_rlist.json")
        self.rlist.append("element1")
        self.rlist.append("element2")
        self.rlist.append("element3")

        self.rdict_id = str(uuid.uuid4())
        self.rdict = RemoteDict(self.rdict_id, "test_rdict.json")
        self.rdict.setItem("key1", "value1")
        self.rdict.setItem("key2", "value2")
        self.rdict.setItem("key3", "value3")

        # Simulamos el adaptador en el contexto actual para las pruebas
        self.current_context = Ice.Current(adapter=self.adapter)

    def tearDown(self):
        """Clean up the test environment."""
        self.adapter.destroy()
        self.communicator.destroy()
        # Eliminar archivos de almacenamiento si existen
        if os.path.exists("test_rlist.json"):
            os.remove("test_rlist.json")
        if os.path.exists("test_rdict.json"):
            os.remove("test_rdict.json")

    def test_iterator(self):
        """Test iterador para RemoteSet, RemoteList y RemoteDict."""
        for collection, expected in [
            (self.rset, ["element1", "element2", "element3"]),
            (self.rlist, ["element1", "element2", "element3"]),
            (self.rdict, ["key1: value1", "key2: value2", "key3: value3"]),
        ]:
            # Crear un iterador para cada tipo
            iterator = collection.iter(current=self.current_context)  # Usar el contexto actual
            collected_elements = []
            try:
                while True:
                    collected_elements.append(iterator.next())
            except StopIteration:  # Se espera la excepción StopIteration al final
                pass
        self.assertEqual(sorted(collected_elements), sorted(expected))  # Ordena antes de comparar

    def test_iterator_stop_iteration(self):
        """Verifica que se lance StopIteration correctamente al final de la iteración."""
        iterator = self.rset.iter(current=self.current_context)
        try:
            while True:
                iterator.next()
        except StopIteration:
            pass  # Se espera que StopIteration se lance cuando se llega al final del iterador

    def test_iterator_cancel_iteration(self):
        """Verifica que se levante CancelIteration si se modifica el conjunto original."""
        iterator = self.rset.iter(current=self.current_context)
        self.rset.add("new_element")  # Modificar el conjunto original
        with self.assertRaises(CancelIteration):
            iterator.next()


if __name__ == "__main__":
    unittest.main()
