import unittest
import uuid
import os
import socket
import Ice
import RemoteTypes as rt
from RemoteTypes import StopIteration, CancelIteration
from remotetypes.remoteset import RemoteSet


def _get_available_port():
    """Encuentra un puerto disponible para evitar conflictos."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))  # Bind al puerto 0 para que el sistema asigne uno disponible.
        return s.getsockname()[1]  # Devuelve el puerto asignado


class TestRemoteSet(unittest.TestCase):
    """Test suite para la clase RemoteSet.

    Esta clase contiene pruebas unitarias para la clase RemoteSet, las cuales prueban
    la funcionalidad de añadir, eliminar, y iterar sobre los elementos en un conjunto remoto.
    También verifica la persistencia y maneja casos límite como intentar eliminar elementos
    inexistentes.
    """

    def setUp(self):
        """Configura el entorno de prueba para RemoteSet."""
        port = _get_available_port()
        self.communicator = Ice.initialize([])
        self.adapter = self.communicator.createObjectAdapterWithEndpoints("TestAdapter", f"default -p {port}")
        self.adapter.activate()

        # Inicializamos los objetos RemoteSet
        self.identifier = str(uuid.uuid4())
        self.rset = RemoteSet(self.identifier)

        # Simulamos el adaptador en el contexto actual para las pruebas
        self.current_context = Ice.Current(adapter=self.adapter)

    def tearDown(self):
        """Limpia los archivos generados durante las pruebas."""
        if os.path.exists(RemoteSet.GLOBAL_STORAGE_FILE):
            os.remove(RemoteSet.GLOBAL_STORAGE_FILE)

    def test_add_item(self):
        """3.8.1: Añade un elemento al conjunto."""
        self.rset.add("item1")
        self.assertTrue(self.rset.contains("item1"))
        self.assertEqual(self.rset.length(), 1)

    def test_remove_item(self):
        """3.1: Borra un elemento existente."""
        self.rset.add("item1")
        self.rset.remove("item1")
        self.assertFalse(self.rset.contains("item1"))
        self.assertEqual(self.rset.length(), 0)

    def test_persistence(self):
        """3.3: Persiste los datos correctamente."""
        self.rset.add("item1")
        self.rset.add("item2")
        new_rset = RemoteSet(self.identifier)  
        self.assertTrue(new_rset.contains("item1"))
        self.assertTrue(new_rset.contains("item2"))
        self.assertEqual(new_rset.length(), 2)

    def test_remove_nonexistent_item(self):
        """3.2: Lanza KeyError si se intenta borrar un elemento inexistente."""
        with self.assertRaises(rt.KeyError):
            self.rset.remove("item1")

    def test_pop_empty_set(self):
        """3.10: Lanza KeyError si el conjunto está vacío."""
        with self.assertRaises(rt.KeyError):
            self.rset.pop()

    def test_add_duplicate(self):
        """3.8.2: No añade elementos duplicados."""
        self.rset.add("item1")
        self.rset.add("item1")  
        self.assertEqual(self.rset.length(), 1)

    def test_hash_unchanged(self):
        """3.6: El hash no cambia si no se modifica el conjunto."""
        self.rset.add("item1")
        original_hash = self.rset.hash()
        self.assertEqual(original_hash, self.rset.hash())

    def test_hash_changes(self):
        """3.7: El hash cambia si se modifica el conjunto."""
        self.rset.add("item1")
        original_hash = self.rset.hash()
        self.rset.add("item2")
        self.assertNotEqual(original_hash, self.rset.hash())

    def test_iterator(self):
        """Test iterador para RemoteSet."""
        self.rset.add("element1")
        self.rset.add("element2")
        self.rset.add("element3")

        iterator = self.rset.iter(current=self.current_context) 
        collected_elements = []

        try:
            while True:
                collected_elements.append(iterator.next())
        except StopIteration:
            pass 

        self.assertEqual(set(collected_elements), {"element1", "element2", "element3"})

    def test_iterator_stop_iteration(self):
        """Verifica que se lance StopIteration correctamente al final de la iteración."""
        iterator = self.rset.iter(current=self.current_context)
        with self.assertRaises(StopIteration):  # Verifica la excepción definida en el Slice
            while True:
                iterator.next()

    def test_iterator_cancel_iteration(self):
        """Verifica que se levante CancelIteration si se modifica el conjunto original."""
        iterator = self.rset.iter(current=self.current_context)
        self.rset.add("new_element")  # Modificar el conjunto original
        with self.assertRaises(CancelIteration):
            iterator.next()

if __name__ == "__main__":
    unittest.main()
