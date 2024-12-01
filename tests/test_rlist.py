import os
import unittest
import uuid
from remotetypes.remotelist import RemoteList
from RemoteTypes import KeyError as RemoteKeyError, IndexError as RemoteIndexError


ITEM = 'elemento_prueba'
OTRO_ITEM = 'otro_elemento'
ITEM_INVALIDO = 'elemento_invalido'
INDICE_INVALIDO = 10
TEMP_STORAGE_FILE = "test_remotelist.json"


class TestRemoteList(unittest.TestCase):
    """Casos de prueba para la clase RemoteList."""

    def setUp(self):
        """Configura el entorno de prueba."""
        # Limpiar archivo de persistencia antes de cada prueba
        if os.path.exists(TEMP_STORAGE_FILE):
            os.remove(TEMP_STORAGE_FILE)
        self.rlist = RemoteList(identifier=str(uuid.uuid4()), storage_file=TEMP_STORAGE_FILE)

    def tearDown(self):
        """Limpieza después de cada prueba."""
        if os.path.exists(TEMP_STORAGE_FILE):
            os.remove(TEMP_STORAGE_FILE)

    def test_remove_existing_item(self):
        """2.1 RList.remove borra un elemento por valor existente."""
        self.rlist.append(ITEM)
        self.rlist.remove(ITEM)
        self.assertEqual(self.rlist.length(), 0)

    def test_remove_nonexistent_item_raises_KeyError(self):
        """2.2 RList.remove lanza KeyError si el elemento no existe."""
        with self.assertRaises(RemoteKeyError):
            self.rlist.remove(ITEM_INVALIDO)

    def test_length_returns_correct_length(self):
        """2.3 RList.length devuelve la longitud correcta."""
        self.assertEqual(self.rlist.length(), 0)
        self.rlist.append(ITEM)
        self.assertEqual(self.rlist.length(), 1)

    def test_contains_returns_false_for_missing_item(self):
        """2.4 RList.contains devuelve False si no está el valor."""
        self.assertFalse(self.rlist.contains(ITEM))

    def test_contains_returns_true_for_existing_item(self):
        """2.5 RList.contains devuelve True si el valor existe."""
        self.rlist.append(ITEM)
        self.assertTrue(self.rlist.contains(ITEM))

    def test_hash_returns_same_value_when_unmodified(self):
        """2.6 RList.hash devuelve enteros iguales si no se modifica."""
        self.rlist.append(ITEM)
        valor_hash_inicial = self.rlist.hash()
        self.assertEqual(self.rlist.hash(), valor_hash_inicial)

    def test_hash_returns_different_value_when_modified(self):
        """2.7 RList.hash devuelve enteros diferentes si se modifica."""
        self.rlist.append(ITEM)
        valor_hash_inicial = self.rlist.hash()
        self.rlist.append(OTRO_ITEM)
        self.assertNotEqual(self.rlist.hash(), valor_hash_inicial)

    def test_append_adds_item_to_end(self):
        """2.8 RList.append añade un elemento al final."""
        self.rlist.append(ITEM)
        self.assertEqual(self.rlist.getItem(0), ITEM)

    def test_pop_returns_last_item(self):
        """2.9.1 RList.pop devuelve un elemento del final."""
        self.rlist.append(ITEM)
        resultado = self.rlist.pop()
        self.assertEqual(resultado, ITEM)

    def test_pop_removes_last_item(self):
        """2.9.2 RList.pop elimina el elemento del final."""
        self.rlist.append(ITEM)
        self.rlist.pop()
        self.assertEqual(self.rlist.length(), 0)

    def test_pop_with_index_returns_item(self):
        """2.10.1 RList.pop devuelve el elemento de la posición indicada."""
        self.rlist.append(ITEM)
        self.rlist.append(OTRO_ITEM)
        resultado = self.rlist.pop(0)
        self.assertEqual(resultado, ITEM)

    def test_pop_with_index_removes_item(self):
        """2.10.2 RList.pop elimina el elemento de la posición indicada."""
        self.rlist.append(ITEM)
        self.rlist.append(OTRO_ITEM)
        self.rlist.pop(0)
        self.assertEqual(self.rlist.getItem(0), OTRO_ITEM)

    def test_pop_with_invalid_index_raises_IndexError(self):
        """2.11 RList.pop lanza la excepción IndexError si la posición no existe."""
        with self.assertRaises(RemoteIndexError):
            self.rlist.pop(INDICE_INVALIDO)

    def test_getItem_returns_item_at_index(self):
        """2.12.1 RList.getItem devuelve el elemento de la posición indicada."""
        self.rlist.append(ITEM)
        resultado = self.rlist.getItem(0)
        self.assertEqual(resultado, ITEM)

    def test_getItem_keeps_item_at_index(self):
        """2.12.2 RList.getItem mantiene el elemento de la posición indicada."""
        self.rlist.append(ITEM)
        _ = self.rlist.getItem(0)
        self.assertEqual(self.rlist.getItem(0), ITEM)

    def test_getItem_with_invalid_index_raises_IndexError(self):
        """2.13 RList.getItem lanza la excepción IndexError si la posición no existe."""
        with self.assertRaises(RemoteIndexError):
            self.rlist.getItem(INDICE_INVALIDO)


if __name__ == '__main__':
    unittest.main()
