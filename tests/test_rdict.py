import unittest
import uuid
import Ice
import tempfile
import json
import RemoteTypes as rt  # Asegúrate de que este import esté presente.
from remotetypes.remotedict import RemoteDict
from RemoteTypes import KeyError as RemoteKeyError
from RemoteTypes import CancelIteration

KEY = 'clave_prueba'
VALUE = 'valor_prueba'
OTRA_CLAVE = 'otra_clave'
OTRO_VALOR = 'otro_valor'
CLAVE_INVALIDA = 'clave_invalida'

class TestRemoteDict(unittest.TestCase):
    """Casos de prueba para la clase RemoteDict."""

    def setUp(self):
        """Configuración inicial para cada prueba."""
        # Crear un archivo temporal para el almacenamiento
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.storage_file = self.temp_file.name
        self.temp_file.close()  # Cerrar el archivo temporal

        # Inicializar el archivo JSON con un objeto vacío
        with open(self.storage_file, 'w') as file:
            json.dump({}, file)

        # Inicializar el comunicador y el adaptador
        self.communicator = Ice.initialize([])
        self.adapter = self.communicator.createObjectAdapterWithEndpoints(f"TestAdapter-{uuid.uuid4()}", "default -p 0")
        self.adapter.activate()

        # Crear el objeto Current de Ice
        self.current = Ice.Current()
        self.current.adapter = self.adapter

        # Crear la instancia de RemoteDict
        self.rdict = RemoteDict(identifier=str(uuid.uuid4()), storage_file=self.storage_file)
        self.adapter.add(self.rdict, Ice.stringToIdentity(self.rdict.id_))


    # def tearDown(self):
    #     """Limpieza después de cada prueba."""
    #     self.adapter.destroy()
    #     self.communicator.destroy()
    #     # Eliminar el archivo temporal
    #     if os.path.exists(self.storage_file):
    #         os.remove(self.storage_file)

    def test_setItem_and_getItem(self):
        """1.8, 1.10.1, 1.10.2: setItem permite recuperar el valor con getItem y mantiene el valor."""
        self.rdict.setItem(KEY, VALUE)
        retrieved_value = self.rdict.getItem(KEY)
        self.assertEqual(retrieved_value, VALUE)
        self.assertEqual(self.rdict.length(), 1)

    def test_getItem_nonexistent_key_raises_KeyError(self):
        """1.9: getItem lanza KeyError si la clave no existe."""
        with self.assertRaises(RemoteKeyError):
            self.rdict.getItem(CLAVE_INVALIDA)

    def test_remove_existing_key(self):
        """1.1: remove borra un elemento existente."""
        self.rdict.setItem(KEY, VALUE)
        self.rdict.remove(KEY)
        self.assertFalse(self.rdict.contains(KEY))
        self.assertEqual(self.rdict.length(), 0)

    def test_remove_nonexistent_key_raises_KeyError(self):
        """1.2: remove lanza KeyError si la clave no existe."""
        with self.assertRaises(RemoteKeyError):
            self.rdict.remove(CLAVE_INVALIDA)

    def test_length_returns_correct_length(self):
        """1.3: length devuelve la longitud correcta."""
        self.assertEqual(self.rdict.length(), 0)
        self.rdict.setItem(KEY, VALUE)
        self.assertEqual(self.rdict.length(), 1)

    def test_contains_returns_false_for_missing_key(self):
        """1.4: contains devuelve False si la clave no existe."""
        self.assertFalse(self.rdict.contains(KEY))

    def test_contains_returns_true_for_existing_key(self):
        """1.5: contains devuelve True si la clave existe."""
        self.rdict.setItem(KEY, VALUE)
        self.assertTrue(self.rdict.contains(KEY))

    def test_hash_returns_same_value_when_unmodified(self):
        """1.6: hash devuelve enteros iguales si no se modifica."""
        self.rdict.setItem(KEY, VALUE)
        hash_value = self.rdict.hash()
        self.assertEqual(self.rdict.hash(), hash_value)

    def test_hash_returns_different_value_when_modified(self):
        """1.7: hash devuelve enteros diferentes si se modifica."""
        self.rdict.setItem(KEY, VALUE)
        hash_value = self.rdict.hash()
        self.rdict.setItem(OTRA_CLAVE, OTRO_VALOR)
        self.assertNotEqual(self.rdict.hash(), hash_value)

    def test_pop_existing_key(self):
        """1.12.1 y 1.12.2: pop devuelve el valor y elimina la clave."""
        self.rdict.setItem(KEY, VALUE)
        value = self.rdict.pop(KEY)
        self.assertEqual(value, VALUE)
        self.assertFalse(self.rdict.contains(KEY))
        self.assertEqual(self.rdict.length(), 0)

    def test_pop_nonexistent_key_raises_KeyError(self):
        """1.11: pop lanza KeyError si la clave no existe."""
        with self.assertRaises(RemoteKeyError):
            self.rdict.pop(CLAVE_INVALIDA)

    def test_iterator_traverses_all_keys(self):
        """Prueba que el iterador recorre todas las claves y valores formateados."""
        keys_values = {"clave_prueba: valor_prueba", "otra_clave: otro_valor"}
        self.rdict.setItem("clave_prueba", "valor_prueba")
        self.rdict.setItem("otra_clave", "otro_valor")
        iterator = self.rdict.iter(current=self.current)
        collected_items = set()
        while True:
            try:
                collected_items.add(iterator.next())
            except rt.StopIteration:
                break
        self.assertEqual(collected_items, keys_values)


    def test_iterator_raises_CancelIteration_if_dict_modified(self):
        """Prueba que el iterador lanza CancelIteration si el diccionario es modificado."""
        self.rdict.setItem(KEY, VALUE)
        iterator = self.rdict.iter(current=self.current)
        self.rdict.setItem(OTRA_CLAVE, OTRO_VALOR)  # Modifica el diccionario
        with self.assertRaises(CancelIteration):
            iterator.next()
