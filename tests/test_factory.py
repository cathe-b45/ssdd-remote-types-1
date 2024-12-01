import unittest
import socket
import uuid
import Ice
from remotetypes import RemoteTypes as rt
from remotetypes.factory import Factory


def _get_available_port():
    """Encuentra un puerto disponible para evitar conflictos."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))  # Bind al puerto 0 para que el sistema asigne uno disponible.
        return s.getsockname()[1]  # Devuelve el puerto asignado.


class TestFactory(unittest.TestCase):
    def setUp(self):
        """Inicializa el entorno para las pruebas."""
        self.communicator = Ice.initialize([])
        port = _get_available_port()
        self.adapter = self.communicator.createObjectAdapterWithEndpoints("TestAdapter", f"default -p {port}")
        self.adapter.activate()
        self.factory = Factory()
        # Simulamos el adaptador en el contexto actual
        self.current_context = Ice.Current(adapter=self.adapter)

    def tearDown(self):
        """Limpia el entorno después de cada prueba."""
        self.adapter.destroy()
        self.communicator.destroy()

    def test_get_invalid_type(self):
        """5.1: Factory.get lanza excepción con tipo inválido."""
        with self.assertRaises(ValueError):
            self.factory.get("InvalidType", str(uuid.uuid4()), current=self.current_context)

    def test_get_rdict_creates_new(self):
        """5.1: Factory.get crea un RDict nuevo."""
        identifier = str(uuid.uuid4())
        rdict = self.factory.get(rt.TypeName.RDict, identifier, current=self.current_context)
        self.assertIsNotNone(rdict)
        self.assertEqual(rdict.identifier(), identifier)

    def test_get_rdict_returns_existing(self):
        """5.4: Factory.get devuelve un RDict existente."""
        identifier = str(uuid.uuid4())
        rdict1 = self.factory.get(rt.TypeName.RDict, identifier, current=self.current_context)
        rdict2 = self.factory.get(rt.TypeName.RDict, identifier, current=self.current_context)
        self.assertIs(rdict1, rdict2)

    def test_get_rlist_creates_new(self):
        """5.2: Factory.get crea un RList nuevo."""
        identifier = str(uuid.uuid4())
        rlist = self.factory.get(rt.TypeName.RList, identifier, current=self.current_context)
        self.assertIsNotNone(rlist)
        self.assertEqual(rlist.identifier(), identifier)

    def test_get_rlist_returns_existing(self):
        """5.5: Factory.get devuelve un RList existente."""
        identifier = str(uuid.uuid4())
        rlist1 = self.factory.get(rt.TypeName.RList, identifier, current=self.current_context)
        rlist2 = self.factory.get(rt.TypeName.RList, identifier, current=self.current_context)
        self.assertIs(rlist1, rlist2)

    def test_get_rset_creates_new(self):
        """5.3: Factory.get crea un RSet nuevo."""
        identifier = str(uuid.uuid4())
        rset = self.factory.get(rt.TypeName.RSet, identifier, current=self.current_context)
        self.assertIsNotNone(rset)

    def test_get_rset_returns_existing(self):
        """5.6: Factory.get devuelve un RSet existente."""
        identifier = str(uuid.uuid4())
        rset1 = self.factory.get(rt.TypeName.RSet, identifier, current=self.current_context)
        rset2 = self.factory.get(rt.TypeName.RSet, identifier, current=self.current_context)
        self.assertIs(rset1, rset2)
