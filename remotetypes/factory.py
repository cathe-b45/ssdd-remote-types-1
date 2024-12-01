from typing import Dict
import os
import Ice
import RemoteTypes as rt
from typing import Optional
from remotetypes.remotedict import RemoteDict
from remotetypes.remotelist import RemoteList
from remotetypes.remoteset import RemoteSet


class Factory(rt.Factory):
    STORAGE_PATH = "storage"  # Ruta base para almacenar archivos de persistencia

    def __init__(self) -> None:
        """Inicializa la factoría con los adaptadores necesarios."""
        self._rdicts: Dict[str, rt.RDictPrx] = {}
        self._rlists: Dict[str, rt.RListPrx] = {}
        self._rsets: Dict[str, rt.RSetPrx] = {}

        # Crear el directorio de almacenamiento si no existe
        os.makedirs(self.STORAGE_PATH, exist_ok=True)


    def get(self, typeName: rt.TypeName, identifier: Optional[str] = None, current: Optional[Ice.Current] = None):
        """Obtiene un objeto remoto del tipo especificado.

        Args:
            typeName (rt.TypeName): Tipo del objeto a obtener (RDict, RList, RSet).
            identifier (Optional[str]): Identificador del objeto. Si no se especifica, se usa uno por defecto.
            current (Optional[Ice.Current]): Contexto de la llamada Ice (se maneja automáticamente).

        Returns:
            rt.RTypePrx: Proxy del objeto remoto solicitado.

        """
        if typeName == rt.TypeName.RDict:
            return self._get_rdict(identifier or "default_rdict",current.adapter)
        elif typeName == rt.TypeName.RList:
            return self._get_rlist(identifier or "default_rlist",current.adapter)
        elif typeName == rt.TypeName.RSet:
            return self._get_rset(identifier or "default_rset",current.adapter)
        else:
            raise ValueError(f"Tipo inválido solicitado: {typeName}")

    def _get_rdict(self, identifier: str, adapter) -> rt.RDictPrx:
        """Devuelve un RDict nuevo o existente."""
        if identifier not in self._rdicts:
            storage_file = os.path.join(self.STORAGE_PATH, f"rdict_{identifier}.json")
            rdict = RemoteDict(identifier, storage_file)  # Usa la clase RemoteDict
            proxy = adapter.addWithUUID(rdict)
            self._rdicts[identifier] = rt.RDictPrx.uncheckedCast(proxy)
        return self._rdicts[identifier]

    def _get_rlist(self, identifier: str,adapter) -> rt.RListPrx:
        """Devuelve un RList nuevo o existente."""
        if identifier not in self._rlists:
            storage_file = os.path.join(self.STORAGE_PATH, f"rlist_{identifier}.json")
            rlist = RemoteList(identifier, storage_file)  # Usa la clase RemoteList
            proxy = adapter.addWithUUID(rlist)
            self._rlists[identifier] = rt.RListPrx.uncheckedCast(proxy)
        return self._rlists[identifier]

    def _get_rset(self, identifier: str, adapter) -> rt.RSetPrx:
        """Devuelve un RSet nuevo o existente."""
        if identifier not in self._rsets:
            rset = RemoteSet(identifier)
            proxy = adapter.addWithUUID(rset)  # Registrar como objeto Ice
            self._rsets[identifier] = rt.RSetPrx.uncheckedCast(proxy)
        return self._rsets[identifier]
