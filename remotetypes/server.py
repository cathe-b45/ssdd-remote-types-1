import logging

import Ice

from remotetypes.factory import Factory


class Server(Ice.Application):
    """Ice.Application for the server."""

    def __init__(self) -> None:
        """Initialise the Server objects."""
        super().__init__()
        self.logger = logging.getLogger(__file__)

    def run(self, args: list[str]) -> int:
        """Execute the main server actions..

        It will initialise the needed middleware elements in order to execute the server.
        """
        factory_servant = Factory()
        adapter = self.communicator().createObjectAdapter("remotetypes")
        proxy = adapter.add(factory_servant, self.communicator().stringToIdentity("factory"))
        self.logger.info('Proxy: "%s"', proxy)

        adapter.activate()
        self.shutdownOnInterrupt()
        self.communicator().waitForShutdown()
        return 0
