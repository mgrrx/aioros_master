from typing import Optional

from aiohttp.web import AppRunner

# TODO fix import
from aioros.graph_resource import get_local_address

from .master_api_server import start_server
from .param_cache import ParamCache
from .registration_manager import RegistrationManager


class Master:

    def __init__(self):
        self._param_cache: Optional[ParamCache] = None
        self._registration_manager: Optional[RegistrationManager] = None
        self._server: Optional[AppRunner] = None
        self._uri: Optional[str] = None

    async def init(
        self,
        loop,
        host: str = None,
        port: int = 11311,
    ) -> None:
        host = host or get_local_address()
        self._registration_manager = RegistrationManager(loop)
        self._param_cache = ParamCache()
        self._server, self._uri = await start_server(
            host,
            port,
            self._param_cache,
            self._registration_manager)

    async def close(self) -> None:
        if self._server:
            await self._server.cleanup()
            self._server = None
        self._param_cache = None
        self._registration_manager = None
