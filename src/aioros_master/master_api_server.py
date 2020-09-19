from asyncio import get_event_loop
from inspect import getfullargspec
from os import getpid
from os import kill
from signal import SIGINT
from types import MappingProxyType
from typing import Any
from typing import List
from typing import Tuple

from aiohttp.web import AppRunner
from aiohttp.web import Application
from aiohttp.web import TCPSite
from aiohttp_xmlrpc.handler import XMLRPCView

from .param_cache import ParamCache
from .registration_manager import RegistrationManager


AnyResult = Tuple[int, str, Any]
BoolResult = Tuple[int, str, bool]
IntResult = Tuple[int, str, int]
StrResult = Tuple[int, str, str]
TopicInfo = Tuple[str, str]


class MasterApi(XMLRPCView):

    def __init__(self, request):
        super().__init__(request)
        method_arg_mapping = dict(self.__method_arg_mapping__)
        method_arg_mapping['system.multicall'] = \
            getfullargspec(self.rpc_multicall)
        self.__method_arg_mapping__ = MappingProxyType(method_arg_mapping)

        allowed_methods = dict(self.__allowed_methods__)
        allowed_methods['system.multicall'] = 'rpc_multicall'
        self.__allowed_methods__ = MappingProxyType(allowed_methods)

    async def rpc_multicall(self, call_list):
        results = []
        for call in call_list:
            method = self._lookup_method(call['methodName'])
            results.append(await method(*call['params']))

        return results

    async def rpc_getPid(
        self,
        caller_id: str
    ) -> IntResult:
        return 1, '', getpid()

    async def rpc_getUri(
        self,
        caller_id: str
    ) -> StrResult:
        return 1, '', self.request.app['xmlrpc_uri']

    async def rpc_shutdown(
        self,
        caller_id: str,
        msg: str=''
    ) -> IntResult:
        get_event_loop().call_soon(kill, getpid(), SIGINT)
        return 1, 'shutdown', 0

    async def rpc_deleteParam(
        self,
        caller_id: str,
        key: str
    ) -> IntResult:
        try:
            del self.request.app['param_cache'][key]
        except KeyError:
            return -1, '', 0
        self.request.app['registration_manager'].on_param_update(
            key, {}, caller_id)
        return 1, '', 0

    async def rpc_getParam(
        self,
        caller_id: str,
        key: str
    ) -> AnyResult:
        try:
            return 1, '', self.request.app['param_cache'][key]
        except KeyError:
            return -1, '', 0

    async def rpc_setParam(
        self,
        caller_id: str,
        key: str,
        value: Any
    ) -> IntResult:
        self.request.app['param_cache'][key] = value
        self.request.app['registration_manager'].on_param_update(
            key, value, caller_id)
        return 1, '', 0

    async def rpc_searchParam(
        self,
        caller_id: str,
        key: str
    ) -> AnyResult:
        try:
            return 1, '', self.request.app['param_cache'].search(
                key, caller_id)
        except KeyError:
            return -1, '', 0

    async def rpc_hasParam(
        self,
        caller_id: str,
        key: str
    ) -> BoolResult:
        return 1, '', key in self.request.app['param_cache']

    async def rpc_getParamNames(
        self,
        caller_id: str
    ) -> Tuple[int, str, List[str]]:
        return 1, '', list(self.request.app['param_cache'].keys())

    async def rpc_subscribeParam(
        self,
        caller_id: str,
        caller_api: str,
        key: str
    ) -> AnyResult:
        try:
            param_value = self.request.app['param_cache'][key]
        except KeyError:
            param_value = {}
        self.request.app['registration_manager'].register_param_subscriber(
            key,
            caller_id,
            caller_api)
        return 1, '', param_value

    async def rpc_unsubscribeParam(
        self,
        caller_id: str,
        caller_api: str,
        key: str
    ) -> IntResult:
        self.request.app['registration_manager'].unregister_param_subscriber(
            key,
            caller_id,
            caller_api)
        return 1, '', 1

    async def rpc_registerService(
        self,
        caller_id: str,
        service: str,
        service_api: str,  # tcpros_uri
        caller_api: str
    ) -> IntResult:
        self.request.app['registration_manager'].register_service(
            service,
            caller_id,
            caller_api,
            service_api)
        return 1, '', 1

    async def rpc_lookupService(
        self,
        caller_id: str,
        service: str
    ) -> StrResult:
        return 1, '', self.request.app['registration_manager'].get_service_api(
            service)

    async def rpc_unregisterService(
        self,
        caller_id: str,
        service: str,
        service_api: str
    ) -> IntResult:
        self.request.app['registration_manager'].unregister_service(
            service,
            caller_id,
            service_api)
        return 1, '', 1

    async def rpc_registerSubscriber(
        self,
        caller_id: str,
        topic: str,
        topic_type: str,
        caller_api: str
    ) -> Tuple[int, str, List[str]]:
        registration_manager = self.request.app['registration_manager']
        registration_manager.register_subscriber(
            topic,
            topic_type,
            caller_id,
            caller_api)
        return 1, '', [
            reg.api
            for reg in registration_manager.publishers[topic]]

    async def rpc_unregisterSubscriber(
        self,
        caller_id: str,
        topic: str,
        caller_api: str
    ) -> IntResult:
        self.request.app['registration_manager'].unregister_subscriber(
            topic,
            caller_id,
            caller_api)
        return 1, '', 1

    async def rpc_registerPublisher(
        self,
        caller_id: str,
        topic: str,
        topic_type: str,
        caller_api: str
    ) -> Tuple[int, str, List[str]]:
        registration_manager = self.request.app['registration_manager']
        registration_manager.register_publisher(
            topic,
            topic_type,
            caller_id,
            caller_api)
        return 1, '', [
            reg.api
            for reg in registration_manager.subscribers[topic]]

    async def rpc_unregisterPublisher(
        self,
        caller_id: str,
        topic: str,
        caller_api: str
    ) -> IntResult:
        self.request.app['registration_manager'].unregister_publisher(
            topic,
            caller_id,
            caller_api)
        return 1, '', 1

    async def rpc_lookupNode(
        self,
        caller_id: str,
        node_name: str
    ) -> StrResult:
        registration_manager = self.request.app['registration_manager']
        try:
            return 1, '', registration_manager.get_caller_api(node_name)
        except KeyError:
            return -1, '', f'unknown node {node_name}'

    async def rpc_getPublishedTopics(
        self,
        caller_id: str,
        subgraph: str
    ) -> Tuple[int, str, List[Tuple[str, str]]]:
        registration_manager = self.request.app['registration_manager']
        return 1, '', [
            (topic, registration_manager.topic_types[topic])
            for topic in registration_manager.publishers
            if topic.startswith(subgraph)]

    async def rpc_getTopicTypes(
        self,
        caller_id: str
    ) -> Tuple[int, str, List[Tuple[str, str]]]:
        registration_manager = self.request.app['registration_manager']
        return 1, '', [
            (topic, registration_manager.topic_types[topic])
            for topic in registration_manager.publishers]

    async def rpc_getSystemState(
        self,
        caller_id: str
    ) -> Tuple[int, str, Tuple[Tuple[str, List[str]],
                               Tuple[str, List[str]],
                               Tuple[str, List[str]]]]:
        reg = self.request.app['registration_manager']
        return 1, '', (
            [(topic, [publisher.caller_id for publisher in publishers])
             for topic, publishers in reg.publishers.items() if publishers],
            [(topic, [subscriber.caller_id for subscriber in subscribers])
             for topic, subscribers in reg.subscribers.items() if subscribers],
            [(topic, [service.caller_id for service in services])
             for topic, services in reg.services.items() if services]
        )


async def start_server(
    host: str,
    port: int,
    param_cache: ParamCache,
    registration_manager: RegistrationManager
) -> Tuple[AppRunner, str]:
    app = Application()
    app.router.add_route('*', '/', MasterApi)
    app.router.add_route('*', '/RPC2', MasterApi)
    runner = AppRunner(app)
    await runner.setup()
    site = TCPSite(runner, host, port)
    await site.start()

    port = site._server.sockets[0].getsockname()[1]
    xmlrpc_uri = f'http://{host}:{port}/'
    app['xmlrpc_uri'] = xmlrpc_uri
    app['param_cache'] = param_cache
    app['registration_manager'] = registration_manager

    return runner, xmlrpc_uri
