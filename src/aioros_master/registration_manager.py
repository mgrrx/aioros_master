from asyncio import gather
from collections import defaultdict
from itertools import chain
from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Set

from aioros.api.node_api_client import NodeApiClient

from .utils import split


class Registration(NamedTuple):
    caller_id: str
    api: str


RegistrationMap = Dict[str, Set[Registration]]


class Node:

    def __init__(self, caller_api: str):
        self.api: str = caller_api
        self.param_subscriptions: set = set()
        self.topic_subscriptions: set = set()
        self.topic_publications: set = set()
        self.services: set = set()
        self._api_client = None

    @property
    def has_any_registration(self):
        return any((self.param_subscriptions,
                    self.topic_subscriptions,
                    self.topic_publications,
                    self.services))

    @property
    def api_client(self):
        if not self._api_client:
            self._api_client = NodeApiClient('/master', self.api)
        return self._api_client

    async def close(self) -> None:
        if self._api_client:
            await self._api_client.close()
            self._api_client = None

    async def publisher_update(
        self,
        topic: str,
        publishers: List[str]
    ) -> None:
        await self.api_client.publisher_update(topic, publishers)

    async def param_update(
        self,
        key: str,
        value: Any
    ) -> None:
        await self.api_client.param_update(key, value)

    async def shutdown(
        self,
        msg: str
    ) -> None:
        await self.api_client.shutdown(msg)

    async def shutdown_and_close(
        self,
        msg: str
    ) -> None:
        await self.shutdown(msg)
        await self.close()


def compute_all_keys(key, value):
    for k, v in value.items():
        _key = key + k + '/'
        yield _key
        if isinstance(v, dict):
            yield from compute_all_keys(_key, v)


class RegistrationManager:

    def __init__(self, loop):
        self._loop = loop
        self.param_subscribers: RegistrationMap = defaultdict(set)
        self.publishers: RegistrationMap = defaultdict(set)
        self.subscribers: RegistrationMap = defaultdict(set)
        self.services: RegistrationMap = defaultdict(set)
        self.topic_types: Dict[str, str] = {}
        self._nodes: Dict[str, Node] = {}

    async def close(self):
        await gather(*[node.close() for node in self._nodes.values()])

    def on_param_update(
        self,
        param_key: str,
        param_value: Any,
        caller_id_to_ignore: str
    ) -> None:
        if not self.param_subscribers:
            return

        if param_key != '/':
            param_key = '/' + '/'.join(split(param_key)) + '/'

        if isinstance(param_value, dict):
            all_keys = set(compute_all_keys(param_key, param_value))
        else:
            all_keys = None

        for key, subscribers in self.param_subscribers.items():
            if param_key.startswith(key):
                value = param_value
                key = param_key
            elif (all_keys is not None
                  and key.startswith(param_key)
                  and key not in all_keys):
                value = {}
            else:
                continue

            for registration in subscribers:
                if registration.caller_id == caller_id_to_ignore:
                    continue

                node = self._nodes[registration.caller_id]
                self._loop.create_task(node.param_update(key[:-1], value))

        if all_keys is None:
            return

        for key in all_keys:
            if key not in self.param_subscribers:
                continue

            sub_key = key[len(param_key):]
            value = param_value
            for ns in split(sub_key):
                value = value[ns]

            for registration in self.param_subscribers[key]:
                node = self._nodes[registration.caller_id]
                self._loop.create_task(node.param_update(key[:-1], value))

    def register_param_subscriber(
        self,
        key: str,
        caller_id: str,
        caller_api: str
    ) -> None:
        self._register_node(caller_id, caller_api) \
            .param_subscriptions.add(key)

        if key != '/':
            key = '/' + '/'.join(split(key)) + '/'

        self.param_subscribers[key].add(Registration(caller_id, caller_api))

    def register_publisher(
        self,
        topic: str,
        topic_type: str,
        caller_id: str,
        caller_api: str
    ) -> None:
        self._register_node(caller_id, caller_api) \
            .topic_publications.add(topic)
        self.publishers[topic].add(Registration(caller_id, caller_api))
        if topic_type != '*' and topic not in self.topic_types:
            self.topic_types[topic] = topic_type
        self._schedule_subscriber_update(topic)

    def register_subscriber(
        self,
        topic: str,
        topic_type: str,
        caller_id: str,
        caller_api: str
    ) -> None:
        self._register_node(caller_id, caller_api) \
            .topic_subscriptions.add(topic)
        self.subscribers[topic].add(Registration(caller_id, caller_api))
        if topic_type != '*' and topic not in self.topic_types:
            self.topic_types[topic] = topic_type

    def register_service(
        self,
        name: str,
        caller_id: str,
        caller_api: str,
        service_api: str
    ) -> None:
        self._register_node(caller_id, caller_api) \
            .services.add(name)
        self.services[name].add(Registration(caller_id, service_api))

    def unregister_param_subscriber(
        self,
        key: str,
        caller_id: str,
        caller_api: str
    ) -> None:

        if key != '/':
            key = '/' + '/'.join(split(key)) + '/'

        try:
            self.param_subscribers[key].remove(
                Registration(caller_id, caller_api))
            if not self.param_subscribers[key]:
                del self.param_subscribers[key]
        except KeyError:
            pass
        try:
            self._nodes[caller_id].param_subscriptions.remove(key)
        except KeyError:
            pass
        self._check_node(caller_id)

    def unregister_publisher(
        self,
        topic: str,
        caller_id: str,
        caller_api: str
    ) -> None:
        try:
            self.publishers[topic].remove(Registration(caller_id, caller_api))
            if not self.publishers[topic]:
                del self.publishers[topic]
        except KeyError:
            pass
        self._schedule_subscriber_update(topic)
        try:
            self._nodes[caller_id].topic_publications.remove(topic)
        except KeyError:
            pass
        self._check_node(caller_id)

    def unregister_subscriber(
        self,
        topic: str,
        caller_id: str,
        caller_api: str
    ) -> None:
        try:
            self.subscribers[topic].remove(Registration(caller_id, caller_api))
            if not self.subscribers[topic]:
                del self.subscribers[topic]
        except KeyError:
            pass
        try:
            self._nodes[caller_id].topic_subscriptions.remove(topic)
        except KeyError:
            pass
        self._check_node(caller_id)

    def unregister_service(
        self,
        service: str,
        caller_id: str,
        service_api: str
    ) -> None:
        try:
            self.services[service].remove(Registration(caller_id, service_api))
        except KeyError:
            pass

        if not self.services.get(service, True):
            del self.services[service]

        try:
            self._nodes[caller_id].services.remove(service)
        except KeyError:
            pass

        self._check_node(caller_id)

    def get_service_api(self, service: str) -> str:
        return next(self.services[service]).api

    def get_caller_api(self, node_name: str) -> str:
        return self._nodes[node_name].api

    def _schedule_subscriber_update(self, topic: str) -> None:
        publishers = [
            publisher.api
            for publisher in self.publishers[topic]]
        for subscriber in self.subscribers[topic]:
            node = self._nodes[subscriber.caller_id]
            self._loop.create_task(node.publisher_update(
                topic,
                publishers
            ))

    def _check_node(self, caller_id):
        node = self._nodes.get(caller_id)
        if node and not node.has_any_registration:
            del self._nodes[caller_id]
            self._loop.create_task(node.close())

    def _register_node(self, caller_id: str, caller_api: str) -> Node:
        node = self._nodes.get(caller_id)
        if node and node.api == caller_api:
            return node
        elif node:
            self._loop.create_task(node.shutdown_and_close(
                'new node registered with same name'))
            self._unregister_all(caller_id)
            node = None

        node = Node(caller_api)
        self._nodes[caller_id] = node
        return node

    def _unregister_all(self, caller_id: str) -> None:
        registrations_chained = chain(self.param_subscribers.values(),
                                      self.publishers.values(),
                                      self.subscribers.values(),
                                      self.services.values())

        for registrations in registrations_chained:
            registrations -= set(reg for reg in registrations
                                 if reg.caller_id == caller_id)
