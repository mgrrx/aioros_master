from collections import deque
from typing import Any
from typing import Iterable


from .utils import split


class ParamCache:

    def __init__(self):
        self._params = {}

    def __contains__(self, key: str) -> bool:
        try:
            self[key]
        except KeyError:
            return False
        return True

    def __getitem__(self, key) -> Any:
        value = self._params
        for namespace in split(key):
            if not isinstance(value, dict):
                raise KeyError(key)
            value = value[namespace]
        return value

    def __setitem__(self, key, value):
        if key == '/':
            if not isinstance(value, dict):
                raise ValueError()
            self._params = value
            return
        splitted = list(split(key))
        d = self._params
        for ns in splitted[:-1]:
            if ns not in d or not isinstance(d[ns], dict):
                d[ns] = {}
            d = d[ns]
        d[splitted[-1]] = value

    def __delitem__(self, key):
        splitted = list(split(key))
        namespace = '/' + '/'.join(splitted[:-1])
        subkey = splitted[-1]
        del self[namespace][subkey]

    def keys(self) -> Iterable[str]:
        worklist = deque(sorted(self._params.items()))
        while worklist:
            key, value = worklist.popleft()
            if isinstance(value, dict):
                worklist.extendleft(
                    (f'{key}/{k}', v)
                    for k, v in sorted(value.items()))
            else:
                yield '/' + key

    def search(self, key, namespace) -> Any:
        if key.startswith('/'):
            if key in self:
                return key
            return None

        key_ns = next(split(key))

        if f'{namespace}/{key_ns}' in self:
            return f'{namespace}/{key}'

        splitted = list(split(namespace))
        for i in range(1, len(splitted) + 1):
            search_key = '/' + '/'.join(splitted[:-i] + [key_ns])
            if search_key in self:
                return '/' + '/'.join(splitted[:-i] + [key])

        return None
