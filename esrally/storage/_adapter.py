# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import NamedTuple, Protocol, runtime_checkable

from esrally.storage._range import NO_RANGE, RangeSet

LOG = logging.getLogger(__name__)


class ServiceUnavailableError(Exception):
    """It is raised when an adapter refuses providing service for example because of too many requests"""


@runtime_checkable
class Writable(Protocol):

    def write(self, data: bytes) -> None:
        pass


@runtime_checkable
class Readable(Protocol):

    def read(self, size: int = -1) -> bytes:
        pass


class Head(NamedTuple):
    url: str
    content_length: int | None = None
    accept_ranges: bool = False
    ranges: RangeSet = NO_RANGE

    @classmethod
    def create(cls, url: str, content_length: int | None = None, accept_ranges: bool | None = None, ranges: RangeSet = NO_RANGE) -> Head:
        if accept_ranges is None:
            accept_ranges = bool(ranges)
        return cls(url=url, accept_ranges=accept_ranges, content_length=content_length, ranges=ranges)


class Adapter(ABC):
    """Base class for storage class client implementation"""

    @classmethod
    def from_url(cls, url: str) -> Adapter:
        return cls()

    @abstractmethod
    def head(self, url: str) -> Head:
        """It gets remote file headers.
        :return: the Head of the remote file.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """

    @abstractmethod
    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        """It downloads a remote bucket object to a local file path.

        :param url: it represents the URL of the remote file object.
        :param stream: it represents the local file stream where to write data to.
        :param ranges: it represents the portion of the file to transfer (it must be empty or a continuous range).
        :raises ServiceUnavailableError: in case on temporary service failure.
        """


class AdapterClassRegistry:
    """AdapterClassRegistry allows to register classes of adapters to be selected according to the target URL."""

    def __init__(self) -> None:
        self._classes: dict[str, type[Adapter]] = {}
        self._lock = threading.Lock()

    def register(self, cls: type[Adapter], prefixes: Iterable[str]) -> type[Adapter]:
        with self._lock:
            keys_to_move: set[str] = set()
            for p in prefixes:
                self._classes[p] = cls
                # Adapter types are sorted in descending order by prefix length.
                keys_to_move.update(k for k in self._classes if len(k) < len(p))
            for k in keys_to_move:
                self._classes[k] = self._classes.pop(k)
        return cls

    def select(self, url: str) -> list[type[Adapter]]:
        return [cls for prefix, cls in self._classes.items() if url.startswith(prefix)]


DEFAULT_CLASS_REGISTRY = AdapterClassRegistry()


def register_adapter_class(
    *prefixes: str, registry: AdapterClassRegistry = DEFAULT_CLASS_REGISTRY
) -> Callable[[type[Adapter]], type[Adapter]]:
    """This class decorator registers an implementation of the Client protocol.
    :param prefixes: list of prefixes names the adapter class has to be registered for
    :return: a type decorator
    """

    def decorator(cls: type[Adapter]) -> type[Adapter]:
        registry.register(cls, prefixes)
        return cls

    return decorator


def adapter_classes(url: str, registry: AdapterClassRegistry = DEFAULT_CLASS_REGISTRY) -> list[type[Adapter]]:
    return registry.select(url)
