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

import importlib
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import NamedTuple, Protocol, runtime_checkable

from esrally.config import Config
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

    __adapter_prefixes__: tuple[str, ...] = tuple()

    @classmethod
    def from_config(cls, cfg: Config) -> Adapter:
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


ADAPTER_CLASS_NAMES = ",".join(
    [
        "esrally.storage._http:HTTPAdapter",
    ]
)


class AdapterRegistry:
    """AdapterClassRegistry allows to register classes of adapters to be selected according to the target URL."""

    def __init__(self, cfg: Config) -> None:
        self._classes: dict[str, type[Adapter]] = {}
        self._adapters: dict[type[Adapter], Adapter] = {}
        self._lock = threading.Lock()
        self._cfg = cfg

    @classmethod
    def from_config(cls, cfg: Config) -> AdapterRegistry:
        registry = cls(cfg)
        adapters_specs = (
            cfg.opts(section="storage", key="storage.adapters", default_value=ADAPTER_CLASS_NAMES, mandatory=False)
            .replace(" ", "")
            .split(",")
        )
        for spec in adapters_specs:
            module_name, class_name = spec.split(":")
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                LOG.exception("Failed to import module '%s'", module_name)
                continue
            obj = getattr(module, class_name)
            if not isinstance(obj, type) or not issubclass(obj, Adapter):
                raise TypeError(f"'{obj}' is not a valid subclass of Adapter")
            registry.register_class(obj, obj.__adapter_prefixes__)
        return registry

    def register_class(self, cls: type[Adapter], prefixes: Iterable[str]) -> type[Adapter]:
        with self._lock:
            keys_to_move: set[str] = set()
            for p in prefixes:
                self._classes[p] = cls
                # Adapter types are sorted in descending order by prefix length.
                keys_to_move.update(k for k in self._classes if len(k) < len(p))
            for k in keys_to_move:
                self._classes[k] = self._classes.pop(k)
        return cls

    def get(self, url: str) -> Adapter:
        with self._lock:
            for prefix, cls in self._classes.items():
                if url.startswith(prefix):
                    adapter = self._adapters.get(cls)
                    if adapter is None:
                        try:
                            adapter = cls.from_config(self._cfg)
                        except ValueError as ex:
                            LOG.error("Failed to configure adapter for URL '%s': %s", url, ex)
                            continue
                    self._adapters[cls] = adapter
                    return adapter
        raise ValueError(f"No adapter found for url '{url}'")
