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

    # A commas separated list of URL prefixes used to associate an adapter implementation to a remote file URL.
    # This value will be overridden by `Adapter` subclasses to be consumed by `AdapterRegistry` class.
    # Example:
    #   ```
    #   class HTTPAdapter(Adapter):
    #       # The value will serve to associate any URL with "https" scheme to `HTTPAdapter` subclass.
    #       __adapter_URL_prefixes__ = "http://, https://"
    #   ```
    __adapter_URL_prefixes__: str = ""

    @classmethod
    def from_config(cls, cfg: Config) -> Adapter:
        """Default `Adapter` objects factory method used to create adapters from `esrally` client.

        Default implementation will ignore `cfg` parameter. It can be overridden from `Adapter` implementations that
        accept customized parameters from the configuration file.

        :param cfg: the configuration object from which to get configuration values.
        :return: an adapter object.
        """
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


class AdapterClassEntry(NamedTuple):
    url_prefix: str
    cls: type[Adapter]


class AdapterRegistry:
    """AdapterClassRegistry allows to register classes of adapters to be selected according to the target URL."""

    def __init__(self, cfg: Config) -> None:
        self._classes: list[AdapterClassEntry] = []
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
            module = importlib.import_module(module_name)
            obj = getattr(module, class_name)
            if not isinstance(obj, type) or not issubclass(obj, Adapter):
                raise TypeError(f"'{obj}' is not a valid subclass of Adapter")
            registry.register_class(obj, obj.__adapter_URL_prefixes__.split(","))
        return registry

    def register_class(self, cls: type[Adapter], prefixes: Iterable[str]) -> type[Adapter]:
        with self._lock:
            for p in prefixes:
                self._classes.append(AdapterClassEntry(p.strip(), cls))
            # The list of adapter classes is kept sorted from the longest prefix to the shorter to ensure that matching
            # a shorter URL prefix will never hide matching a longer one.
            self._classes.sort(key=lambda e: len(e.url_prefix), reverse=True)
        return cls

    def get(self, url: str) -> Adapter:
        with self._lock:
            for e in self._classes:
                if url.startswith(e.url_prefix):
                    adapter = self._adapters.get(e.cls)
                    if adapter is None:
                        adapter = e.cls.from_config(self._cfg)
                    self._adapters[e.cls] = adapter
                    return adapter
        raise ValueError(f"No adapter found for url '{url}'")
