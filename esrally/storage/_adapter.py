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

import datetime
import importlib
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from esrally.storage._range import NO_RANGE, RangeSet
from esrally.types import Config

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


@dataclass
class Head:
    url: str | None = None
    content_length: int | None = None
    accept_ranges: bool | None = None
    ranges: RangeSet = NO_RANGE
    document_length: int | None = None
    crc32c: str | None = None
    date: datetime.datetime | None = None

    @classmethod
    def create(
        cls,
        url: str | None = None,
        content_length: int | None = None,
        accept_ranges: bool | None = None,
        ranges: RangeSet = NO_RANGE,
        document_length: int | None = None,
        crc32c: str | None = None,
        date: datetime.datetime | None = None,
    ) -> Head:
        if content_length is None and ranges:
            content_length = ranges.size
        if document_length is None and not ranges:
            document_length = content_length
        return cls(
            url=url,
            accept_ranges=accept_ranges,
            content_length=content_length,
            ranges=ranges,
            document_length=document_length,
            crc32c=crc32c,
            date=date,
        )

    def check(self, other: Head) -> None:
        for field in ("url", "content_length", "accept_ranges", "ranges", "document_length", "crc32c", "date"):
            want = getattr(self, field)
            got = getattr(other, field)
            if not ({got, want} & {None, NO_RANGE}) and got != want:
                raise ValueError(f"unexpected '{field}': got {got}, want {want}")


class Adapter(ABC):
    """Base class for storage class client implementation"""

    @classmethod
    def match_url(cls, url: str) -> str:
        """It returns a canonical URL in case this adapter accepts the URL, None otherwise."""
        raise NotImplementedError

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
    def list(self, url: str) -> Iterator[Head]:
        """It gets list of file headers.
        :return: the Head of the remote file.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """

    @abstractmethod
    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        """It downloads a remote bucket object to a local file path.

        :param url: it represents the URL of the remote file object.
        :param stream: it represents the local file stream where to write data to.
        :param head: it allows to specify optional parameters:
            - range: portion of the file to transfer (it must be empty or a continuous range).
            - document_length: the number of bytes to transfer.
            - crc32c the CRC32C checksum of the file.
            - date: the date the file has been modified.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """

    @abstractmethod
    def put(self, stream: Readable, url: str, head: Head | None = None) -> Head:
        """It uploads a local file object to a remote bucket.

        :param stream: it represents the local file stream where to read data from.
        :param url: it represents the URL of the remote file object.
        :param head: it allows to specify optional parameters:
            - range: the portion of the file to transfer (it must be empty or a continuous range).
            - document_length: the number of bytes to transfer.
            - crc32c the CRC32C checksum of the file.
            - date: the date the file was modified.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """


ADAPTER_CLASS_NAMES = ",".join(
    [
        "esrally.storage._tracks:TracksRepositoryAdapter",
        "esrally.storage._s3:S3Adapter",
        "esrally.storage._http:HTTPAdapter",
    ]
)


class AdapterRegistry:
    """AdapterClassRegistry allows to register classes of adapters to be selected according to the target URL."""

    def __init__(self, cfg: Config) -> None:
        self._classes: list[type[Adapter]] = []
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
            except ModuleNotFoundError:
                LOG.exception("unable to import module '%s'.", module_name)
                continue
            try:
                obj = getattr(module, class_name)
            except AttributeError:
                raise ValueError("Invalid Adapter class name: '{class_name}'.")
            if not isinstance(obj, type) or not issubclass(obj, Adapter):
                raise TypeError(f"'{obj}' is not a valid subclass of Adapter")
            registry.register_class(obj)
        return registry

    def register_class(self, cls: type[Adapter]) -> type[Adapter]:
        with self._lock:
            self._classes.append(cls)
        return cls

    def get(self, url: str) -> tuple[Adapter, str]:
        with self._lock:
            for cls in self._classes:
                try:
                    actual_url = cls.match_url(url)
                    break
                except NotImplementedError:
                    continue
            else:
                raise ValueError(f"No adapter found for url '{url}'")

        adapter = self._adapters.get(cls)
        if adapter is None:
            adapter = cls.from_config(self._cfg)
        self._adapters[cls] = adapter
        return adapter, actual_url
