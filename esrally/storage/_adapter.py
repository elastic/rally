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

import copy
import dataclasses
import datetime
import importlib
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Container, Iterator
from typing import Any

from typing_extensions import Self

from esrally import types
from esrally.storage._config import StorageConfig
from esrally.storage._range import NO_RANGE, RangeSet

LOG = logging.getLogger(__name__)


class ServiceUnavailableError(Exception):
    """It is raised when an adapter refuses providing service for example because of too many requests"""


_HEAD_CHECK_IGNORE = frozenset(["url"])


@dataclasses.dataclass
class Head:
    url: str | None = None
    content_length: int | None = None
    accept_ranges: bool | None = None
    ranges: RangeSet = NO_RANGE
    document_length: int | None = None
    crc32c: str | None = None
    date: datetime.datetime | None = None

    def check(self, other: Head, ignore: Container[str] = _HEAD_CHECK_IGNORE) -> None:
        for field in ("url", "content_length", "accept_ranges", "ranges", "document_length", "crc32c", "date"):
            if ignore is not None and field in ignore:
                continue
            want = getattr(self, field)
            got = getattr(other, field)
            if _all_specified(got, want) and got != want:
                # If both got and want are specified, then they have to match.
                raise ValueError(f"unexpected '{field}': got {got}, want {want}")


def _all_specified(*objs: Any) -> bool:
    # This behaves like all(), but it treats False as True.
    return all(o or o is False for o in objs)


class Adapter(ABC):
    """Base class for storage class client implementation"""

    @classmethod
    @abstractmethod
    def match_url(cls, url: str) -> bool:
        """It returns a canonical URL in case this adapter accepts the URL, None otherwise."""

    @classmethod
    @abstractmethod
    def from_config(cls, cfg: types.Config) -> Self:
        """Default `Adapter` objects factory method used to create adapters from `esrally` client.

        Default implementation will ignore `cfg` parameter. It can be overridden from `Adapter` implementations that
        accept customized parameters from the configuration file.

        :param cfg: the configuration object from which to get configuration values.
        :return: an adapter object.
        """

    @abstractmethod
    def head(self, url: str) -> Head:
        """It gets remote file headers.
        :return: the Head of the remote file.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """

    @abstractmethod
    def get(self, url: str, *, check_head: Head | None = None) -> tuple[Head, Iterator[bytes]]:
        """It downloads a remote bucket object to a local file path.

        :param url: it represents the URL of the remote file object.
        :param check_head: it allows to specify optional parameters:
            - range: portion of the file to transfer (it must be empty or a continuous range).
            - content_length: the number of bytes to transfer.
            - crc32c the CRC32C checksum of the file.
            - date: the date the file has been modified.
        :raises ServiceUnavailableError: in case on temporary service failure.
        :returns: a tuple containing the Head of the remote file, and the iterator of bytes received from the service.
        """


class AdapterRegistry:
    """AdapterClassRegistry allows to register classes of adapters to be selected according to the target URL."""

    @classmethod
    def from_config(cls, cfg: types.Config) -> Self:
        return cls(StorageConfig.from_config(cfg))

    def __init__(self, cfg: StorageConfig) -> None:
        self._classes: list[type[Adapter]] = []
        self._adapters: dict[type[Adapter], Adapter] = {}
        self._lock = threading.Lock()
        self._cfg = StorageConfig.from_config(cfg)
        for name in self._cfg.adapters:
            try:
                self.register_class(name)
            except ImportError:
                LOG.exception("failed registering adapter '%s'", name)
                continue

    def register_class(self, cls: type[Adapter] | str, position: int | None = None) -> type[Adapter]:
        if isinstance(cls, str):
            module_name, class_name = cls.split(":", 1)
            module = importlib.import_module(module_name)
            try:
                cls = getattr(module, class_name)
            except AttributeError:
                raise ValueError(f"invalid adapter class name: '{class_name}'")
        if not isinstance(cls, type) or not issubclass(cls, Adapter):
            raise TypeError(f"'{cls}' is not a subclass of Adapter")
        with self._lock:
            if position is None:
                self._classes.append(cls)
            else:
                self._classes.insert(position, cls)
        return cls

    def get(self, url: str) -> Adapter:
        for cls in self._classes:
            if cls.match_url(url):
                break
        else:
            raise ValueError(f"No adapter found for URL: {url}")

        adapter = self._adapters.get(cls)
        if adapter is not None:
            return adapter

        self._adapters[cls] = adapter = cls.from_config(self._cfg)
        return adapter


@dataclasses.dataclass
class DummyAdapter(Adapter):
    heads: dict[str, Head] = dataclasses.field(default_factory=dict)
    data: dict[str, bytes] = dataclasses.field(default_factory=dict)

    @classmethod
    def match_url(cls, url: str) -> bool:
        return True

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        return cls()

    def head(self, url: str) -> Head:
        try:
            return copy.copy(self.heads[url])
        except KeyError:
            raise FileNotFoundError from None

    def get(self, url: str, *, check_head: Head | None = None) -> tuple[Head, Iterator[bytes]]:
        ranges: RangeSet = NO_RANGE
        if check_head is not None:
            ranges = check_head.ranges
            if len(ranges) > 1:
                raise NotImplementedError("len(head.ranges) > 1")

        data = self.data[url]
        if ranges:
            data = data[ranges.start : ranges.end]

        head = Head(url, content_length=ranges.size, ranges=ranges, document_length=len(data))
        return head, iter((data,))
