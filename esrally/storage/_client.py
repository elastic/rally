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
import time
import urllib.parse
from collections import defaultdict, deque
from collections.abc import Iterator
from dataclasses import dataclass
from random import Random
from typing import NamedTuple

from typing_extensions import Self

from esrally import types
from esrally.storage._adapter import (
    AdapterRegistry,
    Head,
    ServiceUnavailableError,
    Writable,
)
from esrally.storage._mirror import MirrorList
from esrally.utils import pretty
from esrally.utils.threads import WaitGroup, WaitGroupLimitError

LOG = logging.getLogger(__name__)

MIRRORS_FILES = "~/.rally/storage-mirrors.json"
MAX_CONNECTIONS = 4
RANDOM = Random(time.monotonic_ns())


class CachedHeadError(Exception):
    pass


@dataclass
class CachedHead:
    timestamp: float
    head: Head | None = None
    error: Exception | None = None

    def __init__(self, timestamp: float, /, head: Head | None = None, error: Exception | None = None):
        self.timestamp = timestamp
        if head is not None:
            if error is not None:
                raise ValueError("cannot specify both head and error")
            self.head = head
        elif error is not None:
            try:
                raise CachedHeadError("Cached exception") from error
            except CachedHeadError as ex:
                self.error = ex
        else:
            raise ValueError("must specify either head or error")

    def get(self, ttl: float | None = None) -> Head:
        if ttl is not None:
            if time.monotonic() > self.timestamp + ttl:
                raise TimeoutError("cached head has expired")
        if self.error is not None:
            raise self.error
        assert self.head is not None
        return self.head


class Client:
    """It handles client instances allocation allowing reusing pre-allocated instances from the same thread."""

    @classmethod
    def from_config(
        cls, cfg: types.Config, adapters: AdapterRegistry | None = None, mirrors: MirrorList | None = None, random: Random | None = None
    ) -> Self:
        if adapters is None:
            adapters = AdapterRegistry.from_config(cfg)
        if mirrors is None:
            mirrors = MirrorList.from_config(cfg)
        if random is None:
            random_seed = cfg.opts(section="storage", key="storage.random_seed", default_value=None, mandatory=False)
            if random_seed is None:
                random = RANDOM
            else:
                random = Random(random_seed)
        max_connections = int(cfg.opts(section="storage", key="storage.max_connections", default_value=MAX_CONNECTIONS, mandatory=False))
        return cls(adapters=adapters, mirrors=mirrors, random=random, max_connections=max_connections)

    def __init__(
        self,
        adapters: AdapterRegistry,
        mirrors: MirrorList,
        random: Random,
        max_connections: int = MAX_CONNECTIONS,
    ):
        self._adapters: AdapterRegistry = adapters
        self._cached_heads: dict[str, CachedHead] = {}
        self._connections: dict[str, WaitGroup] = defaultdict(lambda: WaitGroup(max_count=max_connections))
        self._lock = threading.Lock()
        self._mirrors: MirrorList = mirrors
        self._random: Random = random
        self._stats: dict[str, deque[ServerStats]] = defaultdict(lambda: deque(maxlen=100))

    @property
    def adapters(self):
        return self._adapters

    def head(self, url: str, ttl: float | None = None) -> Head:
        """It gets remote file headers."""

        if ttl is not None:
            # when time-to-leave is given, it looks up for pre-cached head first
            try:
                return self._cached_heads[url].get(ttl)
            except (KeyError, TimeoutError):
                # no cached head, or it has expired.
                pass

        adapter = self._adapters.get(url)
        start_time = time.monotonic()
        try:
            head = adapter.head(url)
            error = None
        except Exception as ex:
            head = None
            error = ex
        finally:
            end_time = time.monotonic()

        with self._lock:
            # It records per-server time statistics.
            self._stats[_server_key(url)].append(ServerStats(url, start_time, end_time))
            # The cached value could be an exception, or a head. In this way it will not retry previously failed
            # urls until the TTL expires.
            self._cached_heads[url] = CachedHead(start_time, head=head, error=error)

        if error is not None:
            raise error
        assert head is not None
        return head

    def resolve(self, url: str, want: Head | None, ttl: float = 60.0) -> Iterator[Head]:
        """It looks up mirror list for given URL and yield mirror heads.
        :param url: the remote file URL at its mirrored source location.
        :param want: extra parameters to mach remote heads.
            - document_length: if not none it will filter out mirrors which file has an unexpected document lengths.
            - crc32c: if not none it will filter out mirrors which file has an unexpected crc32c checksum.
            - accept_ranges: if True it will filter out mirrors that are not supporting ranges.
        :param ttl: the time to live value (in seconds) to use for cached heads retrieval.
        :return: iterator over mirror URLs
        """

        try:
            urls = self._mirrors.resolve(url)
        except ValueError:
            urls = [url]
        else:
            if len(urls) > 1:
                # It shuffles mirror URLs in the hope two threads will try different mirrors for the same URL.
                self._random.shuffle(urls)

                # It uses measured latencies and the number of connections to affect the order of the mirrors.
                weights = {u: self._average_latency(u) * self._random.uniform(1.0, self._server_connections(url).count) for u in urls}
                LOG.debug("resolve '%s': mirror weights: %s", url, weights)
                urls.sort(key=lambda u: weights[u])
                LOG.debug("resolve '%s': mirror urls: %s", url, urls)

        if url not in urls:
            # It ensures source URL is in the list so that it will be used as fall back when any mirror works.
            urls.append(url)

        for u in urls:
            try:
                got = self.head(u, ttl=ttl)
                if want is not None:
                    want.check(got)
            except CachedHeadError:
                # The error was previously cached, therefore it has been already logged.
                pass
            except Exception as ex:
                if u == url:
                    LOG.warning("Failed to get head from original URL: '%s', %s", u, ex)
                else:
                    LOG.warning("Failed to get head from mirror URL: '%s', %s", u, ex)
            else:
                yield got

    def get(self, url: str, stream: Writable, want: Head | None = None) -> Head:
        """It downloads a remote bucket object to a local file path.

        :param url: the URL of the remote file.
        :param stream: the destination file stream where to write data to.
        :param want: extra params for getting the file:
            - document_length: the document length of the file to transfer.
            - crc32c: the crc32c checksum of the file to transfer.
            - ranges: the portion of the file to transfer.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """
        if want is None:
            want_head = None
        elif want.ranges:
            want_head = Head(accept_ranges=True, content_length=want.document_length, date=want.date, crc32c=want.crc32c)
        else:
            want_head = Head(content_length=want.content_length, date=want.date, crc32c=want.crc32c)
        for got in self.resolve(url, want=want_head):
            assert got.url is not None
            adapter = self._adapters.get(got.url)
            connections = self._server_connections(got.url)
            try:
                connections.add(1)
            except WaitGroupLimitError:
                LOG.debug("connection limit exceeded for url '%s'", url)
                continue
            try:
                return adapter.get(got.url, stream, want=want)
            except ServiceUnavailableError as ex:
                LOG.warning("service unavailable error received: url='%s' %s", url, ex)
                with self._lock:
                    # It corrects the maximum number of connections for this server.
                    connections.max_count = max(1, connections.count)
            finally:
                connections.done()
        raise ServiceUnavailableError(f"no connections available for getting URL '{url}'")

    def _server_connections(self, url: str) -> WaitGroup:
        with self._lock:
            return self._connections[_server_key(url)]

    def _server_stats_from_url(self, url: str, ttl: float | None = None) -> deque[ServerStats]:
        with self._lock:
            stats = self._stats[_server_key(url)]
            if ttl is not None:
                # it removes expired latencies
                deadline = time.monotonic() - ttl
                while stats and stats[0].end < deadline:
                    stats.popleft()
        return stats

    def _average_latency(self, url: str, ttl: float | None = 60.0) -> float:
        stats = self._server_stats_from_url(url, ttl)
        if not stats:
            return 0.0
        return sum(s.latency for s in stats) / len(stats)

    def monitor(self):
        with self._lock:
            connections = {u: c for u, c in self._connections.items() if c.count > 0}
        infos = list[str]()
        for url, connection in sorted(connections.items()):
            latency = self._average_latency(url)
            infos.append(f"- '{url}' count={connection.count} latency={pretty.duration(latency)}")
        if len(infos) > 0:
            LOG.info("Active client connection(s):\n  %s", "\n  ".join(infos))


class ServerStats(NamedTuple):
    url: str
    start: float
    end: float

    @property
    def latency(self):
        return self.end - self.start


def _head_or_raise(value: Head | Exception) -> Head:
    if isinstance(value, Head):
        return value
    if isinstance(value, Exception):
        raise value
    raise TypeError(f"unexpected value type in cached heads: got {value}, expected Head | Exception")


def _server_key(url: str) -> str:
    u = urllib.parse.urlparse(url)
    return f"{u.scheme}://{u.netloc}/"
