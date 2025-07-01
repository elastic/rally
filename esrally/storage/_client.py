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
from random import Random
from typing import NamedTuple

from esrally import config
from esrally.storage._adapter import (
    Adapter,
    AdapterRegistry,
    Head,
    ServiceUnavailableError,
    Writable,
)
from esrally.storage._mirror import MirrorList
from esrally.storage._range import NO_RANGE, RangeSet
from esrally.utils import pretty
from esrally.utils.threads import WaitGroup, WaitGroupLimitError

LOG = logging.getLogger(__name__)

MIRRORS_FILES = "~/.rally/storage-mirrors.json"
MAX_CONNECTIONS = 8


class Client(Adapter):
    """It handles client instances allocation allowing reusing pre-allocated instances from the same thread."""

    @classmethod
    def from_config(cls, cfg: config.Config) -> Client:
        max_connections = int(cfg.opts(section="storage", key="storage.max_connections", default_value=MAX_CONNECTIONS, mandatory=False))
        random_seed = cfg.opts(section="storage", key="storage.random_seed", default_value=None, mandatory=False)
        random = None
        if random_seed is not None:
            random = Random(random_seed)
        return cls(
            max_connections=max_connections, mirrors=MirrorList.from_config(cfg), adapters=AdapterRegistry.from_config(cfg), random=random
        )

    def __init__(
        self,
        adapters: AdapterRegistry,
        mirrors: MirrorList,
        random: Random | None = None,
        max_connections: int = MAX_CONNECTIONS,
    ):
        if random is None:
            random = Random(time.time())
        self._adapters: AdapterRegistry = adapters
        self._cached_heads: dict[str, tuple[Head | Exception, float]] = {}
        self._connections: dict[str, WaitGroup] = defaultdict(lambda: WaitGroup(max_count=max_connections))
        self._lock = threading.Lock()
        self._mirrors: MirrorList = mirrors
        self._random: Random = random
        self._stats: dict[str, deque[ServerStats]] = defaultdict(lambda: deque(maxlen=100))

    def head(self, url: str, ttl: float | None = None) -> Head:
        """It gets remote file headers."""

        start_time = time.monotonic()
        if ttl is not None:
            # when time-to-leave is given, it looks up for pre-cached head first
            try:
                value, last_time = self._cached_heads[url]
            except KeyError:
                pass
            else:
                if start_time <= last_time + ttl:
                    # cached value or error is enough recent to be used.
                    return _head_or_raise(value)

        adapter = self._adapters.get(url)
        try:
            value = adapter.head(url)
        except Exception as ex:
            LOG.error("Failed to fetch remote head for file: %s, %s", url, ex)
            value = ex
        end_time = time.monotonic()

        with self._lock:
            # It records per-server time statistics.
            self._stats[_server_key(url)].append(ServerStats(url, start_time, end_time))
            # The cached value could be an exception, or a head. In this way it will not retry previously failed
            # urls until the TTL expires.
            self._cached_heads[url] = value, start_time

        return _head_or_raise(value)

    def resolve(
        self, url: str, document_length: int | None = None, crc32c: str | None = None, accept_ranges: bool = False, ttl: float = 60.0
    ) -> Iterator[Head]:
        """It looks up mirror list for given URL and yield mirror heads.
        :param url: the remote file URL at its mirrored source location.
        :param document_length: if not none it will filter out mirrors which file has an unexpected document lengths.
        :param crc32c: if not none it will filter out mirrors which file has an unexpected crc32c checksum.
        :param accept_ranges: if True it will filter out mirrors that are not supporting ranges.
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

        if len(urls) > 1:
            LOG.debug("resolved mirror URLs for URL '%s': %s", url, urls)

        for u in urls:
            try:
                head = self.head(u, ttl=ttl)
            except Exception:
                # The exception is already logged by head method before caching it.
                continue
            if document_length is not None and head.document_length is not None and document_length != head.document_length:
                LOG.debug("unexpected document length: %r, got %d, want %d", url, head.document_length, document_length)
                continue
            if crc32c is not None and head.crc32c is not None and head.crc32c != crc32c:
                LOG.debug("unexpected crc32c checksum: %r, got %d, want %d", url, head.crc32c, crc32c)
                continue
            if accept_ranges and not head.accept_ranges:
                LOG.debug("it doesn't accept ranges: %r", url)
                continue
            yield head

    def get(
        self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE, document_length: int | None = None, crc32c: str | None = None
    ) -> Head:
        """It downloads a remote bucket object to a local file path.

        :param url: the URL of the remote file.
        :param stream: the destination file stream where to write data to.
        :param document_length: the document length of the file to transfer.
        :param crc32c: the crc32c checksum of the file to transfer.
        :param ranges: the portion of the file to transfer.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """
        for head in self.resolve(url, accept_ranges=bool(ranges), document_length=document_length, crc32c=crc32c):
            connections = self._server_connections(head.url)
            try:
                connections.add(1)
            except WaitGroupLimitError:
                LOG.debug("connection limit exceeded: url='%s'", url)
                continue
            adapter = self._adapters.get(head.url)
            try:
                return adapter.get(head.url, stream, ranges)
            except ServiceUnavailableError as ex:
                LOG.debug("service unavailable error received: url='%s' %s", url, ex)
                with self._lock:
                    # It corrects the maximum number of connections for this server.
                    connections.max_count = max(1, connections.count)
            finally:
                connections.done()

        raise ServiceUnavailableError(f"no service available for getting URL '{url}'")

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
