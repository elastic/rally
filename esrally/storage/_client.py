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
import os.path
import threading
import time
import urllib.parse
from collections import defaultdict, deque
from collections.abc import Iterable, Iterator
from random import Random
from typing import NamedTuple

from esrally import config
from esrally.storage._adapter import (
    Adapter,
    Head,
    ServiceUnavailableError,
    Writable,
    adapter_class,
    adapter_classes,
)
from esrally.storage._mirror import Mirror
from esrally.storage._range import NO_RANGE, RangeSet
from esrally.utils.threads import WaitGroup, WaitGroupLimitError

LOG = logging.getLogger(__name__)

MIRRORS_FILES = "~/.rally/storage/mirrors.yml"
MAX_CONNECTIONS = 4


class Client(Adapter):
    """It handles client instances allocation allowing reusing pre-allocated instances from the same thread."""

    def __init__(
        self,
        adapter_classes: dict[str, type[Adapter]] | None = None,
        mirrors: Iterable[Mirror] = tuple(),
        random: Random | None = None,
        max_connections: int = MAX_CONNECTIONS,
    ):
        if random is None:
            random = Random(time.time())
        self._adapters: dict[str, Adapter] = {}
        self._adapter_classes: dict[str, type[Adapter]] | None = adapter_classes
        self._cached_heads: dict[str, tuple[Head | Exception, float]] = {}
        self._connections: dict[str, WaitGroup] = defaultdict(lambda: WaitGroup(max_count=max_connections))
        self._lock = threading.Lock()
        self._mirrors: list[Mirror] = list(mirrors)
        self._random: Random = random
        self._stats: dict[str, deque[ServerStats]] = defaultdict(lambda: deque(maxlen=100))

    @classmethod
    def from_config(cls, cfg: config.Config) -> Client:
        adapter_names = cfg.opts(section="storage", key="storage.adapter_names", default_value="", mandatory=False)
        max_connections = int(cfg.opts(section="storage", key="storage.max_connections", default_value=MAX_CONNECTIONS, mandatory=False))
        mirrors = []
        for filename in cfg.opts(section="storage", key="storage.mirrors_files", default_value=MIRRORS_FILES, mandatory=False).split(","):
            filename = filename.strip()
            if filename:
                filename = os.path.expanduser(filename)
                if os.path.isfile(filename):
                    mirrors.append(Mirror.from_file(filename))
        return cls(adapter_classes=adapter_classes(names=adapter_names), mirrors=mirrors, max_connections=max_connections)

    def head(self, url: str, ttl: float | None = None) -> Head:
        """It gets remote file headers."""

        start_time = time.monotonic_ns()
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

        try:
            value = self._adapter(url).head(url)
        except Exception as ex:
            value = ex
        end_time = time.monotonic_ns()

        with self._lock:
            # It records per-server time statistics.
            self._stats[_server_key(url)].append(ServerStats(url, start_time, end_time))
            # The cached value could be an exception, or a head. In this way it will not retry previously failed
            # urls until the TTL expires.
            self._cached_heads[url] = value, start_time

        return _head_or_raise(value)

    def resolve(self, url: str, content_length: int | None = None, accept_ranges: bool = False, ttl: float = 60.0) -> Iterator[Head]:
        """It looks up mirror list for given URL and yield mirror heads.
        :param url:
        :param content_length: if not none it will filter out mirrors with a wrong content length.
        :param accept_ranges: if True it will filter out mirrors that are not supporting ranges.
        :param ttl: the time to live value (in seconds) to use for cached heads retrieval.
        :return: iterator over mirror URLs
        """

        mirror_urls = list({u for m in self._mirrors for u in m.urls(url)})
        if len(mirror_urls) == 0:
            yield self.head(url, ttl=ttl)
            return

        if len(mirror_urls) == 1:
            yield self.head(mirror_urls[0], ttl=ttl)
            return

        # It makes sure workers will not pick the same URLs as first mirror option.
        self._random.shuffle(mirror_urls)

        # It uses measured latencies and the number of connections to affect the order of the mirrors.
        weights = {u: self._average_latency(u) * self._random.uniform(1.0, self._server_connections(url).count) for u in mirror_urls}
        # LOG.debug("weights: %s", weights)
        mirror_urls.sort(key=lambda u: weights[u])
        # LOG.debug("mirror_urls: %s", mirror_urls)

        for u in mirror_urls:
            try:
                head = self.head(u, ttl=ttl)
            except FileNotFoundError:
                LOG.debug("file not found: %r", url)
                continue
            except Exception as ex:
                LOG.debug("error getting file head: %r, %s", url, ex)
                continue
            if content_length is not None and content_length != head.content_length:
                LOG.debug("unexpected content length: %r, got %d, want %d", url, content_length, head.content_length)
                continue
            if accept_ranges and not head.accept_ranges:
                LOG.debug("it doesn't accept ranges: %r", url)
                continue
            yield head

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        """It downloads a remote bucket object to a local file path.

        :param url: it represents the URL of the remote file.
        :param stream: it represents the destination file stream where to write data to.
        :param ranges: it represents the portion of the file to transfer.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """
        for head in self.resolve(url, accept_ranges=bool(ranges)):
            connections = self._server_connections(head.url)
            try:
                connections.add(1)
            except WaitGroupLimitError:
                continue
            try:
                return self._adapter(head.url).get(head.url, stream, ranges)
            except ServiceUnavailableError:
                with self._lock:
                    connections.max_count = max(1, connections.count - 1)
            finally:
                connections.done()

        raise ServiceUnavailableError(f"any service available for getting {url}")

    def _server_connections(self, url: str) -> WaitGroup:
        with self._lock:
            return self._connections[_server_key(url)]

    def _server_stats_from_url(self, url: str, ttl: float | None = None) -> deque[ServerStats]:
        with self._lock:
            stats = self._stats[_server_key(url)]
            if ttl is not None:
                # it removes expired latencies
                deadline = time.monotonic_ns() - ttl
                while stats and stats[0].end < deadline:
                    stats.popleft()
        return stats

    def _average_latency(self, url: str, ttl: float | None = 60.0) -> float:
        stats = self._server_stats_from_url(url, ttl)
        if not stats:
            return 0.0
        return sum(s.latency for s in stats) / len(stats)

    def _adapter(self, url: str) -> Adapter:
        """It obtains an adapter instance for given protocol.

        It will return the same client instance when re-called with the same protocol id from the same thread. Returned
        client is intended to be used only from the calling thread.

        :param url: the target url the client is being created for.
        :return: a client instance for given protocol.
        """
        with self._lock:
            adapter = self._adapters.get(url)
            if adapter is None:
                # It uses registered classes mapping to create a client instance for each required protocol.

                # Given the same prefix it will return the same instance later when re-called from the same thread.
                cls = adapter_class(url=url, classes=self._adapter_classes)
                self._adapters[url] = adapter = cls.from_url(url)
        return adapter

    def monitor(self):
        for url, connections in self._connections.items():
            if connections.count > 0:
                latency = self._average_latency(url)
                LOG.info("active client connection(s) %s: count=%d, latency=%f", url, connections.count, latency)


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
