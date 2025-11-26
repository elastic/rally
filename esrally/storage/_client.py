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

import dataclasses
import logging
import threading
import time
import urllib.parse
from collections import defaultdict, deque
from collections.abc import Generator
from random import Random
from typing import Literal

from typing_extensions import Self

from esrally import types
from esrally.storage._adapter import (
    AdapterRegistry,
    GetResponse,
    Head,
    ServiceUnavailableError,
)
from esrally.storage._config import StorageConfig
from esrally.storage._mirror import MirrorList
from esrally.utils import pretty
from esrally.utils.threads import WaitGroup, WaitGroupLimitError

LOG = logging.getLogger(__name__)


class CachedHeadError(Exception):
    """CachedHeadError is intended to wrap another exception.

    It is being raised when an attempt is made to retrieve a head from the cache to handle the case of recent past
    failure fetching the same head.
    """


@dataclasses.dataclass
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
            # It creates a CachedHeadError to wraps error with the stack trace sets to here. So it will be nice to see
            # the chain of exceptions to understand what gone one when debugging an exception borrowed from the cache.
            # The wrapping of the original error is required to allow triggering a special handling when the error is
            # recovered from an old cached request, or is got from an actual request.
            try:
                raise CachedHeadError(f"Cached error: {error}") from error
            except CachedHeadError as ex:
                self.error = ex
        else:
            raise ValueError("must specify either head or error")

    def get(self, *, cache_ttl: float | None = None) -> Head:
        if cache_ttl is not None:
            if time.monotonic() > self.timestamp + cache_ttl:
                raise TimeoutError("cached head has expired")
        if self.error is not None:
            raise self.error
        assert self.head is not None
        return self.head


@dataclasses.dataclass
class MirrorFailure:
    url: str
    mirror_url: str
    error: str
    timestamp: float = 0.0


class Client:
    """It handles client instances allocation allowing reusing pre-allocated instances from the same thread."""

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        return cls(
            adapters=AdapterRegistry.from_config(cfg),
            mirrors=MirrorList.from_config(cfg),
            random=Random(cfg.random_seed),
            max_connections=cfg.max_connections,
            cache_ttl=cfg.cache_ttl,
        )

    def __init__(
        self,
        adapters: AdapterRegistry,
        mirrors: MirrorList,
        random: Random | None = None,
        max_connections: int = StorageConfig.DEFAULT_MAX_CONNECTIONS,
        cache_ttl: float = StorageConfig.DEFAULT_CACHE_TTL,
    ):
        self._adapters: AdapterRegistry = adapters
        self._cached_heads: dict[str, CachedHead] = {}
        self._connections: dict[str, WaitGroup] = defaultdict(lambda: WaitGroup(max_count=max_connections))
        self._lock = threading.Lock()
        self._mirrors: MirrorList = mirrors
        self._random: Random = random or Random(StorageConfig.DEFAULT_RANDOM_SEED)
        self._request_stats: dict[str, deque[RequestStat]] = defaultdict(lambda: deque(maxlen=10000))
        self._cache_ttl: float = cache_ttl
        self._mirror_failures: dict[str, dict[str, MirrorFailure]] = defaultdict(dict)

    @property
    def adapters(self):
        return self._adapters

    def head(self, url: str, *, cache_ttl: float | None = None) -> Head:
        """It gets remote file headers."""
        if cache_ttl is None:
            cache_ttl = self._cache_ttl
        if cache_ttl > 0.0:
            # When the time-to-live is given, it looks up for pre-cached head first.
            try:
                return self._cached_heads[url].get(cache_ttl=cache_ttl)
            except (KeyError, TimeoutError):
                # Cached head is missing, or it has expired.
                pass

        try:
            # It sends HEAD request.
            head = self._head(url)
        except Exception as ex:
            # It caches the error.
            with self._lock:
                # The cached value could be an exception. In this way it will not retry previously failed urls until
                # the TTL expires.
                self._cached_heads[url] = CachedHead(time.monotonic(), error=ex)
            raise

        # It caches the response.
        self._cached_heads[url] = CachedHead(time.monotonic(), head=head)
        return head

    def _head(self, url: str) -> Head:
        """It sends HEAD request recording statistics for later use."""
        adapter = self._adapters.get(url)
        stat = RequestStat("HEAD", url)
        try:
            with stat:
                return adapter.head(url)
        finally:
            with self._lock:
                # It makes statistics available for load balancing following requests.
                self._request_stats[_server_key(url)].append(stat)

    def resolve(self, url: str, *, check_head: Head | None = None, cache_ttl: float | None = None) -> Generator[Head]:
        """It looks up mirror list for given URL and yield mirror heads.
        :param url: the remote file URL at its mirrored source location.
        :param check_head: extra parameters to mach remote heads.
            - document_length: if not none it will filter out mirrors which file has an unexpected document lengths.
            - crc32c: if not none it will filter out mirrors which file has an unexpected crc32c checksum.
            - accept_ranges: if True it will filter out mirrors that are not supporting ranges.
        :param cache_ttl: the time to live value (in seconds) to use for cached heads retrieval.
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
                got = self.head(u, cache_ttl=cache_ttl)
                if check_head is not None:
                    check_head.check(got)
            except CachedHeadError:
                # The error was previously cached, therefore it has been treated.
                continue
            except Exception as ex:
                if u == url:
                    LOG.warning("Failed to get head from original URL: '%s', %s", u, ex)
                    continue

                # It saves mirror failures so that we can later fix the failure after benchmark execution on CI.
                LOG.warning("Failed to get head from mirror URL: '%s': %s", u, ex)
                self._mirror_failures[url][u] = MirrorFailure(
                    url=url, error=f"{type(ex).__name__}:{ex}", mirror_url=u, timestamp=time.time()
                )
                continue

            if u != url and url in self._mirror_failures:
                LOG.debug("Find valid mirror URL: '%s' -> '%s'", url, u)
                # It eventually removes a temporary mirror failure that has fixed himself.
                self._mirror_failures[url].pop(u, None)

            yield got

    def mirror_failures(self, url: str) -> list[MirrorFailure]:
        """Returns failures that have been reported after fetching HEAD from mirror URLs."""
        return list(self._mirror_failures[url].values())

    def get(self, url: str, *, check_head: Head | None = None) -> GetResponse:
        """It downloads a remote bucket object to a local file path.

        :param url: the URL of the remote file.
        :param check_head: extra params for getting the file:
            - document_length: the document length of the file to transfer.
            - crc32c: the crc32c checksum of the file to transfer.
            - ranges: the portion of the file to transfer.
        :raises ServiceUnavailableError: in case on temporary service failure.
        """
        if check_head is None:
            resolve_head = None
        elif check_head.ranges:
            resolve_head = Head(
                accept_ranges=True, content_length=check_head.document_length, date=check_head.date, crc32c=check_head.crc32c
            )
        else:
            resolve_head = Head(url=url, content_length=check_head.content_length, date=check_head.date, crc32c=check_head.crc32c)

        # It iterates heads from all servers
        for head in self.resolve(url, check_head=resolve_head):
            # It checks connections counter for given URL.
            wg = self._server_connections(head.url)
            try:
                wg.add(1)
            except WaitGroupLimitError:
                LOG.debug("Connection limit exceeded for url '%s'", head.url)
                continue

            try:
                got = self._get(head.url, check_head=check_head)
            except ServiceUnavailableError as ex:
                LOG.warning("service unavailable error received: url='%s' %s", url, ex)
                with self._lock:
                    # It corrects the maximum number of connections for this server.
                    wg.max_count = max(1, wg.count)
                wg.done()
                continue

            def iter_chunks(wg: WaitGroup, chunks: Generator[bytes]) -> Generator[bytes]:
                # It wraps chunks generator so it do release connections wait group once response has been processed.
                try:
                    yield from chunks
                finally:
                    wg.done()
                    chunks.close()

            got.chunks = iter_chunks(wg, got.chunks)
            return got

        raise ServiceUnavailableError(f"no connections available for getting URL '{url}'")

    def _get(self, url: str, check_head: Head | None = None) -> GetResponse:
        """It sends GET request recording statistics for later use."""
        # It gets client adapter for given URL.
        adapter = self._adapters.get(url)
        stat = RequestStat("GET", url)
        try:
            with stat:
                # It will update statistics every time a chunk of data is read.
                got: GetResponse = adapter.get(url, check_head=check_head)
                got.chunks = stat.iterate_chunks(got.chunks)
                return got
        finally:
            with self._lock:
                # It makes statistics available for load balancing following requests.
                self._request_stats[_server_key(url)].append(stat)

    def _server_connections(self, url: str) -> WaitGroup:
        with self._lock:
            return self._connections[_server_key(url)]

    def _request_stats_from_url(self, url: str, *, ttl: float | None = None) -> deque[RequestStat]:
        with self._lock:
            stats = self._request_stats[_server_key(url)]
            if ttl is not None:
                # It removes expired statistics
                deadline = time.monotonic() - ttl
                while stats and stats[0].start_time < deadline:
                    stats.popleft()
        return stats

    def _average_latency(self, url: str, *, ttl: float | None = 60.0) -> float:
        stats = self._request_stats_from_url(url, ttl=ttl)
        if not stats:
            return 0.0
        return sum(s.latency for s in stats) / len(stats)

    def _average_throughput(self, url: str, *, ttl: float | None = 60.0) -> float:
        stats = self._request_stats_from_url(url, ttl=ttl)
        if not stats:
            return 0.0
        return sum(s.throughput for s in stats) / len(stats)

    def monitor(self):
        with self._lock:
            connections = {u: c for u, c in self._connections.items() if c.count > 0}
        infos = list[str]()
        for url, connection in sorted(connections.items()):
            latency = self._average_latency(url)
            throughput = self._average_throughput(url)
            infos.append(f"- '{url}' count={connection.count} latency={pretty.duration(latency)}, throughput={pretty.size(throughput)}/s")
        if len(infos) > 0:
            LOG.info("Active client connection(s):\n  %s", "\n  ".join(infos))


RequestMethod = Literal["GET", "HEAD"]


@dataclasses.dataclass
class RequestStat:
    """It recollects per request statistics."""

    # The method of the request.
    method: RequestMethod

    # The URL of the request.
    url: str

    # The moment the request started as returned from time.monotonic()
    start_time: float = dataclasses.field(default_factory=time.monotonic)

    # The time duration (in seconds) before receiving the head of the response.
    latency: float = 0.0

    # The time duration (in seconds) before receiving the last chunk of data.
    duration: float = 0.0

    # The number of bytes has been transferred processing the request.
    transferred_bytes: int = 0

    # Used to asynchronously update the statistics
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.latency = self.duration = time.monotonic() - self.start_time

    @property
    def throughput(self) -> float:
        with self._lock:
            return self.transferred_bytes / self.duration

    def iterate_chunks(self, chunks: Generator[bytes]) -> Generator[bytes]:
        """It wraps data chunk iterator to update statistics."""
        try:
            for chunk in chunks:
                with self._lock:
                    self.transferred_bytes += len(chunk)
                    self.duration = time.monotonic() - self.start_time
                yield chunk
        finally:
            chunks.close()


def _head_or_raise(value: Head | Exception) -> Head:
    if isinstance(value, Head):
        return value
    if isinstance(value, Exception):
        raise value
    raise TypeError(f"unexpected value type in cached heads: got {value}, expected Head | Exception")


def _server_key(url: str) -> str:
    u = urllib.parse.urlparse(url)
    return f"{u.scheme}://{u.netloc}/"
