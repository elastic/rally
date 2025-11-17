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
import dataclasses
import logging
import os
import queue
import urllib.parse
from collections.abc import Generator, Iterator
from typing import Any

from google.cloud import storage as gcs  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally import storage, types

LOG = logging.getLogger(__name__)


class GSAdapter(storage.Adapter):

    @classmethod
    def match_url(cls, url: str) -> bool:
        try:
            GSAddress.from_url(url)
            return True
        except ValueError:
            return False

    @classmethod
    def from_config(cls, cfg: types.Config) -> Self:
        cfg = storage.StorageConfig.from_config(cfg)
        LOG.debug("Creating Google Cloud Storage adapter from config: %s...", cfg)
        try:
            client = gcs.Client(project=cfg.google_cloud_project)
        except Exception as ex:
            LOG.error("Failed to create Google Cloud Storage adapter: %s", ex)
            raise
        executor = storage.executor_from_config(cfg)
        return cls(client=client, executor=executor, chunk_size=cfg.chunk_size)

    def __init__(
        self,
        client: gcs.Client,
        executor: storage.Executor,
        chunk_size: int = storage.StorageConfig.DEFAULT_CHUNK_SIZE,
        user_project: str | None = None,
    ) -> None:
        self.client = client
        self.executor = executor
        self.chunk_size = chunk_size
        self.user_project = user_project
        self.buffer_size = 10
        self.read_timeout = 10.0

    def head(self, url: str) -> storage.Head:
        blob = self._get_blob(url)
        return storage.Head(url=url, content_length=blob.size, accept_ranges=True, crc32c=blob.crc32c)

    def get(self, url: str, *, check_head: storage.Head | None = None) -> tuple[storage.Head, Iterator[bytes]]:
        blob = self._get_blob(url)
        ranges = check_head and check_head.ranges or storage.NO_RANGE
        if len(ranges) > 1:
            raise ValueError("download range must be continuous")

        if ranges:
            head = storage.Head(url=url, content_length=ranges.size, document_length=blob.size, crc32c=blob.crc32c, ranges=ranges)
        else:
            head = storage.Head(url=url, content_length=blob.size, crc32c=blob.crc32c)

        if check_head is not None:
            check_head.check(head)

        # It spawns a thread that reads data chunk by chunk and puts it in a buffer queue.
        buffer = Buffer(maxsize=self.buffer_size)

        def download_chunks():
            """It downloads file chunks to the buffer before shutdown it."""
            try:
                params: dict[str, Any] = {}
                if ranges:
                    params["start"] = ranges.start
                    params["end"] = ranges.end - 1
                blob.download_to_file(client=self.client, file_obj=buffer, **params)
            finally:
                # It signal chunks iterator the download is terminated.
                buffer.shutdown()

        # It runs the download in an executor threads which will put chunks to the buffer.
        fut = self.executor.submit(download_chunks)

        def iter_chunks() -> Generator[bytes]:
            """It gets chunks from the buffer."""
            try:
                yield from buffer.iter_chunks(timeout=self.read_timeout)
            finally:
                # Eventually raises exceptions produced in downloader thread.
                fut.result()

        return head, iter_chunks()

    def _get_blob(self, url: str) -> gcs.Blob:
        """It fetches file headers from server."""
        address = GSAddress.from_url(url)
        bucket = self.client.bucket(address.bucket, user_project=self.user_project)
        blob = bucket.get_blob(address.blob)
        if blob is None:
            raise FileNotFoundError(f"No such blob: {address}")
        return blob


class _BufferShutDown(RuntimeError):
    pass


class Buffer(queue.Queue[bytes]):
    # shutdown() method has been existing only since Python 3.13. Here we mimic its behavior so that this queue works
    # with earlier versions.

    ShutDown = getattr(queue, "ShutDown", _BufferShutDown)

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self.is_shutdown = False

    def write(self, data: bytes) -> None:
        if data:
            self.put(data)

    def put(self, item: bytes, block: bool = True, timeout: float | None = None) -> None:
        if self.is_shutdown:
            raise self.ShutDown
        super().put(item, block, timeout)

    def get(self, block: bool = True, timeout: float | None = None) -> bytes:
        chunk = super().get(block=block, timeout=timeout)
        if not chunk and self.is_shutdown:
            raise self.ShutDown
        return chunk

    def shutdown(self, _immediate: bool = False) -> None:
        with self.mutex:
            self.is_shutdown = True
            self.not_full.notify()
            self.not_empty.notify()

    def iter_chunks(self, timeout: float | None = None) -> Generator[bytes]:
        try:
            while True:
                yield self.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("Timed out reading chunks")
        except self.ShutDown:
            pass


@dataclasses.dataclass
class GSAddress:

    @classmethod
    def from_url(cls, url: str) -> Self:
        url = url.strip()
        if not url:
            raise ValueError("unspecified remote file url")

        u = urllib.parse.urlparse(url, scheme="gcs")
        hostname: str = u.netloc
        path: str = os.path.normpath(u.path).strip("/")
        match u.scheme:
            case "gs":
                bucket = hostname
                blob = path
            case "https":
                if hostname != "storage.cloud.google.com":
                    raise ValueError(f"unexpected hostname: {url}")
                if "/" not in path:
                    raise ValueError(f"invalid file path: {url}")
                bucket, blob = path.split("/", maxsplit=1)
            case _:
                raise ValueError(f"Unsupported scheme: {u.scheme}")

        if not bucket:
            raise ValueError(f"unspecified bucket name in URL: {url}")
        if not blob:
            raise ValueError(f"unspecified blob name in URL: {url}")

        return cls(bucket=bucket, blob=blob)

    bucket: str
    blob: str
