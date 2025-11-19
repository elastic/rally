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
import urllib.parse
from collections.abc import Generator

from google.cloud import storage  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally import types
from esrally.storage import NO_RANGE, Adapter, GetResponse, Head, StorageConfig

LOG = logging.getLogger(__name__)


class GSAdapter(Adapter):

    @classmethod
    def match_url(cls, url: str) -> bool:
        try:
            GSAddress.from_url(url)
            return True
        except ValueError:
            return False

    @classmethod
    def from_config(cls, cfg: types.Config) -> Self:
        cfg = StorageConfig.from_config(cfg)
        LOG.debug("Creating Google Cloud Storage adapter from config: %s...", cfg)
        try:
            client = storage.Client(project=cfg.google_cloud_project)
        except Exception as ex:
            LOG.error("Failed to create Google Cloud Storage adapter: %s", ex)
            raise
        return cls(client=client, chunk_size=cfg.chunk_size, user_project=cfg.google_cloud_user_project, multipart_size=cfg.multipart_size)

    def __init__(
        self,
        client: storage.Client,
        chunk_size: int = StorageConfig.DEFAULT_CHUNK_SIZE,
        multipart_size: int = StorageConfig.DEFAULT_MULTIPART_SIZE,
        user_project: str | None = StorageConfig.DEFAULT_GOOGLE_CLOUD_USER_PROJECT,
        connect_timeout: float = StorageConfig.DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = StorageConfig.DEFAULT_READ_TIMEOUT,
    ) -> None:
        self.client = client
        self.chunk_size = chunk_size
        self.multipart_size = multipart_size
        self.user_project = user_project
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

    def blob(self, url: str, fetch: bool = True) -> storage.Blob:
        # chunk_size value has to be multiple of 256K
        chunk_size = self.multipart_size + (self.multipart_size % (1024 * 256))
        b = GSAddress.from_url(url).blob(client=self.client, user_project=self.user_project, chunk_size=chunk_size)
        if fetch:
            b.reload(client=self.client, timeout=(self.connect_timeout, self.read_timeout))
        return b

    def head(self, url: str) -> Head:
        blob = self.blob(url)
        return Head(url=url, content_length=blob.size, accept_ranges=True, crc32c=blob.crc32c)

    def get(self, url: str, *, check_head: Head | None = None) -> GetResponse:
        ranges = check_head and check_head.ranges or NO_RANGE
        if len(ranges) > 1:
            raise ValueError("download range must be continuous")

        blob = self.blob(url)
        if ranges:
            head = Head(url=url, content_length=ranges.size, document_length=blob.size, crc32c=blob.crc32c, ranges=ranges)
        else:
            head = Head(url=url, content_length=blob.size, crc32c=blob.crc32c)

        if check_head is not None:
            check_head.check(head)

        def iter_chunks() -> Generator[bytes]:
            """It gets chunks from the buffer."""
            if ranges:
                pos, end = ranges.start, ranges.end
            else:
                pos, end = 0, blob.size

            while pos < end:
                with blob.open("rb") as fd:
                    while pos < end and (chunk := fd.read(min(self.chunk_size, end - pos))):
                        pos += len(chunk)
                        yield chunk

        return GetResponse(head, iter_chunks())


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
        bucket_name: str
        blob_name: str | None = None
        match u.scheme:
            case "gs":
                bucket_name = hostname
                blob_name = path
            case "https":
                if hostname != "storage.cloud.google.com":
                    raise ValueError(f"unexpected hostname: {url}")
                if "/" in path:
                    bucket_name, blob_name = path.split("/", maxsplit=1)
                else:
                    bucket_name = path
            case _:
                raise ValueError(f"Unsupported scheme: {u.scheme}")

        if not bucket_name:
            raise ValueError(f"unspecified bucket name in URL: {url}")
        if blob_name:
            blob_name = urllib.parse.unquote(blob_name)
        else:
            blob_name = None
        return cls(bucket_name=bucket_name, blob_name=blob_name)

    bucket_name: str
    blob_name: str | None = None

    def bucket(self, client: storage.Client | None = None, user_project: str | None = None) -> storage.Bucket:
        return storage.Bucket(client=client, name=self.bucket_name, user_project=user_project)

    def blob(self, client: storage.Client | None = None, user_project: str | None = None, chunk_size: int | None = None) -> storage.Blob:
        return self.bucket(client=client, user_project=user_project).blob(self.blob_name, chunk_size=chunk_size)
