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
from collections.abc import Iterable, Iterator
from typing import TypeVar
from urllib.parse import urlparse
from xml.etree import ElementTree

import requests

import esrally.config
from esrally.storage import (
    AdapterRegistry,
    Client,
    Executor,
    Head,
    HTTPAdapter,
    ThreadPoolExecutor,
    TransferManager,
)
from esrally.types import Config

A = TypeVar("A")


class TracksRepositoryAdapter(HTTPAdapter):

    REPOSITORY_URL = "https://rally-tracks.elastic.co/"

    _XML_CONTENT_TYPE = "application/xml"
    _XML_ENCODING_PREFIX = "charset="
    _XML_NODE_PREFIX = "{http://doc.s3.amazonaws.com/2006-03-01}"
    _XML_NODE_CONTENTS = f"{_XML_NODE_PREFIX}Contents"
    _XML_NODE_KEY = f"{_XML_NODE_PREFIX}Key"
    _XML_NODE_SIZE = f"{_XML_NODE_PREFIX}Size"

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith(cls.REPOSITORY_URL)

    def list(self, url: str) -> Iterator[Head]:
        encoding = "utf-8"
        LOG.debug("Getting file list from '%s'...", url)
        with requests.get(url, timeout=60.0) as res:
            res.raise_for_status()
            content_type = res.headers.get("content-type", "").replace(" ", "")
            if content_type:
                if ";" in content_type:
                    content_type, *others = content_type.split(";")
                    for other in others:
                        if other.startswith(self._XML_ENCODING_PREFIX):
                            encoding = other[len(self._XML_ENCODING_PREFIX) :].lower()
                            break
            if content_type != self._XML_CONTENT_TYPE:
                raise ValueError(f"Invalid content type: '{content_type}' != 'application/xml'")
            content = res.content.decode(encoding)

        LOG.debug("Parsing file list from '%s'...", url)
        base_url = url.rstrip("/") + "/"
        root = ElementTree.fromstring(content)
        for node in root.iter(self._XML_NODE_CONTENTS):
            key = node.find(self._XML_NODE_KEY)
            if key is None or not key.text or key.text.endswith("/"):
                # Skip invalid nodes or directories
                continue

            content_length = None
            size = node.find(self._XML_NODE_SIZE)
            if size is not None and size.text:
                content_length = int(size.text)
                if not content_length:
                    continue
            file_url = base_url + key.text
            LOG.debug("Found track file '%s' (size=%s) ...", file_url, content_length)
            yield Head(url=file_url, content_length=content_length)


BUCKET_URLS = (
    "s3://es-perf-fressi-eu-west-1/",
    "s3://es-perf-fressi-us-west-1/",
)

LOG = logging.getLogger(__name__)


class Sync:

    @classmethod
    def from_config(
        cls,
        cfg: Config,
        adapters: AdapterRegistry | None = None,
        client: Client | None = None,
        executor: Executor | None = None,
        manager: TransferManager | None = None,
    ) -> Sync:
        if client is None:
            client = Client.from_config(cfg, adapters=adapters)
        client.adapters.register_class(TracksRepositoryAdapter, position=0)
        if executor is None:
            executor = ThreadPoolExecutor.from_config(cfg)
        if manager is None:
            manager = TransferManager.from_config(cfg, client=client, executor=executor)

        bucket_urls = cfg.opts("storage", "track.bucket.urls", BUCKET_URLS, mandatory=False)
        index_url = cfg.opts("storage", "track.repository.url", TracksRepositoryAdapter.REPOSITORY_URL, mandatory=False)
        return cls(client=client, executor=executor, manager=manager, index_url=index_url, bucket_urls=bucket_urls)

    def __init__(self, client: Client, executor: Executor, manager: TransferManager, index_url: str, bucket_urls: Iterable[str]) -> None:
        self._client = client
        self._executor = executor
        self._manager = manager
        self._index_url = index_url
        if isinstance(bucket_urls, str):
            bucket_urls = bucket_urls.split(",")
        self._bucket_urls = [u.strip().rstrip("/") + "/" for u in bucket_urls]

    def run(self) -> None:
        for head in self._client.list(self._index_url):
            assert head.url is not None
            url: str = head.url  # It makes mypy happy
            try:
                head = self._client.head(url)
            except Exception as ex:
                LOG.error("Failed fetching head from '%s': %s", url, ex)

            path = urlparse(url).path.lstrip("/")
            bucket_urls = {u + path for u in self._bucket_urls}
            for bucket_url in list(bucket_urls):
                try:
                    got = self._client.head(bucket_url)
                    head.check(got)
                    bucket_urls.remove(bucket_url)
                except Exception as ex:
                    LOG.debug("File '%s' need to be updated: %s", bucket_url, ex)

            if not bucket_urls:
                LOG.info("File '%s' already uploaded on buckets.", url)
                continue

            tr1 = self._manager.get(url)
            tr1.wait()
            for bucket_url in bucket_urls:
                self._client.put(tr1.path, bucket_url, head=head)


def main():
    logging.basicConfig(level=logging.INFO)
    sync = Sync.from_config(esrally.config.Config())
    sync.run()


if __name__ == "__main__":
    main()
