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
from collections.abc import Iterable
from urllib.parse import urlparse

from esrally import config
from esrally.config import Config
from esrally.storage._adapter import AdapterRegistry
from esrally.storage._client import Client
from esrally.storage._executor import Executor, ThreadPoolExecutor
from esrally.storage._manager import TransferManager

INDEX_URL = "https://rally-tracks.elastic.co/"

BUCKET_URLS = (
    "s3://es-perf-fressi-eu-west-1/",
    "s3://es-perf-fressi-us-west-1/",
)

MIRRORS_FILES = "~/.rally/storage-mirrors.json"


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
        if executor is None:
            executor = ThreadPoolExecutor.from_config(cfg)
        if manager is None:
            manager = TransferManager.from_config(cfg, client=client, executor=executor)

        bucket_urls = cfg.opts("storage", "storage.sync.bucket_url", BUCKET_URLS, mandatory=False)
        index_url = cfg.opts("storage", "storage.sync.index_url", INDEX_URL, mandatory=False)
        return cls(client=client, executor=executor, manager=manager, index_url=index_url, bucket_urls=bucket_urls)

    def __init__(self, client: Client, executor: Executor, manager: TransferManager, index_url: str, bucket_urls: Iterable[str]) -> None:
        self._client = client
        self._executor = executor
        self._manager = manager
        self._index_url = index_url
        if isinstance(bucket_urls, str):
            bucket_urls = bucket_urls.split(",")
        self._bucket_urls = [u.strip().rstrip("/") + "/" for u in bucket_urls]

    def start(self) -> None:
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
                    LOG.debug("File '%s' need to be uploaded: %s", bucket_url, ex)

            if not bucket_urls:
                LOG.info("File '%s' already uploaded on buckets.", url)
                continue

            tr1 = self._manager.get(url)
            tr1.wait()
            for bucket_url in bucket_urls:
                self._client.put(tr1.path, bucket_url, head=head)


def main():
    logging.basicConfig(level=logging.INFO)
    cfg = config.Config()
    cfg.add(config.Scope.application, "storage", "storage.mirrors_files", MIRRORS_FILES)
    sync = Sync.from_config(cfg)
    sync.start()


if __name__ == "__main__":
    main()
