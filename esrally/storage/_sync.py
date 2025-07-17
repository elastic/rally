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
from urllib.parse import urlparse

from esrally import config
from esrally.config import Config
from esrally.storage._adapter import AdapterRegistry
from esrally.storage._client import Client
from esrally.storage._executor import Executor, ThreadPoolExecutor
from esrally.storage._manager import TransferManager

INDEX_URL = "https://rally-tracks.elastic.co/"
# BUCKET_URL = "s3://es-perf-fressi-eu-central-1/"
BUCKET_URL = "s3://es-perf-fressi-us-west-1/"

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

        index_url = cfg.opts("storage", "storage.sync.index_url", INDEX_URL, mandatory=False)
        bucket_url = cfg.opts("storage", "storage.sync.bucket_url", BUCKET_URL, mandatory=False)
        return cls(client=client, executor=executor, manager=manager, index_url=index_url, bucket_url=bucket_url)

    def __init__(self, client: Client, executor: Executor, manager: TransferManager, index_url: str, bucket_url: str) -> None:
        self._client = client
        self._executor = executor
        self._manager = manager
        self._index_url = index_url
        self._bucket_url = bucket_url.strip().rstrip("/") + "/"

    def start(self) -> None:
        for head in self._client.list(self._index_url):
            assert head.url is not None
            url: str = head.url  # It makes mypy happy
            try:
                head = self._client.head(url)
            except Exception as ex:
                LOG.error("Failed fetching head from '%s': %s", url, ex)

            path = urlparse(url).path.lstrip("/")
            bucket_url = self._bucket_url + path

            try:
                got = self._client.head(bucket_url)
                head.check(got)
            except Exception as ex:
                LOG.error("Failed fetching head from '%s': %s", bucket_url, ex)

            tr1 = self._manager.get(url)
            tr1.wait()
            self._client.put(tr1.path, bucket_url, head=head)


def main():
    logging.basicConfig(level=logging.INFO)
    cfg = config.Config()
    cfg.add(config.Scope.application, "storage", "storage.mirrors_files", MIRRORS_FILES)
    sync = Sync.from_config(cfg)
    sync.start()


if __name__ == "__main__":
    main()
