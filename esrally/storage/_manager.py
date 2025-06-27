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

import concurrent.futures
import logging
import os
import threading

from esrally import config
from esrally.storage._client import MAX_CONNECTIONS, Client
from esrally.storage._range import NO_RANGE, RangeSet
from esrally.storage._transfer import Transfer
from esrally.utils.threads import ContinuousTimer

LOG = logging.getLogger(__name__)

LOCAL_DIR = "~/.rally/storage"
MONITOR_INTERVAL = 2.0  # Seconds
THREAD_NAME_PREFIX = "esrally.storage.transfer-worker"
MULTIPART_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 32


class Manager:
    """It creates and perform file transfer operations in background."""

    def __init__(
        self,
        client: Client,
        local_dir: str = LOCAL_DIR,
        monitor_interval: float = MONITOR_INTERVAL,
        multipart_size: int = MULTIPART_SIZE,
        max_workers: int = MAX_WORKERS,
        max_connections: int = MAX_CONNECTIONS,
    ):
        """It manages files transfers.

        It executes file transfers in background. It periodically logs the status of transfers that are in progress.

        :param local_dir: default directory used to download files to, or upload files from.
        :param monitor_interval: time interval (in seconds) separating background calls to the _monitor method.
        :param client: _client.Client instance used to allocate/reuse storage adapters.
        :param multipart_size: length of every part when working with multipart.
        :param max_workers: max number of connections per remote server when working with multipart.
        """
        self._client = client
        self._lock = threading.Lock()

        local_dir = os.path.expanduser(local_dir)
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)
        self._local_dir = local_dir
        if monitor_interval <= 0:
            raise ValueError(f"invalid monitor interval: {monitor_interval}")
        self._transfers: list[Transfer] = []
        self._max_workers = max(1, max_workers)
        self._max_connections = max(1, min(max_connections, max_workers))
        self._multipart_size = multipart_size
        self._executor: concurrent.futures.Executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="esrally.storage.transfer-worker"
        )
        self._monitor_timer = ContinuousTimer(interval=monitor_interval, function=self.monitor, name="esrally.storage.transfer-monitor")
        self._monitor_timer.start()

    def shutdown(self):
        with self._lock:
            transfers = self._transfers
            self._transfers = []
        for tr in transfers:
            tr.close()
        self._monitor_timer.cancel()

    @classmethod
    def from_config(cls, cfg: config.Config) -> Manager:
        """It creates a TransferManager with initialization values taken from given configuration."""
        local_dir = cfg.opts(section="storage", key="storage.local_dir", default_value=LOCAL_DIR, mandatory=False)
        monitor_interval = cfg.opts(section="storage", key="storage.monitor_interval", default_value=MONITOR_INTERVAL, mandatory=False)
        max_workers = cfg.opts(section="storage", key="storage.max_workers", default_value=MAX_WORKERS, mandatory=False)
        max_connections = cfg.opts(section="storage", key="storage.max_connections", default_value=MAX_CONNECTIONS, mandatory=False)
        multipart_size = cfg.opts(section="storage", key="storage.multipart_size", default_value=MULTIPART_SIZE, mandatory=False)
        return cls(
            local_dir=local_dir,
            monitor_interval=float(monitor_interval),
            multipart_size=multipart_size,
            max_workers=max_workers,
            max_connections=int(max_connections),
            client=Client.from_config(cfg),
        )

    def get(self, url: str, path: os.PathLike | str | None = None, ranges: RangeSet = NO_RANGE) -> Transfer:
        """It starts a new transfer of a file from local path to a remote url.

        :param url: remote file address.
        :param path: local file address.
        :param ranges: the part of the file to download
        :return: started transfer object.
        """
        return self._transfer(url=url, path=path, ranges=ranges)

    def _transfer(
        self,
        url: str,
        path: os.PathLike | str | None = None,
        ranges: RangeSet = NO_RANGE,
    ) -> Transfer:
        if path is None:
            path = os.path.join(self._local_dir, url)
        # This also ensures the path is a string
        path = os.path.normpath(os.path.expanduser(path))

        head = self._client.head(url)
        # This also ensures the path is a string
        tr = Transfer(
            client=self._client,
            url=url,
            path=path,
            document_length=head.content_length,
            executor=self._executor,
            todo=ranges,
            max_connections=self._max_connections,
            multipart_size=self._multipart_size,
            crc32c=head.crc32c,
        )
        with self._lock:
            self._transfers.append(tr)
            self._update_transfers()
        tr.start()
        return tr

    def monitor(self):
        with self._lock:
            transfers = self._transfers
            # It removes finished transfers and update max connections
            self._update_transfers()
        if transfers:
            LOG.info("Transfers in progress:\n  %s", "\n  ".join(tr.info() for tr in transfers))
        self._client.monitor()

    def _update_transfers(self):
        available_workers = max(1, int(self._max_workers * 0.8))
        self._transfers = transfers = [tr for tr in self._transfers if not tr.finished]
        if transfers:
            max_connections = min(self._max_connections, max(1, available_workers // len(transfers)))
            for tr in transfers:
                tr.max_connections = max_connections
                tr.save_status()
                tr.start()
