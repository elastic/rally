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

import atexit
import logging
import os
import threading

from typing_extensions import Self

from esrally import types
from esrally.storage._client import MAX_CONNECTIONS, Client
from esrally.storage._executor import MAX_WORKERS, Executor, ThreadPoolExecutor
from esrally.storage._transfer import MULTIPART_SIZE, Transfer
from esrally.utils.threads import ContinuousTimer

LOG = logging.getLogger(__name__)

LOCAL_DIR = "~/.rally/storage"
MONITOR_INTERVAL = 2.0  # Seconds
THREAD_NAME_PREFIX = "esrally.storage.transfer-worker"


class TransferManager:
    """It creates and perform file transfer operations in background."""

    @classmethod
    def from_config(cls, cfg: types.Config, client: Client | None = None, executor: Executor | None = None) -> Self:
        """It creates a TransferManager with initialization values taken from given configuration."""
        local_dir = cfg.opts(section="storage", key="storage.local_dir", default_value=LOCAL_DIR, mandatory=False)
        monitor_interval = cfg.opts(section="storage", key="storage.monitor_interval", default_value=MONITOR_INTERVAL, mandatory=False)
        max_connections = cfg.opts(section="storage", key="storage.max_connections", default_value=MAX_CONNECTIONS, mandatory=False)
        max_workers = cfg.opts(section="storage", key="storage.max_workers", default_value=MAX_WORKERS, mandatory=False)
        multipart_size = cfg.opts(section="storage", key="storage.multipart_size", default_value=MULTIPART_SIZE, mandatory=False)
        if client is None:
            client = Client.from_config(cfg)
        if executor is None:
            executor = ThreadPoolExecutor.from_config(cfg)
        return cls(
            client=client,
            executor=executor,
            local_dir=local_dir,
            monitor_interval=float(monitor_interval),
            multipart_size=multipart_size,
            max_connections=int(max_connections),
            max_workers=int(max_workers),
        )

    def __init__(
        self,
        client: Client,
        executor: Executor,
        local_dir: str = LOCAL_DIR,
        monitor_interval: float = MONITOR_INTERVAL,
        max_connections: int = MAX_CONNECTIONS,
        max_workers: int = MAX_WORKERS,
        multipart_size: int = MULTIPART_SIZE,
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
        self._executor = executor
        self._lock = threading.Lock()

        local_dir = os.path.expanduser(local_dir)
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)

        self._local_dir = local_dir
        if monitor_interval <= 0:
            raise ValueError(f"invalid monitor interval: {monitor_interval} <= 0")

        self._transfers: list[Transfer] = []

        if max_workers < 1:
            raise ValueError(f"invalid max_workers: {max_workers} < 1")
        self._max_workers = max_workers

        if max_connections < 1:
            raise ValueError(f"invalid max_connections: {max_connections} < 1")
        self._max_connections = max_connections

        if multipart_size < 1024 * 1024:
            raise ValueError(f"invalid multipart_size: {multipart_size} < {1024 * 1024}")
        self._multipart_size = multipart_size

        self._monitor_timer = ContinuousTimer(interval=monitor_interval, function=self.monitor, name="esrally.storage.transfer-monitor")
        self._monitor_timer.start()

    def shutdown(self):
        with self._lock:
            transfers = self._transfers
            self._transfers = []
        for tr in transfers:
            tr.close()
        self._monitor_timer.cancel()

    def get(self, url: str, path: os.PathLike | str | None = None, document_length: int | None = None) -> Transfer:
        """It starts a new transfer of a file from local path to a remote url.

        :param url: remote file address.
        :param path: local file address.
        :param document_length: the expected file size in bytes.
        :return: started transfer object.
        """
        return self._transfer(url=url, path=path, document_length=document_length)

    def _transfer(
        self,
        url: str,
        path: os.PathLike | str | None = None,
        document_length: int | None = None,
    ) -> Transfer:
        if path is None:
            path = os.path.join(self._local_dir, url)
        # This also ensures the path is a string
        path = os.path.normpath(os.path.expanduser(path))

        head = self._client.head(url)
        if document_length is not None and head.content_length != document_length:
            raise ValueError(f"mismatching document_length: got {head.content_length} bytes, wanted {document_length} bytes")
        tr = Transfer(
            client=self._client,
            url=url,
            path=path,
            document_length=head.content_length,
            executor=self._executor,
            multipart_size=self._multipart_size,
            crc32c=head.crc32c,
        )

        # It sets the actual value for `max_connections` after updating the number of unfinished transfers and before
        # requesting for the first worker threads. In this way it will avoid requesting more worker threads than
        # the allowed per-transfer connections.
        with self._lock:
            self._transfers.append(tr)
        self._update_transfers()
        tr.start()
        return tr

    def monitor(self):
        self._update_transfers()
        self._client.monitor()

    @property
    def max_connections(self) -> int:
        with self._lock:
            max_connections = self._max_connections
            number_of_transfers = len(self._transfers)
            if number_of_transfers > 0:
                max_connections = min(max_connections, (self._max_workers // number_of_transfers) + 1)
        return max_connections

    def _update_transfers(self) -> None:
        """It executes periodic update operations on every unfinished transfer."""
        with self._lock:
            # It first removes finished transfers.
            self._transfers = transfers = [tr for tr in self._transfers if not tr.finished]
            if not transfers:
                return

        # It updates max_connections value for each transfer
        max_connections = self.max_connections
        for tr in transfers:
            # It updates the limit of the number of connections for every transfer because it varies in function of
            # the number of transfers in progress.
            tr.max_connections = max_connections
            # It periodically save transfer status to ensure it will be eventually restored from the current state
            # if required.
            tr.save_status()
            # It ensures every unfinished transfer will periodically receive attention from a worker thread as soon
            # it becomes available to prevent it to get stalled forever.
            tr.start()

        # It logs updated statistics for every transfer.
        LOG.info("Transfers in progress:\n  %s", "\n  ".join(tr.info() for tr in transfers))


_LOCK = threading.Lock()
_MANAGER: TransferManager | None = None


def init_transfer_manager(cfg: types.Config, client: Client | None = None, executor: Executor | None = None) -> bool:
    global _MANAGER
    with _LOCK:
        if _MANAGER is not None:
            LOG.debug("Transfer manager already initialized")
            return False
        _MANAGER = TransferManager.from_config(cfg, client=client, executor=executor)
        atexit.register(_MANAGER.shutdown)
        return True


def quit_transfer_manager() -> bool:
    global _MANAGER
    with _LOCK:
        if _MANAGER is None:
            LOG.debug("Transfer manager not initialized.")
            return False
        _MANAGER.shutdown()
        _MANAGER = None
        return True


def transfer_manager() -> TransferManager:
    if _MANAGER is None:
        raise RuntimeError("Transfer manager not initialized.")
    return _MANAGER
