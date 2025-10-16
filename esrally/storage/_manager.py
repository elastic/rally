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
import atexit
import contextvars
import logging
import os
import threading

from typing_extensions import Self

from esrally import types
from esrally.storage._client import Client
from esrally.storage._config import StorageConfig
from esrally.storage._executor import Executor, executor_from_config
from esrally.storage._transfer import Transfer
from esrally.utils.threads import ContinuousTimer

LOG = logging.getLogger(__name__)


class TransferManager:
    """It creates and perform file transfer operations in background."""

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        """It creates a TransferManager with initialization values taken from given configuration."""
        cfg = StorageConfig.from_config(cfg)
        return cls(
            cfg=cfg,
            client=Client.from_config(cfg),
            executor=executor_from_config(cfg),
        )

    def __init__(
        self,
        cfg: StorageConfig,
        client: Client,
        executor: Executor,
    ):
        """It manages files transfers.

        It executes file transfers in background. It periodically logs the status of transfers that are in progress.
        :param cfg: Configuration object.
        :param client: _client.Client instance used to allocate/reuse storage adapters.
        :param executor: Executor instance used to execute transfer operations.
        """
        self.cfg = cfg
        self._client = client
        self._executor = executor
        self._lock = threading.Lock()

        local_dir = os.path.expanduser(cfg.local_dir)
        if not os.path.isdir(local_dir):
            os.makedirs(local_dir)
        self._local_dir = local_dir

        self._transfers: dict[str, Transfer] = {}

        if cfg.max_workers < 1:
            raise ValueError(f"invalid max_workers: {cfg.max_workers} < 1")

        if cfg.max_connections < 1:
            raise ValueError(f"invalid max_connections: {cfg.max_connections} < 1")

        if cfg.multipart_size < 1024 * 1024:
            raise ValueError(f"invalid multipart_size: {cfg.multipart_size} < {1024 * 1024}")

        if cfg.monitor_interval <= 0:
            raise ValueError(f"invalid monitor interval: {cfg.monitor_interval} <= 0")
        self._monitor_timer = ContinuousTimer(interval=cfg.monitor_interval, function=self.monitor, name="esrally.storage.transfer-monitor")
        self._monitor_timer.start()

    def shutdown(self):
        with self._lock:
            transfers = self._transfers
            self._transfers = {}
        LOG.info("Shutting down transfer manager...")
        for tr in transfers.values():
            try:
                if tr.finished:
                    continue
                LOG.warning("Cancelling transfer: %s...", tr)
                tr.close()
            except Exception as ex:
                LOG.error("error closing transfer: %s, %s", tr.url, ex)
        self._monitor_timer.cancel()
        LOG.info("Transfer manager shut down.")

    def get(self, url: str, path: os.PathLike | str | None = None, document_length: int | None = None) -> Transfer:
        """It starts a new transfer of a file from a remote url to a local path.

        :param url: remote file address.
        :param path: local file address.
        :param document_length: the expected file size in bytes.
        :return: started transfer object.
        """
        if path is None:
            path = os.path.join(self.cfg.local_dir, url)
        # This also ensures the path is a string
        path = os.path.normpath(os.path.expanduser(path))

        tr = self._transfers.get(path)
        if tr is not None:
            tr.start()
            return tr

        head = self._client.head(url)
        if document_length is not None and head.content_length != document_length:
            raise ValueError(f"mismatching document_length: got {head.content_length} bytes, wanted {document_length} bytes")
        tr = Transfer(
            client=self._client,
            url=url,
            path=path,
            document_length=head.content_length,
            executor=self._executor,
            multipart_size=self.cfg.multipart_size,
            crc32c=head.crc32c,
        )

        # It sets the actual value for `max_connections` after updating the number of unfinished transfers and before
        # requesting for the first worker threads. In this way it will avoid requesting more worker threads than
        # the allowed per-transfer connections.
        with self._lock:
            self._transfers[tr.path] = tr
        self._update_transfers()
        tr.start()
        return tr

    def monitor(self):
        self._update_transfers()
        self._client.monitor()

    @property
    def max_connections(self) -> int:
        with self._lock:
            max_connections = self.cfg.max_connections
            number_of_transfers = len(self._transfers)
            if number_of_transfers > 0:
                max_connections = min(max_connections, (self.cfg.max_workers // number_of_transfers) + 1)
        return max_connections

    def _update_transfers(self) -> None:
        """It executes periodic update operations on every unfinished transfer."""
        with self._lock:
            # It first removes finished transfers.
            self._transfers = transfers = {tr.path: tr for tr in self._transfers.values() if not tr.finished}
            if not transfers:
                return

        # It updates max_connections value for each transfer
        max_connections = self.max_connections
        for tr in transfers.values():
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
        LOG.info("Transfers in progress:\n  %s", "\n  ".join(tr.info() for tr in transfers.values()))


_MANAGER = contextvars.ContextVar[TransferManager | None](f"{__name__}.transfer_manager", default=None)


def init_transfer_manager(*, cfg: types.Config | None = None, shutdown_at_exit: bool = True) -> TransferManager:
    manager = _MANAGER.get()
    if manager is not None:
        return manager

    # Initialize transfer manager.
    cfg = StorageConfig.from_config(cfg)
    manager = TransferManager.from_config(cfg)
    _MANAGER.set(manager)

    if shutdown_at_exit:
        atexit.register(manager.shutdown)
    return manager


def get_transfer_manager() -> TransferManager:
    manager = _MANAGER.get()
    if manager is None:
        raise RuntimeError("Transfer manager not initialized.")
    return manager


def shutdown_transfer_manager() -> None:
    manager = _MANAGER.get()
    if manager is None:
        return
    _MANAGER.set(None)
    return manager.shutdown()
