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
from typing import Any, Union

from typing_extensions import Self

from esrally import config, types
from esrally.utils import convert

LOG = logging.getLogger(__name__)

AnyConfig = Union["StorageConfig", types.Config, str, None]


@dataclasses.dataclass
class StorageConfig:
    config_name: str | None = None
    adapters: tuple[str, ...] = (
        "esrally.storage._aws:S3Adapter",
        "esrally.storage._http:HTTPAdapter",
    )
    aws_profile: str = ""
    chunk_size: int = 64 * 1024
    head_ttl: float = 60.0
    local_dir: str = "~/.rally/storage"
    log_actor_name: str = "storage-log-forwarder"
    subprocess_log_level: int = logging.NOTSET
    actor_system_base: str | None = None
    max_connections: int = 4
    max_retries: int | str = 10
    max_workers: int = 32
    mirror_files: tuple[str, ...] = ("~/.rally/storage-mirrors.json",)
    monitor_interval: float = 2.0  # number of seconds
    multipart_size: int = 8 * 1024 * 1024  # number of bytes
    process_startup_method: str | None = None
    random_seed: Any = None
    resolve_ttl: float = 60.0
    thread_name_prefix: str = "esrally.storage.transfer-worker"
    use_threads: bool = False

    @classmethod
    def from_config(cls, cfg: AnyConfig = None) -> Self:
        if isinstance(cfg, cls):
            return cfg

        if cfg is None or isinstance(cfg, str):
            cfg = config.Config(cfg)
            try:
                cfg.load_config(auto_upgrade=True)
            except FileNotFoundError as ex:
                LOG.warning("failed to load storage config from file (name='%s'): %s", cfg.name, ex)

        if not isinstance(cfg, config.Config):
            raise TypeError(f"invalid config type: '{type(cfg)}', want '{types.Config}'")

        return cls(
            config_name=cfg.name,
            actor_system_base=cfg.opts("actor", "actor.system.base", DEFAULT_STORAGE_CONFIG.actor_system_base, False),
            adapters=convert.to_strings(cfg.opts("storage", "storage.adapters", DEFAULT_STORAGE_CONFIG.adapters, False)),
            aws_profile=cfg.opts("storage", "storage.aws.profile", DEFAULT_STORAGE_CONFIG.aws_profile, False).strip(),
            chunk_size=int(cfg.opts("storage", "storage.http.chunk_size", DEFAULT_STORAGE_CONFIG.chunk_size, False)),
            head_ttl=float(cfg.opts("storage", "storage.head_ttl", DEFAULT_STORAGE_CONFIG.head_ttl, False)),
            local_dir=cfg.opts("storage", "storage.local_dir", DEFAULT_STORAGE_CONFIG.local_dir, False),
            log_actor_name=cfg.opts("storage", "storage.log.actor.name", DEFAULT_STORAGE_CONFIG.log_actor_name, False),
            max_connections=int(cfg.opts("storage", "storage.max_connections", DEFAULT_STORAGE_CONFIG.max_connections, False)),
            max_retries=cfg.opts("storage", "storage.http.max_retries", DEFAULT_STORAGE_CONFIG.max_retries, False),
            max_workers=int(cfg.opts("storage", "storage.max_workers", DEFAULT_STORAGE_CONFIG.max_workers, False)),
            mirror_files=convert.to_strings(cfg.opts("storage", "storage.mirror_files", DEFAULT_STORAGE_CONFIG.mirror_files, False)),
            monitor_interval=float(cfg.opts("storage", "storage.monitor_interval", DEFAULT_STORAGE_CONFIG.monitor_interval, False)),
            multipart_size=int(cfg.opts("storage", "storage.multipart_size", DEFAULT_STORAGE_CONFIG.multipart_size, False)),
            process_startup_method=cfg.opts("actor", "actor.process.startup.method", DEFAULT_STORAGE_CONFIG.process_startup_method, False),
            random_seed=cfg.opts("storage", "storage.random_seed", DEFAULT_STORAGE_CONFIG.random_seed, False),
            subprocess_log_level=cfg.opts("storage", "storage.subprocess.log.level", DEFAULT_STORAGE_CONFIG.subprocess_log_level, False),
            resolve_ttl=float(cfg.opts("storage", "storage.resolve_ttl", DEFAULT_STORAGE_CONFIG.head_ttl, False)),
        )


DEFAULT_STORAGE_CONFIG = StorageConfig()
