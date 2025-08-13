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
from typing import Any, Union

from typing_extensions import Self

from esrally import config, types
from esrally.utils import convert

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
    local_dir: str = "~/.rally/storage"
    max_connections: int = 4
    max_retries: int | str = 10
    max_workers: int = 32
    mirror_files: tuple[str, ...] = ("~/.rally/storage-mirrors.json",)
    monitor_interval: float = 2.0  # number of seconds
    multipart_size: int = 8 * 1024 * 1024  # number of bytes
    random_seed: Any = None
    thread_name_prefix: str = "esrally.storage.transfer-worker"

    @classmethod
    def from_config(cls, cfg: AnyConfig = None) -> Self:
        if isinstance(cfg, cls):
            return cfg

        if cfg is None or isinstance(cfg, str):
            cfg = config.Config(cfg)
            cfg.load_config(auto_upgrade=True)

        if not isinstance(cfg, config.Config):
            raise TypeError(f"invalid config type: '{type(cfg)}', want '{types.Config}'")

        return cls(
            config_name=cfg.name,
            adapters=convert.to_strings(
                cfg.opts(section="storage", key="storage.adapters", default_value=DEFAULT_STORAGE_CONFIG.adapters, mandatory=False)
            ),
            aws_profile=cfg.opts(
                "storage", "storage.aws.profile", default_value=DEFAULT_STORAGE_CONFIG.aws_profile, mandatory=False
            ).strip(),
            chunk_size=int(cfg.opts("storage", "storage.http.chunk_size", DEFAULT_STORAGE_CONFIG.chunk_size, False)),
            local_dir=cfg.opts(section="storage", key="storage.local_dir", default_value=DEFAULT_STORAGE_CONFIG.local_dir, mandatory=False),
            max_connections=int(
                cfg.opts(
                    section="storage", key="storage.max_connections", default_value=DEFAULT_STORAGE_CONFIG.max_connections, mandatory=False
                )
            ),
            max_retries=cfg.opts("storage", "storage.http.max_retries", DEFAULT_STORAGE_CONFIG.max_retries, mandatory=False),
            max_workers=int(
                cfg.opts(section="storage", key="storage.max_workers", default_value=DEFAULT_STORAGE_CONFIG.max_workers, mandatory=False)
            ),
            mirror_files=convert.to_strings(
                cfg.opts(section="storage", key="storage.mirror_files", default_value=DEFAULT_STORAGE_CONFIG.mirror_files, mandatory=False)
            ),
            monitor_interval=float(
                cfg.opts(
                    section="storage",
                    key="storage.monitor_interval",
                    default_value=DEFAULT_STORAGE_CONFIG.monitor_interval,
                    mandatory=False,
                )
            ),
            multipart_size=int(
                cfg.opts(
                    section="storage", key="storage.multipart_size", default_value=DEFAULT_STORAGE_CONFIG.multipart_size, mandatory=False
                )
            ),
            random_seed=cfg.opts(
                section="storage", key="storage.random_seed", default_value=DEFAULT_STORAGE_CONFIG.random_seed, mandatory=False
            ),
        )


DEFAULT_STORAGE_CONFIG = StorageConfig()
