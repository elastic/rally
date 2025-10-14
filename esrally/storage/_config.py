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
import logging
from collections.abc import Iterable
from typing import Any

from esrally import config
from esrally.utils import convert

LOG = logging.getLogger(__name__)


class StorageConfig(config.Config):

    DEFAULT_ADAPTERS = (
        "esrally.storage._aws:S3Adapter",
        "esrally.storage._http:HTTPAdapter",
    )

    @property
    def adapters(self) -> tuple[str, ...]:
        return convert.to_strings(self.opts("storage", "storage.adapters", self.DEFAULT_ADAPTERS, False))

    @adapters.setter
    def adapters(self, value: Iterable[str] | None) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.adapters", convert.to_strings(value))

    DEFAULT_AWS_PROFILE = None

    @property
    def aws_profile(self) -> str | None:
        return self.opts("storage", "storage.aws.profile", self.DEFAULT_AWS_PROFILE, False)

    @aws_profile.setter
    def aws_profile(self, value: str | None) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.aws.profile", value)

    DEFAULT_CHUNK_SIZE = 64 * 1024

    @property
    def chunk_size(self) -> int:
        return int(self.opts("storage", "storage.chunk_size", self.DEFAULT_CHUNK_SIZE, False))

    @chunk_size.setter
    def chunk_size(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.chunk_size", value)

    DEFAULT_LOCAL_DIR = "~/.rally/storage"

    @property
    def local_dir(self) -> str:
        return self.opts("storage", "storage.local_dir", self.DEFAULT_LOCAL_DIR, False)

    @local_dir.setter
    def local_dir(self, value: str) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.local_dir", value)

    DEFAULT_MAX_CONNECTIONS = 4

    @property
    def max_connections(self) -> int:
        return int(self.opts("storage", "storage.max_connections", self.DEFAULT_MAX_CONNECTIONS, False))

    @max_connections.setter
    def max_connections(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.max_connections", value)

    DEFAULT_MAX_RETRIES = 3

    @property
    def max_retries(self) -> int | str:
        return self.opts("storage", "storage.http.max_retries", self.DEFAULT_MAX_RETRIES, False)

    @max_retries.setter
    def max_retries(self, value: int | str) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.http.max_retries", value)

    DEFAULT_MAX_WORKERS = 64

    @property
    def max_workers(self) -> int:
        return int(self.opts("storage", "storage.max_workers", self.DEFAULT_MAX_WORKERS, False))

    @max_workers.setter
    def max_workers(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.max_workers", value)

    DEFAULT_MIRROR_FILES = ("~/.rally/storage-mirrors.json",)

    @property
    def mirror_files(self) -> tuple[str, ...]:
        return convert.to_strings(self.opts("storage", "storage.mirror_files", self.DEFAULT_MIRROR_FILES, False))

    @mirror_files.setter
    def mirror_files(self, value: Iterable[str] | None) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.mirror_files", convert.to_strings(value))

    DEFAULT_MONITOR_INTERVAL = 4.0

    @property
    def monitor_interval(self) -> float:
        return self.opts("storage", "storage.monitor_interval", self.DEFAULT_MONITOR_INTERVAL, False)

    @monitor_interval.setter
    def monitor_interval(self, value: float) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.monitor_interval", value)

    DEFAULT_MULTIPART_SIZE = 8 * 1024 * 1024

    @property
    def multipart_size(self) -> int:
        return int(self.opts("storage", "storage.multipart_size", self.DEFAULT_MULTIPART_SIZE, False))

    @multipart_size.setter
    def multipart_size(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.multipart_size", value)

    DEFAULT_RANDOM_SEED = None

    @property
    def random_seed(self) -> Any:
        return self.opts("storage", "storage.random_seed", self.DEFAULT_RANDOM_SEED, False)

    @random_seed.setter
    def random_seed(self, value: Any) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.random_seed", value)

    DEFAULT_CACHE_TTL = 60.0

    @property
    def cache_ttl(self) -> float:
        return self.opts("storage", "storage.cache_ttl", self.DEFAULT_CACHE_TTL, False)

    @cache_ttl.setter
    def cache_ttl(self, value: float) -> None:
        self.add(config.Scope.applicationOverride, "storage", "storage.cache_ttl", value)
