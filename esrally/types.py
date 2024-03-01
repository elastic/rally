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

from typing import Any, Literal, Protocol, TypeVar

Section = Literal[
    "benchmarks",
    "client",
    "defaults",
    "distributions",
    "driver",
    "generator",
    "mechanic",
    "meta",
    "no_copy",
    "node",
    "provisioning",
    "race",
    "reporting",
    "source",
    "system",
    "teams",
    "telemetry",
    "tests",
    "track",
    "tracks",
    "unit-test",
]
Key = Literal[
    "add.chart_name",
    "add.chart_type",
    "add.config.option",
    "add.message",
    "add.race_timestamp",
    "admin.dry_run",
    "admin.track",
    "assertions",
    "async.debug",
    "available.cores",
    "batch_size",
    "build.type",
    "cache",
    "cache.days",
    "car.names",
    "car.params",
    "car.plugins",
    "challenge.name",
    "challenge.root.dir",
    "cluster.name",
    "config.version",
    "data_streams",
    "datastore.host",
    "datastore.number_of_replicas",
    "datastore.number_of_shards",
    "datastore.password",
    "datastore.port",
    "datastore.probe.cluster_version",
    "datastore.secure",
    "datastore.ssl.certificate_authorities",
    "datastore.ssl.verification_mode",
    "datastore.type",
    "datastore.user",
    "delete.config.option",
    "delete.id",
    "devices",
    "distribution.dir",
    "distribution.flavor",
    "distribution.repository",
    "distribution.version",
    "elasticsearch.src.subdir",
    "env.name",
    "exclude.tasks",
    "format",
    "hosts",
    "include.tasks",
    "indices",
    "install.id",
    "list.config.option",
    "list.from_date",
    "list.max_results",
    "list.races.benchmark_name",
    "list.to_date",
    "load_driver_hosts",
    "local.dataset.cache",
    "master.nodes",
    "metrics.log.dir",
    "metrics.request.downsample.factor",
    "metrics.url",
    "network.host",
    "network.http.port",
    "node.http.port",
    "node.ids",
    "node.name",
    "node.name.prefix",
    "numbers.align",
    "offline.mode",
    "on.error",
    "options",
    "other.key",
    "output.path",
    "output.processingtime",
    "params",
    "passenv",
    "pipeline",
    "plugin.community-plugin.src.dir",
    "plugin.community-plugin.src.subdir",
    "plugin.params",
    "preserve.install",
    "preserve_benchmark_candidate",
    "private.url",
    "profiling",
    "quiet.mode",
    "race.id",
    "rally.cwd",
    "rally.root",
    "release.cache",
    "release.url",
    "remote.benchmarking.supported",
    "remote.repo.url",
    "repository.name",
    "repository.revision",
    "root.dir",
    "runtime.jdk",
    "sample.key",
    "sample.property",
    "sample.queue.size",
    "seed.hosts",
    "serverless.mode",
    "serverless.operator",
    "skip.rest.api.check",
    "snapshot.cache",
    "source.build.method",
    "source.revision",
    "src.root.dir",
    "target.arch",
    "target.os",
    "team.path",
    "team.repository.dir",
    "test.mode.enabled",
    "time.start",
    "track.name",
    "track.path",
    "track.repository.dir",
    "user.tags",
    "values",
]
_Config = TypeVar("_Config", bound="Config")


class Config(Protocol):
    def add(self, scope, section: Section, key: Key, value: Any) -> None:
        ...

    def add_all(self, source: _Config, section: Section) -> None:
        ...

    def opts(self, section: Section, key: Key, default_value=None, mandatory: bool = True) -> Any:
        ...

    def all_opts(self, section: Section) -> dict:
        ...

    def exists(self, section: Section, key: Key) -> bool:
        ...
