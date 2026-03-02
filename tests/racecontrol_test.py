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

import os
from unittest import mock

import pytest

from esrally import config, exceptions, racecontrol
from esrally.utils import opts


@pytest.fixture
def running_in_docker():
    os.environ["RALLY_RUNNING_IN_DOCKER"] = "true"
    # just yield anything to signal the fixture is ready
    yield True
    del os.environ["RALLY_RUNNING_IN_DOCKER"]


@pytest.fixture
def benchmark_only_pipeline():
    test_pipeline_name = "benchmark-only"
    original = racecontrol.pipelines[test_pipeline_name]
    pipeline = racecontrol.Pipeline(test_pipeline_name, "Pipeline intended for unit-testing", mock.Mock())
    yield pipeline
    # restore prior pipeline!
    racecontrol.pipelines[test_pipeline_name] = original


@pytest.fixture
def unittest_pipeline():
    pipeline = racecontrol.Pipeline("unit-test-pipeline", "Pipeline intended for unit-testing", mock.Mock())
    yield pipeline
    del racecontrol.pipelines[pipeline.name]


def test_finds_available_pipelines():
    expected = [
        ["from-sources", "Builds and provisions Elasticsearch, runs a benchmark and reports results."],
        ["from-distribution", "Downloads an Elasticsearch distribution, provisions it, runs a benchmark and reports results."],
        ["benchmark-only", "Assumes an already running Elasticsearch instance, runs a benchmark and reports results"],
        ["multi-cluster", "Runs the benchmark against each cluster in --target-hosts (one full run per cluster)."],
    ]

    assert expected == racecontrol.available_pipelines()


def test_prevents_running_an_unknown_pipeline():
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "invalid")
    cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "5.0.0")

    with pytest.raises(
        exceptions.SystemSetupError, match=r"Unknown pipeline \[invalid]. List the available pipelines with [\S]+? list pipelines."
    ):
        racecontrol.run(cfg)


def test_passes_benchmark_only_pipeline_in_docker(running_in_docker, benchmark_only_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "benchmark-only")

    racecontrol.run(cfg)

    benchmark_only_pipeline.target.assert_called_once_with(cfg)


def test_fails_without_benchmark_only_pipeline_in_docker(running_in_docker, unittest_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "unit-test-pipeline")

    with pytest.raises(
        exceptions.SystemSetupError,
        match=(
            "Only the \\[benchmark-only\\] and \\[multi-cluster\\] pipelines are supported by the Rally Docker image.\n"
            "Add --pipeline=benchmark-only or --pipeline=multi-cluster in your Rally arguments and try again.\n"
            "For more details read the docs at "
            "https://esrally.readthedocs.io/en/.*/pipelines.html\n"
        ),
    ):
        racecontrol.run(cfg)


def test_runs_a_known_pipeline(unittest_pipeline):
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "unit-test-pipeline")
    cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "")

    racecontrol.run(cfg)

    unittest_pipeline.target.assert_called_once_with(cfg)


def test_multi_cluster_requires_multiple_clusters():
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "system", "race.id", "test-race-id")
    cfg.add(config.Scope.applicationOverride, "race", "pipeline", "multi-cluster")
    # Single cluster (only "default" key)
    cfg.add(
        config.Scope.applicationOverride,
        "client",
        "hosts",
        opts.TargetHosts("127.0.0.1:9200"),
    )
    cfg.add(
        config.Scope.applicationOverride,
        "client",
        "options",
        opts.ClientOptions("timeout:60", target_hosts=cfg.opts("client", "hosts")),
    )

    with pytest.raises(
        exceptions.SystemSetupError,
        match="The multi-cluster pipeline requires multiple named clusters in --target-hosts",
    ):
        racecontrol.multi_cluster(cfg)


@mock.patch("esrally.racecontrol.race")
def test_multi_cluster_runs_single_race_with_all_clusters(mock_race):
    """Multi-cluster runs one race with full multi-cluster config; driver runs each task against each cluster."""
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "system", "race.id", "base-race-id")
    cfg.add(
        config.Scope.applicationOverride,
        "client",
        "hosts",
        opts.TargetHosts('{"cluster-a": ["127.0.0.1:9200"], "cluster-b": ["10.0.0.1:9200"]}'),
    )
    cfg.add(
        config.Scope.applicationOverride,
        "client",
        "options",
        opts.ClientOptions(
            '{"cluster-a": {"timeout": 60}, "cluster-b": {"timeout": 60}}',
            target_hosts=cfg.opts("client", "hosts"),
        ),
    )

    racecontrol.multi_cluster(cfg)

    assert mock_race.call_count == 1
    mock_race.assert_called_once_with(cfg, external=True)
