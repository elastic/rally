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
import re
from datetime import datetime
from unittest import mock

import pytest

from esrally import config, exceptions, racecontrol
from esrally import version as rally_version


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
            f"Only the {re.escape('[benchmark-only]')} pipeline is supported by the Rally Docker image.\n"
            "Add --pipeline=benchmark-only in your Rally arguments and try again.\n"
            "For more details read the docs for the benchmark-only pipeline in "
            "https://esrally.readthedocs.io/en/.*/pipelines.html#benchmark-only\n"
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


def test_on_preparation_complete_stores_race_by_default() -> None:
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "race", "prepare.only", False)
    coord = racecontrol.BenchmarkCoordinator(cfg)
    coord.race = mock.Mock()
    coord.race.challenge = mock.Mock(auto_generated=False)
    coord.race.track_name = "t"
    coord.race.challenge_name = "c"
    coord.race.car = "defaults"
    coord.race.distribution_version = "8.0.0"
    coord.race_store = mock.Mock()

    with mock.patch("esrally.racecontrol.console"):
        coord.on_preparation_complete("oss", "8.0.0", "abc123")

    coord.race_store.store_race.assert_called_once_with(coord.race)


def test_on_preparation_complete_skips_race_store_when_prepare_only() -> None:
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "race", "prepare.only", True)
    coord = racecontrol.BenchmarkCoordinator(cfg)
    coord.race = mock.Mock()
    coord.race.challenge = mock.Mock(auto_generated=False)
    coord.race.track_name = "t"
    coord.race.challenge_name = "c"
    coord.race.car = "defaults"
    coord.race.distribution_version = "8.0.0"
    coord.race_store = mock.Mock()

    with mock.patch("esrally.racecontrol.console"):
        coord.on_preparation_complete("oss", "8.0.0", "abc123")

    coord.race_store.store_race.assert_not_called()


def test_on_prepare_only_complete_closes_metrics_store() -> None:
    cfg = config.Config()
    coord = racecontrol.BenchmarkCoordinator(cfg)
    coord.metrics_store = mock.Mock()

    coord.on_prepare_only_complete()

    coord.metrics_store.close.assert_called_once()


@mock.patch("esrally.racecontrol.client.factory.cluster_distribution_version")
@mock.patch("esrally.racecontrol.track.load_track")
@mock.patch("esrally.racecontrol.metrics.create_race")
@mock.patch("esrally.racecontrol.metrics.metrics_store")
@mock.patch("esrally.racecontrol.metrics.race_store")
def test_setup_prepare_only_skips_cluster_distribution_probe(
    mock_race_store: mock.Mock,
    mock_metrics_store: mock.Mock,
    mock_create_race: mock.Mock,
    mock_load_track: mock.Mock,
    mock_cluster_version: mock.Mock,
) -> None:
    mock_cluster_version.side_effect = AssertionError("cluster probe should not run")
    mock_challenge = mock.Mock()
    mock_challenge.user_info = None
    mock_challenge.serverless_info = []
    mock_challenge.__str__ = mock.Mock(return_value="default")
    mock_track = mock.Mock()
    mock_track.find_challenge_or_default.return_value = mock_challenge
    mock_track.__str__ = mock.Mock(return_value="unittest")
    mock_load_track.return_value = mock_track
    mock_race = mock.Mock()
    mock_race.track = mock_track
    mock_race.challenge = mock_challenge
    mock_create_race.return_value = mock_race
    mock_metrics_store.return_value = mock.Mock()
    mock_race_store.return_value = mock.Mock()

    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "race", "prepare.only", True)
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "system", "time.start", datetime(2017, 8, 20, 1, 0, 0))
    cfg.add(config.Scope.benchmark, "system", "env.name", "unittest")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "benchmark-only")
    cfg.add(config.Scope.benchmark, "race", "user.tags", {})
    cfg.add(config.Scope.benchmark, "track", "challenge.name", "default")
    cfg.add(config.Scope.benchmark, "track", "params", {})
    cfg.add(config.Scope.benchmark, "mechanic", "car.names", ["defaults"])
    cfg.add(config.Scope.benchmark, "mechanic", "car.params", {})
    cfg.add(config.Scope.benchmark, "mechanic", "plugin.params", {})
    cfg.add(config.Scope.benchmark, "reporting", "datastore.type", "in-memory")

    coord = racecontrol.BenchmarkCoordinator(cfg)
    coord.setup(sources=False)

    mock_cluster_version.assert_not_called()
    assert cfg.opts("mechanic", "distribution.version") == rally_version.minimum_es_version()
    assert cfg.opts("mechanic", "distribution.flavor") == "default"
    assert cfg.opts("reporting", "datastore.type") == "in-memory"
