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
# pylint: disable=protected-access

import contextlib
import os
from unittest import mock

import pytest

from esrally import config, exceptions, racecontrol
from esrally.track import params, track
from esrally.utils import opts


@pytest.fixture(autouse=True)
def _reset_validators():
    # the validator registry is module-global; ensure no registration leaks across tests
    yield
    params._clear_validators()


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
            "Only the \\[benchmark-only\\] pipeline is supported by the Rally Docker image.\n"
            "Add --pipeline=benchmark-only in your Rally arguments and try again.\n"
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


def _coordinator_cfg(challenge_name, track_params):
    cfg = config.Config()
    # a pinned distribution version skips the cluster version probe so setup() reaches validation without any I/O
    cfg.add(config.Scope.application, "mechanic", "distribution.version", "8.0.0")
    cfg.add(config.Scope.application, "track", "challenge.name", challenge_name)
    cfg.add(config.Scope.application, "track", "params", track_params)
    return cfg


def _track_with_challenge(challenge_name):
    challenge = track.Challenge(challenge_name, default=True, schedule=[])
    return track.Track(name="unittest", challenges=[challenge])


def test_setup_invokes_track_param_validators_for_selected_challenge():
    cfg = _coordinator_cfg("validate-challenge", {"scheduling": [1, 2, 3]})

    received = []

    def validator(track_params):
        received.append(track_params)
        raise exceptions.TrackConfigError("'scheduling' must have 1 or 2 elements but had 3.")

    params.register_validator("validate-challenge", validator)
    with mock.patch("esrally.racecontrol.track.load_track", return_value=_track_with_challenge("validate-challenge")):
        coordinator = racecontrol.BenchmarkCoordinator(cfg)
        with pytest.raises(exceptions.TrackConfigError, match="'scheduling' must have 1 or 2 elements but had 3."):
            coordinator.setup()
    # the validator ran fail-fast (before metrics/engine setup) and received the resolved track params
    assert received == [{"scheduling": [1, 2, 3]}]


def test_benchmark_actor_reports_rally_error_from_setup_without_traceback():
    cfg = _coordinator_cfg("validate-challenge", {"scheduling": [1, 2, 3]})
    sender = mock.Mock()

    with mock.patch("esrally.actor.log.post_configure_actor_logging"):
        benchmark_actor = racecontrol.BenchmarkActor()

    with (
        mock.patch.object(benchmark_actor, "send") as send,
        mock.patch(
            "esrally.racecontrol.BenchmarkCoordinator.setup",
            side_effect=exceptions.TrackConfigError("invalid track parameters"),
        ),
    ):
        benchmark_actor.receiveMsg_Setup(racecontrol.Setup(cfg), sender)

    send.assert_called_once()
    assert send.call_args.args[0] is sender
    failure = send.call_args.args[1]
    assert isinstance(failure, racecontrol.actor.BenchmarkFailure)
    assert failure.message == "invalid track parameters"
    assert "Traceback" not in failure.message


@mock.patch("esrally.racecontrol.metrics.race_store")
@mock.patch("esrally.racecontrol.metrics.metrics_store")
@mock.patch("esrally.racecontrol.metrics.create_race")
def test_setup_continues_when_no_validators_registered(create_race, metrics_store, race_store):
    cfg = _coordinator_cfg("no-validators-challenge", {"scheduling": [1, 2, 3]})

    with mock.patch("esrally.racecontrol.track.load_track", return_value=_track_with_challenge("no-validators-challenge")):
        coordinator = racecontrol.BenchmarkCoordinator(cfg)
        # no validators are registered for this challenge, so setup() must proceed past validation
        coordinator.setup()

    create_race.assert_called_once()


@mock.patch("esrally.racecontrol.metrics.race_store")
@mock.patch("esrally.racecontrol.metrics.metrics_store")
@mock.patch("esrally.racecontrol.metrics.create_race")
def test_setup_runs_all_validators_and_continues_when_they_pass(create_race, metrics_store, race_store):
    cfg = _coordinator_cfg("multi-validator-challenge", {"scheduling": [1]})

    calls = []
    params.register_validator("multi-validator-challenge", lambda p: calls.append("first"))
    params.register_validator("multi-validator-challenge", lambda p: calls.append("second"))
    with mock.patch("esrally.racecontrol.track.load_track", return_value=_track_with_challenge("multi-validator-challenge")):
        coordinator = racecontrol.BenchmarkCoordinator(cfg)
        coordinator.setup()
    # both validators ran (in order) and, because they passed, setup() proceeded past validation
    assert calls == ["first", "second"]
    create_race.assert_called_once()


def test_multi_cluster_flag_rejected_with_single_host():
    """--multi-cluster with a single host in --target-hosts should be caught by CLI validation."""
    # This validation happens in configure_connection_params (rally.py), not racecontrol,
    # so here we just confirm that benchmark-only still works without the flag for a single host.
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "system", "race.id", "28a032d1-0b03-4579-ad2a-c65316f126e9")
    cfg.add(config.Scope.benchmark, "race", "pipeline", "benchmark-only")


@mock.patch("esrally.racecontrol.race")
def test_benchmark_only_with_multi_cluster_flag(mock_race, unittest_pipeline):
    """benchmark-only pipeline with --multi-cluster flag runs a single race covering all clusters."""
    cfg = config.Config()
    cfg.add(config.Scope.applicationOverride, "system", "race.id", "base-race-id")
    cfg.add(config.Scope.applicationOverride, "race", "pipeline", "benchmark-only")
    cfg.add(config.Scope.applicationOverride, "driver", "multi.cluster", True)
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
    cfg.add(config.Scope.benchmark, "mechanic", "distribution.version", "")

    racecontrol.run(cfg)

    assert mock_race.call_count == 1


def _prepare_track_cfg():
    cfg = config.Config()
    cfg.add(config.Scope.benchmark, "track", "track.name", "unittest")
    return cfg


@contextlib.contextmanager
def _patched_prepare_track(ask_result):
    actor_system = mock.Mock()
    prep_actor = mock.sentinel.track_preparation_actor
    actor_system.createActor.return_value = prep_actor
    actor_system.ask.return_value = ask_result
    with (
        mock.patch("esrally.racecontrol.actor.bootstrap_actor_system", return_value=actor_system),
        mock.patch("esrally.racecontrol.track.load_track", return_value=_track_with_challenge("unittest")),
        mock.patch("esrally.racecontrol.track.resolve_challenge_and_invoke_validators"),
    ):
        yield actor_system, prep_actor


def _assert_preparation_actor_stopped(actor_system, prep_actor):
    actor_system.tell.assert_called_once()
    stopped_actor, exit_msg = actor_system.tell.call_args.args
    assert stopped_actor is prep_actor
    assert isinstance(exit_msg, racecontrol.thespian.actors.ActorExitRequest)


def test_prepare_track_succeeds_on_track_prepared():
    with _patched_prepare_track(racecontrol.driver.TrackPrepared()) as (actor_system, prep_actor):
        racecontrol.prepare_track(_prepare_track_cfg())

    actor_system.ask.assert_called_once()
    ask_actor, ask_msg = actor_system.ask.call_args.args
    assert ask_actor is prep_actor
    assert isinstance(ask_msg, racecontrol.driver.PrepareTrackStandalone)
    _assert_preparation_actor_stopped(actor_system, prep_actor)


def test_prepare_track_raises_on_benchmark_failure():
    failure = racecontrol.actor.BenchmarkFailure("boom", "root cause")
    with _patched_prepare_track(failure) as (actor_system, prep_actor):
        with pytest.raises(exceptions.RallyError) as exc_info:
            racecontrol.prepare_track(_prepare_track_cfg())

    assert exc_info.value.message == "boom"
    # the preparation actor must still be stopped even though result handling raised
    _assert_preparation_actor_stopped(actor_system, prep_actor)


def test_prepare_track_raises_on_unexpected_reply():
    with _patched_prepare_track(mock.sentinel.unexpected) as (actor_system, prep_actor):
        with pytest.raises(exceptions.RallyError, match="Got an unexpected result while preparing track"):
            racecontrol.prepare_track(_prepare_track_cfg())

    _assert_preparation_actor_stopped(actor_system, prep_actor)


def test_prepare_track_stops_actor_on_keyboard_interrupt():
    with _patched_prepare_track(None) as (actor_system, prep_actor):
        actor_system.ask.side_effect = KeyboardInterrupt
        with pytest.raises(exceptions.UserInterrupted):
            racecontrol.prepare_track(_prepare_track_cfg())

    _assert_preparation_actor_stopped(actor_system, prep_actor)
