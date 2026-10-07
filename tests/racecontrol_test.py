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

import asyncio
import contextlib
import os
import signal
from unittest import mock

import pytest

from esrally import actor, config, driver, exceptions, racecontrol
from esrally.track import params, track
from esrally.utils import opts
from tests.conftest import FakeHandle, FakeObjectRef


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


def test_race_reports_rally_error_from_setup_without_starting_actors(fake_ray):
    cfg = _coordinator_cfg("validate-challenge", {"scheduling": [1, 2, 3]})

    with mock.patch(
        "esrally.racecontrol.BenchmarkCoordinator.setup",
        side_effect=exceptions.TrackConfigError("invalid track parameters"),
    ):
        with pytest.raises(exceptions.TrackConfigError) as exc_info:
            racecontrol.race(cfg)

    assert exc_info.value.message == "invalid track parameters"
    assert "Traceback" not in exc_info.value.full_message
    assert fake_ray.created == []


def test_race_converts_ctrl_c_to_user_interrupted(fake_ray):
    async def interrupted_race(self):
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(10)

    with (
        mock.patch("esrally.racecontrol.RaceCoordinator.setup"),
        mock.patch("esrally.racecontrol.RaceCoordinator.run", interrupted_race),
    ):
        with pytest.raises(exceptions.UserInterrupted):
            racecontrol.race(config.Config())


class FakeMechanic:
    def __init__(self, health_failure=None):
        self.node_mechanics = [FakeHandle("node-mechanic")]
        self.health_failure = health_failure
        self.events: list[str] = []

    async def start_engine(self, open_metrics_context, sources=False, distribution=False, external=False, docker=False):
        self.events.append("start_engine")
        return "team-revision"

    async def check_health(self):
        if self.health_failure:
            raise self.health_failure

    async def stop_engine(self):
        self.events.append("stop_engine")


class TestRaceCoordinator:
    @pytest.fixture
    def race_coordinator(self, fake_ray):
        race_coordinator = racecontrol.RaceCoordinator(config.Config(), external=True)
        race_coordinator.coordinator = mock.create_autospec(racecontrol.BenchmarkCoordinator, instance=True)
        race_coordinator.coordinator.race = mock.Mock()
        race_coordinator.coordinator.metrics_store = mock.Mock(open_context={"race-id": "1"})
        race_coordinator.coordinator.current_track = mock.sentinel.track
        race_coordinator.coordinator.cancelled = False
        race_coordinator.coordinator.error = False
        race_coordinator.mechanic = FakeMechanic()
        race_coordinator.progress = mock.Mock()
        return race_coordinator

    @staticmethod
    def driver_behaviors(run_benchmark=b"final-metrics", poll=None):
        return {
            "prepare_benchmark": driver.PreparationComplete("default", "9.2.7", "abc", target_id="cluster", target_platform="on-prem"),
            "run_benchmark": run_benchmark,
            "poll": poll or driver.DriverStatus(progress=[], finished_tasks=[]),
        }

    @pytest.mark.asyncio
    async def test_runs_benchmark_and_reports_results(self, race_coordinator, fake_ray):
        polls = [
            driver.DriverStatus(progress=[("Running a", "[100% done]"), None], finished_tasks=[driver.TaskFinished(b"metrics-a", 1.0)]),
            driver.DriverStatus(progress=[], finished_tasks=[]),
        ]
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(
            poll=lambda: polls.pop(0) if polls else driver.DriverStatus(progress=[], finished_tasks=[])
        )

        await race_coordinator.run()

        (created,) = fake_ray.created_of(driver.DriverActor)
        assert created.args == (race_coordinator.cfg, race_coordinator.mechanic.node_mechanics)
        assert created.kwargs == {"host": "localhost", "name": "driver"}
        coordinator = race_coordinator.coordinator
        assert coordinator.race.team_revision == "team-revision"
        coordinator.on_preparation_complete.assert_called_once_with(
            "default", "9.2.7", "abc", target_id="cluster", target_platform="on-prem", target_auth_type=None
        )
        # results of finished tasks are added before results are reported
        assert coordinator.mock_calls[-2:] == [
            mock.call.on_task_finished(b"metrics-a"),
            mock.call.on_benchmark_complete(b"final-metrics"),
        ]
        race_coordinator.progress.print.assert_called_once_with("Running a", "[100% done]")
        race_coordinator.progress.finish.assert_called_once_with()
        # everything is stopped
        assert created.handle.calls_to("stop") == [((), {})]
        assert created.handle in fake_ray.killed
        assert race_coordinator.mechanic.events == ["start_engine", "stop_engine"]

    @pytest.mark.asyncio
    async def test_reports_benchmark_failure(self, race_coordinator, fake_ray):
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(
            run_benchmark=actor.BenchmarkFailure("Error in load generator [0]", "boom")
        )

        with pytest.raises(exceptions.RallyError) as exc_info:
            await race_coordinator.run()

        assert exc_info.value.message == "Error in load generator [0]"
        assert exc_info.value.cause == "boom"
        assert race_coordinator.coordinator.error
        race_coordinator.coordinator.on_benchmark_complete.assert_not_called()
        assert race_coordinator.mechanic.events == ["start_engine", "stop_engine"]

    @pytest.mark.asyncio
    async def test_reports_failure_of_node_mechanic(self, race_coordinator, fake_ray):
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(run_benchmark=FakeObjectRef(pending=True))
        race_coordinator.mechanic.health_failure = actor.BenchmarkFailure("Error in mechanic", "flush failed")

        with pytest.raises(exceptions.RallyError, match="Error in mechanic"):
            await race_coordinator.run()

    @pytest.mark.asyncio
    async def test_benchmark_cancelled_by_actor(self, race_coordinator, fake_ray):
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(run_benchmark=actor.BenchmarkCancelled())

        await race_coordinator.run()

        assert race_coordinator.coordinator.cancelled
        race_coordinator.coordinator.on_benchmark_complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_reports_dead_actors(self, race_coordinator, fake_ray):
        class ActorDiedError(Exception):
            pass

        ActorDiedError.__module__ = "ray.exceptions"
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(run_benchmark=ActorDiedError("The actor died unexpectedly"))

        with pytest.raises(exceptions.RallyError, match="A Rally actor has died unexpectedly."):
            await race_coordinator.run()

    @pytest.mark.asyncio
    async def test_cancellation_cancels_driver(self, race_coordinator, fake_ray):
        fake_ray.behaviors[driver.DriverActor] = self.driver_behaviors(run_benchmark=FakeObjectRef(pending=True))
        run = asyncio.create_task(race_coordinator.run())
        await asyncio.sleep(0.05)

        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

        (created,) = fake_ray.created_of(driver.DriverActor)
        assert created.handle.calls_to("cancel") == [((), {})]
        assert created.handle.calls_to("stop") == [((), {})]
        assert race_coordinator.coordinator.cancelled
        assert race_coordinator.mechanic.events == ["start_engine", "stop_engine"]


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
def _patched_prepare_track():
    with (
        mock.patch("esrally.racecontrol.track.load_track", return_value=_track_with_challenge("unittest")) as load_track,
        mock.patch("esrally.racecontrol.track.resolve_challenge_and_invoke_validators"),
    ):
        yield load_track


def _assert_preparation_actor_stopped(fake_ray):
    (created,) = fake_ray.created_of(racecontrol.driver.TrackPreparationActor)
    assert created.kwargs == {"host": "localhost", "name": "track-preparator"}
    assert created.handle.calls_to("stop") == [((), {})]
    assert fake_ray.killed == [created.handle]
    return created.handle


def test_prepare_track_succeeds(fake_ray):
    with _patched_prepare_track() as load_track:
        racecontrol.prepare_track(_prepare_track_cfg())

    t = load_track.return_value
    preparator = _assert_preparation_actor_stopped(fake_ray)
    # dependencies have already been installed by the coordinating process
    assert preparator.calls_to("prepare_track") == [((t,), {"install_dependencies": False})]


def test_prepare_track_raises_on_benchmark_failure(fake_ray):
    fake_ray.behaviors[racecontrol.driver.TrackPreparationActor] = {
        "prepare_track": racecontrol.actor.BenchmarkFailure("boom", "root cause")
    }
    with _patched_prepare_track():
        with pytest.raises(exceptions.RallyError) as exc_info:
            racecontrol.prepare_track(_prepare_track_cfg())

    assert exc_info.value.message == "boom"
    # the preparation actor must still be stopped even though result handling raised
    _assert_preparation_actor_stopped(fake_ray)


def test_prepare_track_stops_actor_on_keyboard_interrupt(fake_ray):
    def interrupt_while_preparing(*args, **kwargs):
        # simulate Ctrl-C while the track is being prepared
        asyncio.get_running_loop().call_soon(os.kill, os.getpid(), signal.SIGINT)
        return FakeObjectRef(pending=True)

    fake_ray.behaviors[racecontrol.driver.TrackPreparationActor] = {"prepare_track": interrupt_while_preparing}
    with _patched_prepare_track():
        with pytest.raises(exceptions.UserInterrupted):
            racecontrol.prepare_track(_prepare_track_cfg())

    _assert_preparation_actor_stopped(fake_ray)
