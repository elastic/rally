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
import asyncio
import dataclasses
import os
import signal
import sys
import threading
from typing import Any
from unittest import mock

import pytest

from esrally import actor, exceptions
from esrally.utils import cases, net
from tests.conftest import FakeHandle, FakeObjectRef


class RayTaskErrorLike(Exception):
    """
    Mimics how Ray wraps an exception raised in an actor method: in a class named ``RayTaskError(<cause class>)``.
    """

    def __init__(self, cause: BaseException):
        super().__init__(str(cause))
        self.cause = cause


RayTaskErrorLike.__name__ = "RayTaskError(BenchmarkFailure)"


class TestFailures:
    def test_benchmark_failure_is_a_rally_error(self):
        failure = actor.BenchmarkFailure("message", "cause")
        assert isinstance(failure, exceptions.RallyError)
        assert failure.full_message == "message\n\tcause"

    def test_benchmark_failure_from_current_exception_contains_traceback(self):
        failure = None
        try:
            raise ValueError("boom")
        except ValueError:
            failure = actor.BenchmarkFailure.from_current_exception("Error in test")
        assert failure is not None
        assert failure.message == "Error in test"
        assert "Traceback" in failure.cause
        assert "ValueError: boom" in failure.cause

    def test_unwrap_returns_cause_of_ray_task_errors(self):
        cause = actor.BenchmarkFailure("inner")
        assert actor.unwrap(RayTaskErrorLike(cause)) is cause

    def test_unwrap_returns_other_exceptions_as_is(self):
        e = ValueError("plain")
        assert actor.unwrap(e) is e


class TestConvertFailures:
    @actor.convert_failures("test actor")
    def sync_method(self, e: BaseException | None):
        if e is not None:
            raise e
        return "result"

    @actor.convert_failures("test actor")
    async def async_method(self, e: BaseException | None):
        if e is not None:
            raise e
        return "result"

    def test_returns_result(self):
        assert self.sync_method(None) == "result"

    def test_keeps_benchmark_failures(self):
        failure = actor.BenchmarkFailure("original")
        with pytest.raises(actor.BenchmarkFailure) as exc_info:
            self.sync_method(failure)
        assert exc_info.value is failure

    def test_keeps_benchmark_cancelled(self):
        with pytest.raises(actor.BenchmarkCancelled):
            self.sync_method(actor.BenchmarkCancelled())

    def test_unwraps_failures_raised_in_other_actors(self):
        failure = actor.BenchmarkFailure("in another actor")
        with pytest.raises(actor.BenchmarkFailure) as exc_info:
            self.sync_method(RayTaskErrorLike(failure))
        assert exc_info.value is failure

    def test_converts_other_exceptions(self):
        with pytest.raises(actor.BenchmarkFailure) as exc_info:
            self.sync_method(KeyError("missing"))
        assert exc_info.value.message == "Error in test actor"
        assert "KeyError: 'missing'" in exc_info.value.cause

    def test_includes_message_of_rally_errors(self):
        with pytest.raises(actor.BenchmarkFailure) as exc_info:
            self.sync_method(exceptions.SystemSetupError("Elasticsearch REST API layer is not available."))
        assert exc_info.value.message == "Error in test actor: Elasticsearch REST API layer is not available."

    @pytest.mark.asyncio
    async def test_async_returns_result(self):
        assert await self.async_method(None) == "result"

    @pytest.mark.asyncio
    async def test_async_converts_other_exceptions(self):
        with pytest.raises(actor.BenchmarkFailure, match="Error in test actor"):
            await self.async_method(RuntimeError("boom"))

    @pytest.mark.asyncio
    async def test_async_keeps_cancellation(self):
        with pytest.raises(asyncio.CancelledError):
            await self.async_method(asyncio.CancelledError())


class Failing:
    def __init__(self):
        self.failures: list[BaseException] = []

    def _fail(self, failure: BaseException) -> None:
        self.failures.append(failure)

    @actor.report_failures("failing actor")
    def sync_method(self, e: BaseException | None):
        if e is not None:
            raise e
        return "result"

    @actor.report_failures("failing actor")
    async def async_method(self, e: BaseException | None):
        if e is not None:
            raise e
        return "result"


class TestReportFailures:
    def test_returns_result(self):
        target = Failing()
        assert target.sync_method(None) == "result"
        assert target.failures == []

    def test_reports_converted_failure(self):
        target = Failing()
        assert target.sync_method(OSError("disk full")) is None
        assert len(target.failures) == 1
        assert isinstance(target.failures[0], actor.BenchmarkFailure)
        assert target.failures[0].message == "Error in failing actor"

    def test_reports_cancellation_as_is(self):
        target = Failing()
        cancelled = actor.BenchmarkCancelled()
        target.sync_method(cancelled)
        assert target.failures == [cancelled]

    @pytest.mark.asyncio
    async def test_async_reports_converted_failure(self):
        target = Failing()
        assert await target.async_method(ValueError("bad")) is None
        assert isinstance(target.failures[0], actor.BenchmarkFailure)

    @pytest.mark.asyncio
    async def test_async_propagates_task_cancellation(self):
        target = Failing()
        with pytest.raises(asyncio.CancelledError):
            await target.async_method(asyncio.CancelledError())
        assert target.failures == []


@dataclasses.dataclass
class NodeResourceCase:
    host: str
    want: str


@cases.cases(
    localhost=NodeResourceCase(host="localhost", want=actor.HEAD_NODE_RESOURCE),
    loopback=NodeResourceCase(host="127.0.0.1", want=actor.HEAD_NODE_RESOURCE),
    other_loopback=NodeResourceCase(host="127.0.1.1", want=actor.HEAD_NODE_RESOURCE),
    ip=NodeResourceCase(host="10.5.5.6", want="node:10.5.5.6"),
    hostname=NodeResourceCase(host="loaddriver1", want="node:10.0.0.42"),
)
def test_node_resource(case: NodeResourceCase, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(net, "resolve", lambda host: "10.0.0.42" if host == "loaddriver1" else host)
    assert actor.node_resource(case.host) == case.want


class FakeActorClass:
    def __init__(self):
        self.options_calls: list[dict[str, Any]] = []
        self.remote_calls: list[tuple[tuple, dict]] = []

    def options(self, **options: Any) -> "FakeActorClass":
        self.options_calls.append(options)
        return self

    def remote(self, *args: Any, **kwargs: Any) -> str:
        self.remote_calls.append((args, kwargs))
        return "handle"


class TestCreateActor:
    @pytest.fixture
    def actor_class(self, monkeypatch: pytest.MonkeyPatch) -> FakeActorClass:
        actor_class = FakeActorClass()
        monkeypatch.setattr(actor, "remote_class", lambda cls: actor_class)
        return actor_class

    def test_places_actor_on_host(self, actor_class: FakeActorClass):
        assert actor.create_actor(object, "arg", host="10.5.5.6", name="worker-1", key="value") == "handle"
        assert actor_class.options_calls == [{"name": "worker-1", "resources": {"node:10.5.5.6": actor.NODE_RESOURCE_UNITS}}]
        assert actor_class.remote_calls == [(("arg",), {"key": "value"})]

    def test_places_actor_on_coordinator(self, actor_class: FakeActorClass):
        actor.create_actor(object, host="localhost")
        assert actor_class.options_calls == [{"resources": {actor.HEAD_NODE_RESOURCE: actor.NODE_RESOURCE_UNITS}}]

    def test_uses_scheduling_strategy(self, actor_class: FakeActorClass):
        actor.create_actor(object, host="10.5.5.6", strategy="same-node")
        assert actor_class.options_calls == [{"scheduling_strategy": "same-node"}]


class TestRemoteClass:
    def test_rally_actors_reserve_no_cpu_and_are_not_restarted(self, monkeypatch: pytest.MonkeyPatch):
        import ray  # pylint: disable=import-outside-toplevel

        decorator_args = []

        def fake_remote(**kwargs):
            decorator_args.append(kwargs)
            return lambda cls: ("remote", cls)

        monkeypatch.setattr(ray, "remote", fake_remote)
        monkeypatch.setattr(actor, "_REMOTE_CLASSES", {})

        class SomeActor:
            pass

        assert actor.remote_class(SomeActor) == ("remote", SomeActor)
        # memoized
        assert actor.remote_class(SomeActor) == ("remote", SomeActor)
        assert decorator_args == [{"num_cpus": 0, "max_restarts": 0}]


@dataclasses.dataclass
class ClusterRunningCase:
    address: str | None
    reachable: set[tuple[str, int]]
    want: bool


@cases.cases(
    no_daemon=ClusterRunningCase(address=None, reachable=set(), want=False),
    local_daemon_without_address_file=ClusterRunningCase(address=None, reachable={("127.0.0.1", 1900)}, want=True),
    daemon=ClusterRunningCase(address="10.5.5.5:1900", reachable={("10.5.5.5", 1900)}, want=True),
    stale_address_file=ClusterRunningCase(address="10.5.5.5:1900", reachable=set(), want=False),
    local_instance_on_random_port=ClusterRunningCase(address="127.0.0.1:61234", reachable={("127.0.0.1", 61234)}, want=False),
)
def test_is_cluster_running(case: ClusterRunningCase, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(actor, "daemon_address", lambda: case.address)
    monkeypatch.setattr(actor, "_can_connect", lambda host, port: (host, port) in case.reachable)
    assert actor.is_cluster_running() == case.want


def test_daemon_address_reads_ray_address_file(tmp_path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("RAY_TMPDIR", str(tmp_path))
    assert actor.daemon_address() is None
    (tmp_path / "ray").mkdir()
    (tmp_path / "ray" / "ray_current_cluster").write_text("10.5.5.5:1900\n")
    assert actor.daemon_address() == "10.5.5.5:1900"


class FakeProcess:
    def __init__(self, name: str, cmdline: list[str]):
        self._name = name
        self._cmdline = cmdline

    def name(self) -> str:
        return self._name

    def cmdline(self) -> list[str]:
        return self._cmdline


@dataclasses.dataclass
class DaemonRunningCase:
    processes: list[FakeProcess]
    want: bool


@cases.cases(
    nothing=DaemonRunningCase(processes=[], want=False),
    daemon=DaemonRunningCase(processes=[FakeProcess("raylet", ["raylet", "--gcs-address=10.5.5.5:1900"])], want=True),
    local_instance=DaemonRunningCase(processes=[FakeProcess("raylet", ["raylet", "--gcs-address=127.0.0.1:61234"])], want=False),
    other_process=DaemonRunningCase(processes=[FakeProcess("python3", ["python3", "--gcs-address=10.5.5.5:1900"])], want=False),
)
def test_is_daemon_running_locally(case: DaemonRunningCase, monkeypatch: pytest.MonkeyPatch):
    import psutil  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(psutil, "process_iter", lambda: case.processes)
    assert actor.is_daemon_running_locally() == case.want


class TestInitRay:
    @pytest.fixture
    def ray_init(self, monkeypatch: pytest.MonkeyPatch) -> mock.Mock:
        import ray  # pylint: disable=import-outside-toplevel

        init = mock.Mock()
        monkeypatch.setattr(ray, "init", init)
        for key in actor.RAY_ENVIRONMENT_DEFAULTS:
            monkeypatch.delenv(key, raising=False)
        return init

    def test_connects_to_daemon(self, ray_init: mock.Mock, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "is_cluster_running", lambda: True)
        monkeypatch.setattr(actor, "daemon_address", lambda: "10.5.5.5:1900")

        assert actor.init_ray(namespace="rally-1", num_cpus=8) is True

        ray_init.assert_called_once_with(
            address="10.5.5.5:1900", include_dashboard=False, configure_logging=False, log_to_driver=True, namespace="rally-1"
        )

    def test_starts_local_instance(self, ray_init: mock.Mock, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "is_cluster_running", lambda: False)

        assert actor.init_ray(namespace="rally-1", num_cpus=8) is False

        ray_init.assert_called_once_with(
            address="local",
            _node_ip_address="127.0.0.1",
            num_cpus=8,
            object_store_memory=actor.DEFAULT_OBJECT_STORE_MEMORY,
            include_dashboard=False,
            configure_logging=False,
            log_to_driver=True,
            namespace="rally-1",
        )

    def test_disables_ray_uv_run_support_even_if_ray_was_imported_before(self, ray_init: mock.Mock, monkeypatch: pytest.MonkeyPatch):
        # pylint: disable-next=import-outside-toplevel
        from ray._private import ray_constants

        monkeypatch.setattr(actor, "is_cluster_running", lambda: False)
        monkeypatch.setattr(ray_constants, "RAY_ENABLE_UV_RUN_RUNTIME_ENV", True)

        actor.init_ray(namespace="rally-1")

        assert os.environ["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] == "0"
        assert ray_constants.RAY_ENABLE_UV_RUN_RUNTIME_ENV is False

    def test_sets_environment_defaults(self, ray_init: mock.Mock, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "is_cluster_running", lambda: False)
        monkeypatch.setenv("RAY_DEDUP_LOGS", "1")

        actor.init_ray(namespace="rally-1")

        assert os.environ["RAY_USAGE_STATS_ENABLED"] == "0"
        # users can override all defaults
        assert os.environ["RAY_DEDUP_LOGS"] == "1"


class TestConfigureRayEnvironment:
    @pytest.fixture(autouse=True)
    def clean_environment(self, monkeypatch: pytest.MonkeyPatch):
        for key in actor.RAY_ENVIRONMENT_DEFAULTS:
            monkeypatch.delenv(key, raising=False)
        # restore whatever configure_ray_environment() sets
        monkeypatch.setattr(os, "environ", dict(os.environ))

    def test_sets_all_defaults_before_ray_is_imported(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delitem(sys.modules, "ray", raising=False)
        actor.configure_ray_environment()
        assert {k: os.environ[k] for k in actor.RAY_ENVIRONMENT_DEFAULTS} == actor.RAY_ENVIRONMENT_DEFAULTS

    def test_keeps_authentication_mode_once_ray_is_imported(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setitem(sys.modules, "ray", mock.Mock())
        actor.configure_ray_environment()
        assert "RAY_AUTH_MODE" not in os.environ
        assert os.environ["RAY_USAGE_STATS_ENABLED"] == "0"


class TestStopActor:
    @pytest.fixture
    def killed(self, monkeypatch: pytest.MonkeyPatch) -> list:
        killed: list = []
        monkeypatch.setattr(actor, "kill_actor", killed.append)
        return killed

    @pytest.mark.asyncio
    async def test_stops_and_kills_actor(self, killed: list):
        handle = FakeHandle()
        await actor.stop_actor(handle, timeout=1)
        assert [name for name, _, _ in handle.calls] == ["stop"]
        assert killed == [handle]

    @pytest.mark.asyncio
    async def test_kills_actor_that_does_not_stop_in_time(self, killed: list):
        handle = FakeHandle(behaviors={"stop": FakeObjectRef(pending=True)})
        await actor.stop_actor(handle, timeout=0.01)
        assert killed == [handle]

    @pytest.mark.asyncio
    async def test_kills_actor_that_fails_to_stop(self, killed: list):
        handle = FakeHandle(behaviors={"stop": RuntimeError("actor died")})
        await actor.stop_actor(handle, timeout=1)
        assert killed == [handle]


class TestRequireNode:
    def test_does_not_wait_for_coordinator(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "_node_is_alive", mock.Mock(side_effect=AssertionError("must not be called")))
        actor.require_node("localhost", timeout=0)

    def test_returns_when_node_is_alive(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "_node_is_alive", lambda ip: ip == "10.5.5.6")
        actor.require_node("10.5.5.6", timeout=0)

    def test_fails_when_node_does_not_join(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "_node_is_alive", lambda ip: False)
        with pytest.raises(exceptions.LaunchError, match=r"No Rally daemon is running on \[10.5.5.6\]"):
            actor.require_node("10.5.5.6", timeout=0)

    @pytest.mark.asyncio
    async def test_async_fails_when_node_does_not_join(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(actor, "_node_is_alive", lambda ip: False)
        with pytest.raises(exceptions.LaunchError, match="Are Rally daemons on all targeted machines running?"):
            await actor.require_node_async("10.5.5.6", timeout=0)


class TestRunAsync:
    def test_returns_result(self):
        async def main():
            return 42

        assert actor.run_async(main) == 42

    def test_ctrl_c_cancels_coroutine_which_can_clean_up(self):
        cleaned_up = threading.Event()

        async def main():
            # simulate Ctrl-C while waiting
            asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGINT)
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cleaned_up.set()
                raise

        with pytest.raises(KeyboardInterrupt):
            actor.run_async(main)
        assert cleaned_up.is_set()
        # the signal handler has been removed again
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler

    def test_coroutine_may_handle_ctrl_c_itself(self):
        async def main():
            asyncio.get_running_loop().call_later(0.05, os.kill, os.getpid(), signal.SIGINT)
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise exceptions.UserInterrupted("cancelled") from None

        with pytest.raises(exceptions.UserInterrupted):
            actor.run_async(main)
