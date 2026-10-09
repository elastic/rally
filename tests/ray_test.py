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
"""
Tests of Rally's actor layer with a real (local) Ray instance. Set ``RALLY_SKIP_RAY_TESTS=1`` to skip them.
"""

import logging
import os
import time

import psutil
import pytest

from esrally import actor
from esrally.utils import process

pytestmark = [pytest.mark.ray, pytest.mark.slow]


@pytest.fixture(scope="module")
def ray_instance(tmp_path_factory):
    if os.environ.get("RALLY_SKIP_RAY_TESTS"):
        pytest.skip("RALLY_SKIP_RAY_TESTS is set")
    environment = dict(os.environ)
    # actor processes inherit the environment of the Ray instance: let them log to a temporary directory
    os.environ["RALLY_HOME"] = str(tmp_path_factory.mktemp("rally-home"))
    actor.configure_ray_environment()
    import ray  # pylint: disable=import-outside-toplevel

    ray.init(
        address="local",
        _node_ip_address="127.0.0.1",
        num_cpus=2,
        object_store_memory=80 * 1024 * 1024,
        include_dashboard=False,
        configure_logging=False,
        log_to_driver=False,
        namespace="rally-tests",
    )
    try:
        yield ray
    finally:
        ray.shutdown()
        os.environ.clear()
        os.environ.update(environment)


class EchoActor(actor.RallyActorBase):
    def __init__(self, other=None):
        super().__init__(name="echo")
        self.other = other

    @actor.convert_failures("echo")
    async def echo(self, value):
        logging.getLogger(__name__).info("Echoing [%s]", value)
        return value, os.getpid()

    def stdin(self):
        stat = os.fstat(0)
        return stat.st_dev, stat.st_ino

    @actor.convert_failures("echo")
    def fail(self):
        raise ValueError("boom")

    @actor.convert_failures("echo")
    async def call_other(self):
        return await self.other.fail.remote()

    async def stop(self):
        pass


class Worker(actor.RallyActorBase):
    """
    Has the same name as Rally's load generator actor so that its process is considered a Rally process.
    """

    def pid(self):
        return os.getpid()


def test_round_trip_and_failures(ray_instance):
    echo = actor.create_actor(EchoActor, host="localhost")

    value, pid = ray_instance.get(echo.echo.remote("hello"))
    assert value == "hello"
    assert pid != os.getpid()

    with pytest.raises(actor.BenchmarkFailure) as exc_info:
        ray_instance.get(echo.fail.remote())
    failure = actor.unwrap(exc_info.value)
    assert failure.message == "Error in echo"
    assert "ValueError: boom" in failure.cause
    actor.kill_actor(echo)


def test_failures_of_nested_actors_are_not_wrapped_twice(ray_instance):
    inner = actor.create_actor(EchoActor, host="localhost")
    outer = actor.create_actor(EchoActor, inner, host="localhost")

    with pytest.raises(actor.BenchmarkFailure) as exc_info:
        ray_instance.get(outer.call_other.remote())

    assert actor.unwrap(exc_info.value).message == "Error in echo"
    assert "ValueError: boom" in actor.unwrap(exc_info.value).cause
    actor.kill_actor(outer)
    actor.kill_actor(inner)


def test_stopped_actors_are_dead(ray_instance):
    echo = actor.create_actor(EchoActor, host="localhost")
    ray_instance.get(echo.echo.remote("ping"))

    actor.run_async(lambda: actor.stop_actor(echo, timeout=10))

    with pytest.raises(ray_instance.exceptions.RayActorError):
        ray_instance.get(echo.echo.remote("ping"), timeout=30)


def test_actor_processes_are_recognized_as_rally_processes(ray_instance):
    worker = actor.create_actor(Worker, host="localhost")
    pid = ray_instance.get(worker.pid.remote())

    assert process.is_rally_actor_process(psutil.Process(pid))
    assert pid in [p.pid for p in process.find_all_other_rally_processes()]
    actor.kill_actor(worker)


def test_actors_log_with_their_address(ray_instance):
    echo = actor.create_actor(EchoActor, host="localhost")
    _, pid = ray_instance.get(echo.echo.remote("logged"))
    actor.kill_actor(echo)

    log_file = os.path.join(os.environ["RALLY_HOME"], ".rally", "logs", "rally.log")
    deadline = time.monotonic() + 10
    expected = f"echo/PID:{pid} tests.ray_test INFO Echoing [logged]"
    while time.monotonic() < deadline:
        if os.path.exists(log_file):
            with open(log_file, encoding="utf-8") as f:
                if expected in f.read():
                    return
        time.sleep(0.1)
    pytest.fail(f"[{expected}] not found in [{log_file}]")


def test_actors_do_not_read_from_the_terminal(ray_instance):
    # If Rally runs in a terminal, child processes of actors that read from it would stop the actor (SIGTTIN).
    echo = actor.create_actor(EchoActor, host="localhost")
    devnull = os.stat(os.devnull)

    assert ray_instance.get(echo.stdin.remote()) == (devnull.st_dev, devnull.st_ino)
    actor.kill_actor(echo)
