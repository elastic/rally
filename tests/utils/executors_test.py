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
import os
import socket
from collections import abc

import pytest
from thespian import actors

from esrally import actor, config
from esrally.utils import executors

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="function", params=[None, "fork", "spawn", "forkserver"])
def process_startup_method(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str | None:
    monkeypatch.setattr(actor, "PROCESS_STARTUP_METHOD", request.param)
    return request.param


@pytest.fixture(scope="function", params=["multiprocTCPBase", "multiprocQueueBase", "multiprocUDPBase"])
def multiproc_system_base(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(scope="function")
def simple_actor_system() -> abc.Iterator[actors.ActorSystem]:
    asys = actors.ActorSystem("simpleSystemBase")
    yield asys
    asys.shutdown()


@pytest.fixture(scope="function")
def multiproc_actor_system(multiproc_system_base: str, process_startup_method: str | None) -> abc.Iterator[actors.ActorSystem]:
    capabilities = {}
    if process_startup_method:
        capabilities["Process Starting Method"] = process_startup_method
    asys = actors.ActorSystem(multiproc_system_base, capabilities)
    try:
        yield asys
    finally:
        asys.shutdown()


@dataclasses.dataclass
class ExecutorCase:
    use_threading: bool = False
    process_startup_method: str | None = None
    want_same_pid: bool = True
    want_same_hostname: bool = True
    system_base: str | None = None


def test_executor_threading():
    _test_executor(ExecutorCase(use_threading=True))


def test_simple_executor(simple_actor_system: actors.ActorSystem):
    _test_executor(ExecutorCase(want_same_pid=False))


def test_multiproc_executor(multiproc_actor_system: actors.ActorSystem, multiproc_system_base: str, process_startup_method: str | None):
    _test_executor(ExecutorCase(want_same_pid=False, system_base=multiproc_system_base, process_startup_method=process_startup_method))


def _test_executor(case: ExecutorCase) -> None:
    cfg = executors.ExecutorsConfig()
    cfg.add(config.Scope.application, "executors", "executors.use_threading", case.use_threading)
    cfg.add(config.Scope.application, "executors", "executors.forwarder.log.level", logging.DEBUG)
    cfg.add(config.Scope.application, "actor", "actor.process.startup.method", case.process_startup_method or "")
    if case.system_base:
        cfg.add(config.Scope.application, "actor", "actor.system.base", case.system_base)
    executor = executors.Executor.from_config(cfg)

    got_hostname = executor.submit(socket.gethostname).result()
    assert (got_hostname == socket.gethostname()) is case.want_same_hostname

    got_pid = executor.submit(get_pid).result()
    assert (got_pid == get_pid()) is case.want_same_pid

    executor.submit(log_some_message, "message logged from child").result()


def log_some_message(msg: str):
    LOG.info("%s (pid=%d)", msg, os.getpid())


def get_pid():
    return os.getpid()
