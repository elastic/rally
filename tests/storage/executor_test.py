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

from esrally.storage._config import DEFAULT_STORAGE_CONFIG, StorageConfig
from esrally.storage._executor import executor_from_config

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="function", params=["fork", "spawn", "forkserver"])
def process_startup_method(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str | None:
    monkeypatch.setattr(DEFAULT_STORAGE_CONFIG, "process_startup_method", request.param)
    return request.param


@pytest.fixture(scope="function", params=["multiprocTCPBase", "multiprocQueueBase", "multiprocUDPBase"])
def multiproc_system_base(request: pytest.FixtureRequest, process_startup_method: str) -> str:
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
    cfg: StorageConfig
    want_same_pid: bool = True
    want_same_hostname: bool = True


def test_executor_threading():
    _test_executor(ExecutorCase(cfg=StorageConfig(use_threads=True)))


def test_simple_executor(simple_actor_system: actors.ActorSystem):
    _test_executor(ExecutorCase(cfg=StorageConfig(use_threads=False, subprocess_log_level=logging.DEBUG), want_same_pid=False))


def test_multiproc_executor(multiproc_actor_system: actors.ActorSystem):
    _test_executor(ExecutorCase(cfg=StorageConfig(use_threads=False, subprocess_log_level=logging.DEBUG), want_same_pid=False))


def _test_executor(case: ExecutorCase):
    executor = executor_from_config(case.cfg)
    got_hostname = executor.submit(socket.gethostname).result()
    assert (got_hostname == socket.gethostname()) is case.want_same_hostname

    got_pid = executor.submit(get_pid).result()
    assert (got_pid == get_pid()) is case.want_same_pid

    executor.submit(log_some_message, "message logged from child").result()


def log_some_message(msg: str):
    LOG.info("%s (pid=%d)", msg, os.getpid())


def get_pid():
    return os.getpid()
