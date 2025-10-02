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
import copy
import dataclasses
import random
from collections.abc import Generator
from typing import Any

import pytest

from esrally import actors, config, types
from esrally.actors._config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_COORDINATOR_IP,
    DEFAULT_COORDINATOR_PORT,
    DEFAULT_EXTERNAL_REQUEST_POLL_INTERVAL,
    DEFAULT_FALLBACK_SYSTEM_BASE,
    DEFAULT_IP,
    DEFAULT_PROCESS_STARTUP_METHOD,
    DEFAULT_SYSTEM_BASE,
    DEFAULT_TRY_JOIN,
    ActorConfig,
    ProcessStartupMethod,
    SystemBase,
)
from esrally.actors._context import CONTEXT
from esrally.actors._proto import PingRequest, PongResponse
from esrally.utils import cases


@pytest.fixture(scope="function", autouse=True)
def clean_event_loop() -> Generator[None, Any, None]:
    asyncio.set_event_loop(None)
    yield
    try:
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(None)
        loop.close()
    except RuntimeError:
        pass


@pytest.fixture(scope="function", autouse=True)
def clean_context():
    token = CONTEXT.set(None)
    try:
        yield
    finally:
        actors.shutdown()
        CONTEXT.reset(token)


@pytest.fixture(scope="function", autouse=True)
def clean_config():
    token = config.CONFIG.set(None)
    try:
        yield
    finally:
        config.clear_config()
        config.CONFIG.reset(token)


WANT_CAPABILITIES = {
    "Thespian ActorSystem Name": DEFAULT_SYSTEM_BASE,
    "coordinator": True,
}


@dataclasses.dataclass
class SystemCase:
    cfg: types.Config = None
    system_base: str = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_ports: int | range = DEFAULT_ADMIN_PORTS
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    coordinator_port: int = DEFAULT_COORDINATOR_PORT
    process_startup_method: ProcessStartupMethod = DEFAULT_PROCESS_STARTUP_METHOD
    try_join: bool = DEFAULT_TRY_JOIN
    external_request_poll_interval: float | None = DEFAULT_EXTERNAL_REQUEST_POLL_INTERVAL
    want_capabilities: dict[str, Any] = dataclasses.field(default_factory=dict)


@cases.cases(
    default=SystemCase(),
    multiprocQueueBase=SystemCase(
        system_base="multiprocQueueBase",
        want_capabilities={
            "Thespian ActorSystem Name": "multiprocQueueBase",
            "Process Startup Method": "spawn",
        },
    ),
    multiprocTCPBase=SystemCase(
        system_base="multiprocTCPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocTCPBase", "ip": DEFAULT_IP}
    ),
    fallback_system_base=SystemCase(
        system_base="invalid-value",
        want_capabilities={"Thespian ActorSystem Name": "multiprocQueueBase"},
    ),
    ip=SystemCase(ip="0.0.0.0", want_capabilities={"ip": "0.0.0.0"}),
    admin_ports=SystemCase(admin_ports=12345, want_capabilities={"Admin Port": 12345}),
    coordinator_ip=SystemCase(
        coordinator_ip="192.168.0.1", want_capabilities={"coordinator": False, "Convention Address.IPv4": "192.168.0.1"}
    ),
    coordinator_port=SystemCase(
        coordinator_port=3210,
        coordinator_ip="192.168.0.2",
        want_capabilities={"coordinator": False, "Convention Address.IPv4": "192.168.0.2:3210"},
    ),
    same_ip=SystemCase(
        ip="192.168.0.3",
        coordinator_ip="192.168.0.3",
        want_capabilities={"coordinator": True, "ip": "192.168.0.3", "Convention Address.IPv4": "192.168.0.3"},
    ),
    fork=SystemCase(
        process_startup_method="fork",
        want_capabilities={"Process Startup Method": "fork"},
    ),
    spawn=SystemCase(
        process_startup_method="spawn",
        want_capabilities={"Process Startup Method": "spawn"},
    ),
    forkserver=SystemCase(
        process_startup_method="forkserver",
        want_capabilities={"Process Startup Method": "forkserver"},
    ),
    no_try_join=SystemCase(
        try_join=False,
    ),
    external_request_poll_interval=SystemCase(
        external_request_poll_interval=0.5,
    ),
)
@pytest.mark.asyncio
async def test_system(case: SystemCase, event_loop: asyncio.AbstractEventLoop) -> None:
    if case.process_startup_method in ["forkserver", "spawn"]:
        pytest.skip(reason="Some issues has been encountered with other methods than fork.")
    cfg = ActorConfig()
    if case.system_base != DEFAULT_SYSTEM_BASE:
        cfg.system_base = case.system_base
    if case.fallback_system_base != DEFAULT_FALLBACK_SYSTEM_BASE:
        cfg.fallback_system_base = case.fallback_system_base
    if case.ip != DEFAULT_IP:
        cfg.ip = case.ip
    if case.admin_ports != DEFAULT_ADMIN_PORTS:
        cfg.admin_ports = case.admin_ports
    if case.coordinator_ip != DEFAULT_COORDINATOR_IP:
        cfg.coordinator_ip = case.coordinator_ip
    if case.coordinator_port != DEFAULT_COORDINATOR_PORT:
        cfg.coordinator_port = case.coordinator_port
    if case.process_startup_method != DEFAULT_PROCESS_STARTUP_METHOD:
        cfg.process_startup_method = case.process_startup_method
    if case.try_join != DEFAULT_TRY_JOIN:
        cfg.try_join = case.try_join
    if case.external_request_poll_interval != DEFAULT_EXTERNAL_REQUEST_POLL_INTERVAL:
        cfg.external_request_poll_interval = case.external_request_poll_interval
    config.init_config(cfg)

    with pytest.raises(actors.ActorContextError):
        actors.get_actor_system()

    system = actors.init_actor_system()

    assert isinstance(system, actors.ActorSystem)
    assert system is actors.get_actor_system()
    try:
        want_capabilities = copy.deepcopy(WANT_CAPABILITIES)
        want_capabilities.update(case.want_capabilities)
        for name, value in want_capabilities.items():
            assert system.capabilities.get(name) == value

        ctx = actors.get_actor_context()
        assert ctx.external_request_poll_interval == case.external_request_poll_interval

        assert isinstance(system, actors.ActorSystem)
        destination = actors.create_actor(actors.AsyncActor)
        assert isinstance(destination, actors.ActorAddress)

        request = PingRequest(message=random.random())
        response = system.ask(destination, request)
        if isinstance(response, actors.PoisonMessage):
            pytest.fail(f"Actor responded with poison message: message={response.poisonMessage!r}, details={response.details!r}")
        assert response == PongResponse(request.req_id, status=request.message)

        value = random.random()
        assert [value] == await asyncio.gather(actors.ping(destination, message=value))  # type: ignore[comparison-overlap]

        assert system is actors.get_actor_system()
    finally:
        actors.shutdown()

    with pytest.raises(actors.ActorContextError):
        actors.get_actor_system()
