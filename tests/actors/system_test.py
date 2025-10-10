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
import sys
from collections.abc import Generator
from typing import Any

import pytest

from esrally import actors, config, types
from esrally.actors._config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_COORDINATOR_IP,
    DEFAULT_FALLBACK_SYSTEM_BASE,
    DEFAULT_IP,
    DEFAULT_LOOP_INTERVAL,
    DEFAULT_PROCESS_STARTUP_METHOD,
    DEFAULT_SYSTEM_BASE,
    ActorConfig,
    ProcessStartupMethod,
    SystemBase,
)
from esrally.actors._context import CONTEXT
from esrally.actors._proto import PingRequest
from esrally.utils import cases

SAME_IP: str = "192.168.23.3"
OTHER_IP: str = "192.168.23.2"


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
    # "Thespian ActorSystem Name": DEFAULT_SYSTEM_BASE,
    "coordinator": True,
}


@dataclasses.dataclass
class SystemCase:
    cfg: types.Config = None
    system_base: str = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_ports: range = DEFAULT_ADMIN_PORTS
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    process_startup_method: ProcessStartupMethod = DEFAULT_PROCESS_STARTUP_METHOD
    loop_interval: float = DEFAULT_LOOP_INTERVAL
    want_capabilities: dict[str, Any] = dataclasses.field(default_factory=dict)
    want_error: Exception | None = None


@cases.cases(
    default=SystemCase(),
    multiprocQueueBase=SystemCase(
        system_base="multiprocQueueBase",
        want_capabilities={
            "Thespian ActorSystem Name": "multiprocQueueBase",
        },
    ),
    multiprocTCPBase=SystemCase(
        system_base="multiprocTCPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocTCPBase", "ip": DEFAULT_IP}
    ),
    ip=SystemCase(system_base="multiprocTCPBase", ip="0.0.0.0", want_capabilities={"ip": "0.0.0.0"}),
    admin_ports=SystemCase(system_base="multiprocTCPBase", admin_ports=range(12340, 12345)),
    coordinator_ip=SystemCase(
        system_base="multiprocTCPBase",
        coordinator_ip=OTHER_IP,
        want_capabilities={"coordinator": False, "Convention Address.IPv4": OTHER_IP},
    ),
    same_coordinator_ip=SystemCase(
        system_base="multiprocTCPBase",
        ip=SAME_IP,
        coordinator_ip=SAME_IP,
        want_capabilities={"coordinator": True, "ip": SAME_IP, "Convention Address.IPv4": SAME_IP},
    ),
    fork=SystemCase(
        system_base="multiprocTCPBase",
        process_startup_method="fork",
        want_capabilities={"Process Startup Method": "fork"},
    ),
    spawn=SystemCase(
        system_base="multiprocQueueBase",
        process_startup_method="spawn",
        want_capabilities={"Process Startup Method": "spawn"},
    ),
    loop_interval=SystemCase(
        loop_interval=2.0,
    ),
)
@pytest.mark.asyncio
async def test_system(case: SystemCase, event_loop: asyncio.AbstractEventLoop) -> None:
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
    if case.process_startup_method != DEFAULT_PROCESS_STARTUP_METHOD:
        cfg.process_startup_method = case.process_startup_method
    if case.loop_interval != DEFAULT_LOOP_INTERVAL:
        cfg.loop_interval = case.loop_interval
    config.init_config(cfg)

    if sys.platform == "darwin" and sys.version_info < (3, 12) and cfg.process_startup_method == "fork":
        # Old versions of Python on OSX have known problems with fork.
        pytest.skip("There are known issues with OSX, Python < 3.12 and fork.")

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

        if case.admin_ports:
            want_ports = set(cfg.admin_ports or [])
            assert system.capabilities.get("Admin Port") in want_ports

        ctx = actors.get_actor_context()

        # It verifies capabilities are copied to the cfg object to be sent to actor subprocesses.
        assert system.capabilities["Thespian ActorSystem Name"] == ctx.cfg.system_base
        if "ip" in system.capabilities:
            assert system.capabilities["ip"] == ctx.cfg.ip
            assert range(system.capabilities["Admin Port"], system.capabilities["Admin Port"] + 1) == ctx.cfg.admin_ports
        assert system.capabilities["Process Startup Method"] == ctx.cfg.process_startup_method

        assert ctx.cfg.loop_interval == case.loop_interval

        assert isinstance(system, actors.ActorSystem)
        destination = await actors.create_actor(actors.AsyncActor)
        assert isinstance(destination, actors.ActorAddress)

        request = PingRequest(message=random.random())
        response = await actors.request(destination, request)
        if isinstance(response, actors.PoisonMessage):
            pytest.fail(f"Actor responded with poison message: message={response.poisonMessage!r}, details={response.details!r}")
        assert response == request.message

        value = random.random()
        assert [value] == await asyncio.gather(actors.ping(destination, message=value))  # type: ignore[comparison-overlap]

        assert system is actors.get_actor_system()
    finally:
        actors.shutdown()

    with pytest.raises(actors.ActorContextError):
        actors.get_actor_system()
