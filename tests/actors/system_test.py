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

import asyncio
import copy
import dataclasses
import random
from collections.abc import Generator
from typing import Any

import pytest

from esrally import actors, config, types
from esrally.actors._config import (
    DEFAULT_ADMIN_PORT,
    DEFAULT_COORDINATOR_IP,
    DEFAULT_COORDINATOR_PORT,
    DEFAULT_FALLBACK_SYSTEM_BASE,
    DEFAULT_IP,
    DEFAULT_SYSTEM_BASE,
    ActorConfig,
    SystemBase,
)
from esrally.actors._context import CONTEXT
from esrally.actors._proto import RequestMessage, ResultMessage
from esrally.utils import cases


@pytest.fixture(scope="function", autouse=True)
def clean_event_loop() -> Generator[None, Any, None]:
    asyncio.set_event_loop(None)
    yield
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(None)
    loop.close()


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
    cfg: types.AnyConfig = None
    system_base: str = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_port: int = DEFAULT_ADMIN_PORT
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    coordinator_port: int = DEFAULT_COORDINATOR_PORT
    want_capabilities: dict[str, Any] = dataclasses.field(default_factory=dict)


@cases.cases(
    default=SystemCase(),
    multiprocQueueBase=SystemCase(system_base="multiprocQueueBase", want_capabilities={"Thespian ActorSystem Name": "multiprocQueueBase"}),
    multiprocTCPBase=SystemCase(
        system_base="multiprocTCPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocTCPBase", "ip": DEFAULT_IP}
    ),
    multiprocUDPBase=SystemCase(
        system_base="multiprocUDPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocUDPBase", "ip": DEFAULT_IP}
    ),
    fallback_system_base=SystemCase(
        system_base="invalid-value",
        want_capabilities={"Thespian ActorSystem Name": "multiprocQueueBase"},
    ),
    ip=SystemCase(ip="0.0.0.0", want_capabilities={"ip": "0.0.0.0"}),
    admin_port=SystemCase(admin_port=12345, want_capabilities={"Admin Port": 12345}),
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
)
def test_system(case: SystemCase, event_loop: asyncio.AbstractEventLoop) -> None:
    cfg = ActorConfig()
    if case.system_base != DEFAULT_SYSTEM_BASE:
        cfg.system_base = case.system_base
    if case.fallback_system_base != DEFAULT_FALLBACK_SYSTEM_BASE:
        cfg.fallback_system_base = case.fallback_system_base
    if case.ip != DEFAULT_IP:
        cfg.ip = case.ip
    if case.admin_port != DEFAULT_ADMIN_PORT:
        cfg.admin_port = case.admin_port
    if case.coordinator_ip != DEFAULT_COORDINATOR_IP:
        cfg.coordinator_ip = case.coordinator_ip
    if case.coordinator_port != DEFAULT_COORDINATOR_PORT:
        cfg.coordinator_port = case.coordinator_port
    config.init_config(cfg)

    with pytest.raises(actors.ContextError):
        actors.get_system()

    system = actors.init_system()

    assert isinstance(system, actors.ActorSystem)
    assert system is actors.get_system()
    try:
        want_capabilities = copy.deepcopy(WANT_CAPABILITIES)
        want_capabilities.update(case.want_capabilities)
        for name, value in want_capabilities.items():
            assert system.capabilities.get(name) == value

        assert isinstance(system, actors.ActorSystem)
        destination = actors.create(RepeatActor)
        assert isinstance(destination, actors.ActorAddress)

        value = random.random()
        got_reply = system.ask(destination, RepeatMessage(value))
        assert got_reply == value

        value = random.random()
        future_response = actors.request(destination, RepeatMessage(value))
        assert isinstance(future_response, asyncio.Future)
        assert asyncio.get_event_loop().run_until_complete(asyncio.wait_for(future_response, timeout=5)) == value

        assert system is actors.get_system()
    finally:
        actors.shutdown()

    with pytest.raises(actors.ContextError):
        actors.get_system()


@dataclasses.dataclass
class RepeatMessage:
    message: Any


class RepeatActor(actors.Actor):

    def receiveMessage(self, msg: Any, sender: actors.ActorAddress) -> None:
        if isinstance(msg, RepeatMessage):
            self.send(sender, msg.message)
            return
        if isinstance(msg, RequestMessage):
            assert isinstance(msg.message, RepeatMessage)
            self.send(sender, ResultMessage(msg.req_id, msg.message.message))
            return
        raise TypeError(f"Invalid message type: {msg}")
