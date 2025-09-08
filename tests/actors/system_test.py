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
import uuid
from typing import Any

import pytest
from thespian import actors

from esrally import types
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
from esrally.actors._system import (
    _CURRENT_SYSTEM,
    ActorSystem,
    ask,
    init_system,
    shutdown_system,
    system,
)
from esrally.utils import cases


@pytest.fixture(scope="function", autouse=True)
def clean_current_system():
    token = _CURRENT_SYSTEM.set(None)
    try:
        yield
    finally:
        shutdown_system()
        _CURRENT_SYSTEM.reset(token)


WANT_CAPABILITIES = {
    "Thespian ActorSystem Name": DEFAULT_SYSTEM_BASE,
    "coordinator": True,
}


@dataclasses.dataclass
class FromConfigCase:
    cfg: types.AnyConfig = None
    system_base: str = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_port: int = DEFAULT_ADMIN_PORT
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    coordinator_port: int = DEFAULT_COORDINATOR_PORT
    want_capabilities: dict[str, Any] = dataclasses.field(default_factory=dict)


@cases.cases(
    default=FromConfigCase(),
    simpleSystemBase=FromConfigCase(system_base="simpleSystemBase", want_capabilities={"Thespian ActorSystem Name": "simpleSystem"}),
    multiprocQueueBase=FromConfigCase(
        system_base="multiprocQueueBase", want_capabilities={"Thespian ActorSystem Name": "multiprocQueueBase"}
    ),
    multiprocTCPBase=FromConfigCase(
        system_base="multiprocTCPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocTCPBase", "ip": DEFAULT_IP}
    ),
    multiprocUDPBase=FromConfigCase(
        system_base="multiprocUDPBase", want_capabilities={"Thespian ActorSystem Name": "multiprocUDPBase", "ip": DEFAULT_IP}
    ),
    fallback_system_base=FromConfigCase(
        system_base="invalid-value",
        fallback_system_base="simpleSystemBase",
        want_capabilities={"Thespian ActorSystem Name": "simpleSystem"},
    ),
    ip=FromConfigCase(ip="0.0.0.0", want_capabilities={"ip": "0.0.0.0"}),
    admin_port=FromConfigCase(admin_port=12345, want_capabilities={"Admin Port": 12345}),
    coordinator_ip=FromConfigCase(
        coordinator_ip="192.168.0.1", want_capabilities={"coordinator": False, "Convention Address.IPv4": "192.168.0.1"}
    ),
    coordinator_port=FromConfigCase(
        coordinator_port=3210,
        coordinator_ip="192.168.0.2",
        want_capabilities={"coordinator": False, "Convention Address.IPv4": "192.168.0.2:3210"},
    ),
    same_ip=FromConfigCase(
        ip="192.168.0.3",
        coordinator_ip="192.168.0.3",
        want_capabilities={"coordinator": True, "ip": "192.168.0.3", "Convention Address.IPv4": "192.168.0.3"},
    ),
)
def test_system(case: FromConfigCase) -> None:
    cfg = ActorConfig.from_config(case.cfg)
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

    with pytest.raises(RuntimeError):
        system()

    got = init_system(cfg)

    assert isinstance(got, ActorSystem)
    try:
        want_capabilities = copy.deepcopy(WANT_CAPABILITIES)
        want_capabilities.update(case.want_capabilities)
        for name, value in want_capabilities.items():
            assert got.capabilities.get(name) == value

        assert isinstance(got, ActorSystem)
        actor_address = got.createActor(PingActor)
        assert isinstance(actor_address, actors.ActorAddress)
        ping = Ping("foo")
        got_pong = got.ask(actor_address, ping)
        assert got_pong == Pong("foo", uuid=ping.uuid)

        assert got is system()

    finally:
        shutdown_system()

    with pytest.raises(RuntimeError):
        system()


class PingActor(actors.Actor):

    def receiveMessage(self, msg, sender):
        if isinstance(msg, Ping):
            self.send(sender, Pong(msg.msg, msg.uuid))
            return
        raise TypeError(f"Invalid message type: {type(msg)}")


@dataclasses.dataclass
class Ping:
    msg: Any
    uuid: uuid.UUID = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass
class Pong:
    msg: Any
    uuid: uuid.UUID


def test_ask() -> None:
    cfg = ActorConfig.from_config()
    cfg.system_base = "simpleSystemBase"
    s = init_system(cfg)
    actor_addr = s.createActor(PingActor)

    pings = [Ping(i) for i in range(10)]
    got = [asyncio.run(ask(actor_addr, p, timeout=1)) for p in pings]
    assert got == [Pong(p.msg, p.uuid) for p in pings]
