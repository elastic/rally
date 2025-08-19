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
import socket
import typing
from unittest import mock

import pytest
import thespian.actors

from esrally import actor, log
from esrally.utils import cases


@pytest.fixture(autouse=True)
def mock_socket(monkeypatch: pytest.MonkeyPatch) -> socket.socket:
    sock_cls = mock.create_autospec(socket.socket)
    sock = sock_cls.return_value
    assert isinstance(sock, socket.socket)
    sock.__enter__.return_value = sock
    sock_cls.return_value = sock
    monkeypatch.setattr(socket, "socket", sock_cls)
    return sock


@dataclasses.dataclass
class DummyActorSystem:
    systemBase: str
    capabilities: dict[str, typing.Any] | None = None
    logDefs: typing.Any = None
    transientUnique: bool = False


@pytest.fixture(autouse=True)
def dummy_actor_system(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(thespian.actors, "ActorSystem", DummyActorSystem)


def resolve(host: str, port: int | None = None):
    return f"{host}!r", port or None


@pytest.fixture(autouse=True)
def mock_resolve(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(actor, "resolve", mock.create_autospec(actor.resolve, side_effect=resolve))


@pytest.fixture(autouse=True)
def mock_load_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(log, "load_configuration", lambda: {"some": "config"})


@dataclasses.dataclass
class BootstrapActorSystemCase:
    system_base: actor.SystemBase = "multiprocTCPBase"
    offline: bool = False
    process_startup_method: actor.ProcessStartupMethod | None = None
    already_running: bool = False
    try_join: bool = False
    prefer_local_only: bool = False
    local_ip: str | None = None
    admin_port: int | None = None
    coordinator_ip: str | None = None
    coordinator_port: int | None = None
    want_error: tuple[Exception, ...] = tuple()
    want_connect: tuple[str, int] | None = None
    want_system_base: actor.SystemBase = "multiprocTCPBase"
    want_capabilities: dict[str, typing.Any] | None = None
    want_log_defs: bool = False


@cases.cases(
    default=BootstrapActorSystemCase(
        want_capabilities={"coordinator": True},
        want_log_defs=True,
    ),
    tcp=BootstrapActorSystemCase(
        system_base="multiprocTCPBase",
        want_capabilities={"coordinator": True},
        want_log_defs=True,
    ),
    udp=BootstrapActorSystemCase(
        system_base="multiprocUDPBase",
        want_capabilities={"coordinator": True},
        want_log_defs=True,
        want_system_base="multiprocUDPBase",
    ),
    queue=BootstrapActorSystemCase(
        system_base="multiprocQueueBase",
        want_capabilities={"coordinator": True},
        want_log_defs=True,
        want_system_base="multiprocQueueBase",
    ),
    local_ip=BootstrapActorSystemCase(
        local_ip="127.0.0.1",
        want_capabilities={"coordinator": True, "ip": "127.0.0.1!r"},
        want_log_defs=True,
    ),
    admin_port=BootstrapActorSystemCase(
        admin_port=1024,
        want_capabilities={"coordinator": True, "Admin Port": 1024},
        want_log_defs=True,
    ),
    coordinator_ip=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        want_capabilities={"Convention Address.IPv4": "192.168.0.1!r", "coordinator": True},
        want_log_defs=True,
    ),
    coordinator_ip_and_port=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        coordinator_port=2000,
        want_capabilities={"Convention Address.IPv4": "192.168.0.1!r:2000", "coordinator": True},
        want_log_defs=True,
    ),
    local_ip_is_coordinator_ip=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        local_ip="192.168.0.1",
        want_capabilities={"Convention Address.IPv4": "192.168.0.1!r", "coordinator": True, "ip": "192.168.0.1!r"},
        want_log_defs=True,
    ),
    local_ip_is_not_coordinator_ip=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        local_ip="192.168.0.2",
        want_capabilities={"Convention Address.IPv4": "192.168.0.1!r", "coordinator": False, "ip": "192.168.0.2!r"},
        want_log_defs=True,
    ),
    offline=BootstrapActorSystemCase(
        offline=True,
        want_system_base="multiprocQueueBase",
        want_capabilities={"coordinator": True},
        want_log_defs=True,
    ),
    try_join=BootstrapActorSystemCase(
        try_join=True,
        want_capabilities={"coordinator": True},
        want_log_defs=True,
        want_connect=("127.0.0.1", 1900),
    ),
    try_join_offline=BootstrapActorSystemCase(
        try_join=True,
        offline=True,
        want_system_base="multiprocQueueBase",
    ),
    try_join_already_running=BootstrapActorSystemCase(
        try_join=True,
        already_running=True,
        want_connect=("127.0.0.1", 1900),
    ),
    try_join_offline_already_running=BootstrapActorSystemCase(
        try_join=True,
        offline=True,
        already_running=True,
        want_system_base="multiprocQueueBase",
    ),
    try_join_already_running_with_ip_and_port=BootstrapActorSystemCase(
        try_join=True,
        already_running=True,
        local_ip="10.0.0.2",
        admin_port=2000,
        want_connect=("10.0.0.2", 2000),
    ),
    try_join_process_startup_method=BootstrapActorSystemCase(
        try_join=True,
        process_startup_method="spawn",
        want_connect=("127.0.0.1", 1900),
        want_capabilities={"coordinator": True, "Process Startup Method": "spawn"},
        want_log_defs=True,
    ),
    prefer_local_only=BootstrapActorSystemCase(
        prefer_local_only=True,
        want_capabilities={"Convention Address.IPv4": "127.0.0.1!r", "coordinator": True, "ip": "127.0.0.1!r"},
        want_log_defs=True,
    ),
    prefer_local_only_offline=BootstrapActorSystemCase(
        offline=True,
        prefer_local_only=True,
        want_capabilities={"coordinator": True},
        want_system_base="multiprocQueueBase",
        want_log_defs=True,
    ),
    coordinator_node=BootstrapActorSystemCase(
        local_ip="192.168.0.41",
        coordinator_ip="192.168.0.41",
        want_capabilities={
            "Convention Address.IPv4": "192.168.0.41!r",
            "coordinator": True,
            "ip": "192.168.0.41!r",
        },
        want_log_defs=True,
    ),
    non_coordinator_node=BootstrapActorSystemCase(
        local_ip="192.168.0.42",
        coordinator_ip="192.168.0.41",
        want_capabilities={
            "Convention Address.IPv4": "192.168.0.41!r",
            "coordinator": False,
            "ip": "192.168.0.42!r",
        },
        want_log_defs=True,
    ),
    coordinator_port=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        coordinator_port=1234,
        local_ip="192.168.0.2",
        want_capabilities={"Convention Address.IPv4": "192.168.0.1!r:1234", "coordinator": False, "ip": "192.168.0.2!r"},
        want_log_defs=True,
    ),
    process_startup_method_fork=BootstrapActorSystemCase(
        process_startup_method="fork",
        want_capabilities={
            "Process Startup Method": "fork",
            "coordinator": True,
        },
        want_log_defs=True,
    ),
    process_startup_method_spawn=BootstrapActorSystemCase(
        process_startup_method="spawn",
        want_capabilities={
            "Process Startup Method": "spawn",
            "coordinator": True,
        },
        want_log_defs=True,
    ),
    process_startup_method_forkserver=BootstrapActorSystemCase(
        process_startup_method="forkserver",
        want_capabilities={
            "Process Startup Method": "forkserver",
            "coordinator": True,
        },
        want_log_defs=True,
    ),
)
def test_bootstrap_actor_system(
    monkeypatch: pytest.MonkeyPatch,
    case: BootstrapActorSystemCase,
    mock_socket: socket.socket,
) -> None:
    if not case.already_running:
        mock_socket.connect.side_effect = socket.error

    monkeypatch.setattr(actor, "__SYSTEM_BASE", case.system_base)
    if case.offline:
        actor.use_offline_actor_system()

    monkeypatch.setattr(actor, "__PROCESS_STARTUP_METHOD", None)
    if case.process_startup_method:
        actor.set_startup_method(case.process_startup_method)

    try:
        got = actor.bootstrap_actor_system(
            try_join=case.try_join,
            prefer_local_only=case.prefer_local_only,
            local_ip=case.local_ip,
            admin_port=case.admin_port,
            coordinator_ip=case.coordinator_ip,
            coordinator_port=case.coordinator_port,
        )
    except tuple(type(e) for e in case.want_error) as ex:  # pylint: disable=catching-non-exception
        assert str(ex) in {str(e) for e in case.want_error}
        return
    assert got is not None
    assert got.systemBase == case.want_system_base
    assert (got.capabilities or {}) == (case.want_capabilities or {})
    assert bool(got.logDefs) == case.want_log_defs
    if case.want_connect:
        mock_socket.connect.assert_called_once_with(case.want_connect)
    else:
        mock_socket.connect.assert_not_called()


@dataclasses.dataclass
class ActorSystemAlreadyRunningCase:
    already_running: bool = False
    ip: str | None = None
    port: int | None = None
    system_base: actor.SystemBase | None = None
    want: bool = None
    want_error: Exception | None = None
    want_connect: tuple[str, int] | None = None


@cases.cases(
    default=ActorSystemAlreadyRunningCase(want_connect=("127.0.0.1", 1900), want=False),
    ip_and_port=ActorSystemAlreadyRunningCase(ip="10.0.0.1", port=1000, want_connect=("10.0.0.1", 1000), want=False),
    already_running=ActorSystemAlreadyRunningCase(already_running=True, want_connect=("127.0.0.1", 1900), want=True),
    ip_and_port_already_running=ActorSystemAlreadyRunningCase(
        ip="10.0.0.1", port=1000, already_running=True, want=True, want_connect=("10.0.0.1", 1000)
    ),
    system_base_simple=ActorSystemAlreadyRunningCase(
        system_base="simpleSystemBase", want_error=ValueError("unsupported system base: simpleSystemBase")
    ),
    system_base_queue=ActorSystemAlreadyRunningCase(
        system_base="multiprocQueueBase", want_error=ValueError("unsupported system base: multiprocQueueBase")
    ),
    system_base_tcp=ActorSystemAlreadyRunningCase(system_base="multiprocTCPBase", want=False, want_connect=("127.0.0.1", 1900)),
    system_base_tcp_already_running=ActorSystemAlreadyRunningCase(
        already_running=True, system_base="multiprocTCPBase", want=True, want_connect=("127.0.0.1", 1900)
    ),
    system_base_udp=ActorSystemAlreadyRunningCase(
        system_base="multiprocUDPBase", want_error=ValueError("unsupported system base: multiprocUDPBase")
    ),
)
def test_actor_system_already_running(
    case: ActorSystemAlreadyRunningCase,
    mock_socket: socket.socket,
):
    if not case.already_running:
        mock_socket.connect.side_effect = socket.error

    try:
        got = actor.actor_system_already_running(ip=case.ip, port=case.port, system_base=case.system_base)
    except type(case.want_error) if case.want_error else tuple() as ex:
        assert str(ex) == str(case.want_error)
        return
    else:
        assert case.want_error is None
    assert got is case.want
    if case.want_connect:
        mock_socket.connect.assert_called_once_with(case.want_connect)
    else:
        mock_socket.connect.assert_not_called()
