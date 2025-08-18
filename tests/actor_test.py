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

from esrally import actor, exceptions
from esrally.utils import cases, net


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


def resolve(ip: str):
    return f"{ip}!r"


@pytest.fixture(autouse=True)
def mock_net_resolve(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(net, "resolve", mock.create_autospec(net.resolve, side_effect=resolve))


@dataclasses.dataclass
class BootstrapActorSystemCase:
    system_base: str = "multiprocTCPBase"
    offline: bool = False
    already_running: bool = False
    try_join: bool = False
    prefer_local_only: bool = False
    local_ip: str | None = None
    coordinator_ip: str | None = None
    coordinator_port: int | None = None
    want_error: tuple[Exception, ...] = tuple()
    want_connect: tuple[str, int] | None = None
    want_system_base: str = "multiprocTCPBase"
    want_capabilities: dict[str, typing.Any] | None = None
    want_log_defs: bool = False


@cases.cases(
    default=BootstrapActorSystemCase(
        want_error=(
            exceptions.SystemSetupError("coordinator IP is required"),
            exceptions.SystemSetupError("local IP is required"),
        )
    ),
    tcp=BootstrapActorSystemCase(
        system_base="multiprocTCPBase",
        want_error=(
            exceptions.SystemSetupError("coordinator IP is required"),
            exceptions.SystemSetupError("local IP is required"),
        ),
    ),
    udp=BootstrapActorSystemCase(
        system_base="multiprocUDPBase",
        want_error=(
            exceptions.SystemSetupError("coordinator IP is required"),
            exceptions.SystemSetupError("local IP is required"),
        ),
    ),
    no_local_ip=BootstrapActorSystemCase(coordinator_ip="127.0.0.1", want_error=(exceptions.SystemSetupError("local IP is required"),)),
    no_coordinator_ip=BootstrapActorSystemCase(
        local_ip="127.0.0.1", want_error=(exceptions.SystemSetupError("coordinator IP is required"),)
    ),
    offline=BootstrapActorSystemCase(
        offline=True,
        want_error=(exceptions.SystemSetupError("Rally requires a network-capable system base but got [multiprocQueueBase]."),),
    ),
    try_join=BootstrapActorSystemCase(
        try_join=True,
        want_capabilities={"coordinator": True},
        want_log_defs=True,
        want_connect=("127.0.0.1", 1900),
    ),
    try_join_already_running=BootstrapActorSystemCase(
        try_join=True,
        already_running=True,
        want_connect=("127.0.0.1", 1900),
    ),
    prefer_local_only=BootstrapActorSystemCase(
        prefer_local_only=True,
        want_capabilities={"Convention Address.IPv4": f"{resolve('127.0.0.1')}:1900", "coordinator": True, "ip": resolve("127.0.0.1")},
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
            "Convention Address.IPv4": f"{resolve('192.168.0.41')}:1900",
            "coordinator": True,
            "ip": resolve("192.168.0.41"),
        },
        want_log_defs=True,
    ),
    non_coordinator_node=BootstrapActorSystemCase(
        local_ip="192.168.0.42",
        coordinator_ip="192.168.0.41",
        want_capabilities={
            "Convention Address.IPv4": f"{resolve('192.168.0.41')}:1900",
            "coordinator": False,
            "ip": resolve("192.168.0.42"),
        },
        want_log_defs=True,
    ),
    coordinator_port=BootstrapActorSystemCase(
        coordinator_ip="192.168.0.1",
        coordinator_port=1234,
        local_ip="192.168.0.2",
        want_capabilities={"Convention Address.IPv4": f"{resolve('192.168.0.1')}:1234", "coordinator": False, "ip": resolve("192.168.0.2")},
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
    try:
        got = actor.bootstrap_actor_system(
            try_join=case.try_join,
            prefer_local_only=case.prefer_local_only,
            local_ip=case.local_ip,
            coordinator_ip=case.coordinator_ip,
            coordinator_port=case.coordinator_port,
        )
    except tuple(type(e) for e in case.want_error) as ex:  # pylint: disable=catching-non-exception
        assert str(ex) in {str(e) for e in case.want_error}
        return
    assert got is not None
    assert got.systemBase == case.want_system_base
    assert got.capabilities == case.want_capabilities
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
    want: bool = False
    want_connect: tuple[str, int] | None = ("127.0.0.1", 1900)


@cases.cases(
    default=ActorSystemAlreadyRunningCase(),
    ip_and_port=ActorSystemAlreadyRunningCase(ip="10.0.0.1", port=1000, want_connect=("10.0.0.1", 1000)),
    already_running=ActorSystemAlreadyRunningCase(already_running=True, want=True),
    ip_and_port_already_running=ActorSystemAlreadyRunningCase(
        ip="10.0.0.1", port=1000, already_running=True, want=True, want_connect=("10.0.0.1", 1000)
    ),
    system_base_queue=ActorSystemAlreadyRunningCase(already_running=False, want=False, system_base="multiprocQueueBase", want_connect=None),
    system_base_tcp=ActorSystemAlreadyRunningCase(already_running=False, want=False, system_base="multiprocTCPBase"),
    system_base_tcp_already_running=ActorSystemAlreadyRunningCase(already_running=True, want=True, system_base="multiprocTCPBase"),
    system_base_udp=ActorSystemAlreadyRunningCase(already_running=False, want=False, system_base="multiprocUDPBase", want_connect=None),
)
def test_actor_system_already_running(
    case: ActorSystemAlreadyRunningCase,
    mock_socket: socket.socket,
):
    if not case.already_running:
        mock_socket.connect.side_effect = socket.error

    got = actor.actor_system_already_running(ip=case.ip, port=case.port, system_base=case.system_base)
    assert got is case.want
    if case.want_connect:
        mock_socket.connect.assert_called_once_with(case.want_connect)
    else:
        mock_socket.connect.assert_not_called()
