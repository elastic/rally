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
from typing import get_args

from esrally.actors._config import (
    DEFAULT_ADMIN_PORTS,
    DEFAULT_COORDINATOR_IP,
    DEFAULT_COORDINATOR_PORT,
    DEFAULT_FALLBACK_SYSTEM_BASE,
    DEFAULT_IP,
    DEFAULT_PROCESS_STARTUP_METHOD,
    DEFAULT_SYSTEM_BASE,
    ActorConfig,
    SystemBase,
)
from esrally.utils import cases, convert


@dataclasses.dataclass
class FromConfigCase:
    name: str | None = None
    system_base: SystemBase = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_ports: range | int = DEFAULT_ADMIN_PORTS
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    coordinator_port: int = DEFAULT_COORDINATOR_PORT
    process_startup_method: str | None = DEFAULT_PROCESS_STARTUP_METHOD
    want_name: str | None = None


@cases.cases(
    default=FromConfigCase(),
    with_name=FromConfigCase(name="some_name", want_name="some_name"),
    system_base=FromConfigCase(system_base="multiprocQueueBase"),
    fallback_system_base=FromConfigCase(fallback_system_base="multiprocTCPBase"),
    fallback_system_base_none=FromConfigCase(fallback_system_base=None),
    ip=FromConfigCase(ip="some_ip"),
    admin_ports=FromConfigCase(admin_ports=1234),
    coordinator_ip=FromConfigCase(coordinator_ip="some_ip"),
    coordinator_port=FromConfigCase(coordinator_port=4321),
    fork=FromConfigCase(process_startup_method="fork"),
    forkserver=FromConfigCase(process_startup_method="forkserver"),
    spawn=FromConfigCase(process_startup_method="spawn"),
)
def test_from_config(case: FromConfigCase) -> None:
    cfg = ActorConfig(case.name)
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
    assert isinstance(cfg, ActorConfig)
    assert cfg.name == case.want_name
    assert cfg.system_base in get_args(SystemBase)
    assert cfg.system_base == case.system_base
    assert cfg.fallback_system_base in (get_args(SystemBase) + (None,))
    assert cfg.fallback_system_base == case.fallback_system_base
    assert cfg.ip == case.ip
    assert cfg.admin_ports == convert.to_range(case.admin_ports)
    assert cfg.coordinator_ip == case.coordinator_ip
    assert cfg.coordinator_port == case.coordinator_port
