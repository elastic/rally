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
    DEFAULT_ADMIN_PORT,
    DEFAULT_COORDINATOR_IP,
    DEFAULT_COORDINATOR_PORT,
    DEFAULT_FALLBACK_SYSTEM_BASE,
    DEFAULT_IP,
    DEFAULT_SYSTEM_BASE,
    ActorConfig,
    SystemBase,
)
from esrally.utils import cases


@dataclasses.dataclass
class FromConfigCase:
    name: str | None = None
    system_base: SystemBase = DEFAULT_SYSTEM_BASE
    fallback_system_base: SystemBase | None = DEFAULT_FALLBACK_SYSTEM_BASE
    ip: str = DEFAULT_IP
    admin_port: int = DEFAULT_ADMIN_PORT
    coordinator_ip: str = DEFAULT_COORDINATOR_IP
    coordinator_port: int = DEFAULT_COORDINATOR_PORT
    want_name: str | None = None


@cases.cases(
    default=FromConfigCase(),
    with_name=FromConfigCase(name="some_name", want_name="some_name"),
    system_base=FromConfigCase(system_base="multiprocUDPBase"),
    fallback_system_base=FromConfigCase(fallback_system_base="multiprocTCPBase"),
    fallback_system_base_none=FromConfigCase(fallback_system_base=None),
    ip=FromConfigCase(ip="some_ip"),
    admin_port=FromConfigCase(admin_port=1234),
    coordinator_ip=FromConfigCase(coordinator_ip="some_ip"),
    coordinator_port=FromConfigCase(coordinator_port=4321),
)
def test_from_config(case: FromConfigCase) -> None:
    got = ActorConfig(case.name)
    if case.system_base != DEFAULT_SYSTEM_BASE:
        got.system_base = case.system_base
    if case.fallback_system_base != DEFAULT_FALLBACK_SYSTEM_BASE:
        got.fallback_system_base = case.fallback_system_base
    if case.ip != DEFAULT_IP:
        got.ip = case.ip
    if case.admin_port != DEFAULT_ADMIN_PORT:
        got.admin_port = case.admin_port
    if case.coordinator_ip != DEFAULT_COORDINATOR_IP:
        got.coordinator_ip = case.coordinator_ip
    if case.coordinator_port != DEFAULT_COORDINATOR_PORT:
        got.coordinator_port = case.coordinator_port
    assert isinstance(got, ActorConfig)
    assert got.name == case.want_name
    assert got.system_base in get_args(SystemBase)
    assert got.system_base == case.system_base
    assert got.fallback_system_base in (get_args(SystemBase) + (None,))
    assert got.fallback_system_base == case.fallback_system_base
    assert got.ip == case.ip
    assert got.admin_port == case.admin_port
    assert got.coordinator_ip == case.coordinator_ip
    assert got.coordinator_port == case.coordinator_port
