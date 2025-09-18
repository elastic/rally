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

from typing import Literal

from esrally import config

SystemBase = Literal["multiprocQueueBase", "multiprocTCPBase", "multiprocUDPBase"]

DEFAULT_SYSTEM_BASE: SystemBase = "multiprocTCPBase"
DEFAULT_FALLBACK_SYSTEM_BASE: SystemBase = "multiprocQueueBase"

DEFAULT_IP: str = "127.0.0.1"
DEFAULT_ADMIN_PORT: int = 0
DEFAULT_COORDINATOR_IP: str = ""
DEFAULT_COORDINATOR_PORT: int = 0
DEFAULT_ROUTER_ADDRESS: str | None = None

ProcessStartupMethod = Literal[
    None,
    "fork",
    "spawn",
    "forkserver",
]

DEFAULT_PROCESS_STARTUP_METHOD: ProcessStartupMethod = None


class ActorConfig(config.Config):

    @property
    def system_base(self) -> SystemBase:
        return self.opts("actors", "actors.system_base", default_value=DEFAULT_SYSTEM_BASE, mandatory=False)

    @system_base.setter
    def system_base(self, value: SystemBase) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.system_base", value)

    @property
    def fallback_system_base(self) -> SystemBase:
        return self.opts("actors", "actors.fallback_system_base", default_value=DEFAULT_FALLBACK_SYSTEM_BASE, mandatory=False)

    @fallback_system_base.setter
    def fallback_system_base(self, value: SystemBase) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.fallback_system_base", value)

    @property
    def ip(self) -> str:
        return self.opts("actors", "actors.ip", default_value=DEFAULT_IP, mandatory=False).strip()

    @ip.setter
    def ip(self, value: str) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.ip", value.strip())

    @property
    def admin_port(self) -> int:
        return int(self.opts("actors", "actors.admin_port", default_value=DEFAULT_ADMIN_PORT, mandatory=False))

    @admin_port.setter
    def admin_port(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.admin_port", int(value))

    @property
    def coordinator_ip(self) -> str:
        return self.opts("actors", "actors.coordinator_ip", default_value=DEFAULT_COORDINATOR_IP, mandatory=False).strip()

    @coordinator_ip.setter
    def coordinator_ip(self, value: str) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.coordinator_ip", value.strip())

    @property
    def coordinator_port(self) -> int:
        return int(self.opts("actors", "actors.coordinator_port", default_value=DEFAULT_COORDINATOR_PORT, mandatory=False))

    @coordinator_port.setter
    def coordinator_port(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.coordinator_port", int(value))

    @property
    def process_startup_method(self) -> ProcessStartupMethod:
        return self.opts("actors", "actors.process_startup_method", default_value=DEFAULT_PROCESS_STARTUP_METHOD, mandatory=False)

    @process_startup_method.setter
    def process_startup_method(self, value: ProcessStartupMethod) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.process_startup_method", value)
