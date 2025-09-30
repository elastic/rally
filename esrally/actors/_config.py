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
from typing import Literal

from esrally import config
from esrally.utils import convert

# SystemBase is the type of actor system to be created in the application.
SystemBase = Literal["multiprocQueueBase", "multiprocTCPBase"]

DEFAULT_SYSTEM_BASE: SystemBase = "multiprocTCPBase"
DEFAULT_FALLBACK_SYSTEM_BASE: SystemBase = "multiprocQueueBase"

DEFAULT_IP: str = "127.0.0.1"
DEFAULT_ADMIN_PORTS: range = range(1900, 2000)
DEFAULT_COORDINATOR_IP: str = ""
DEFAULT_COORDINATOR_PORT: int = 0
DEFAULT_ROUTER_ADDRESS: str | None = None
DEFAULT_TRY_JOIN: bool = True

# ProcessStartupMethod values are used to specify the way actor processes have to be created.
ProcessStartupMethod = Literal[
    None,
    "fork",  # A call to fork function is called to create a new actor process.
    "spawn",  # A process is executed from scratch to create a new actor.
    "forkserver",  # It uses a server process to fork new actor processes.
]

DEFAULT_PROCESS_STARTUP_METHOD: ProcessStartupMethod = None


class ActorConfig(config.Config):
    """Configuration class defining properties to read and set '[actors'] section."""

    @property
    def system_base(self) -> SystemBase:
        """The actor system base used to initialize Thespian actor system"""
        return self.opts("actors", "actors.system_base", default_value=DEFAULT_SYSTEM_BASE, mandatory=False)

    @system_base.setter
    def system_base(self, value: SystemBase) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.system_base", value)

    @property
    def fallback_system_base(self) -> SystemBase:
        """The alternative system base used to initialize Thespian actor system.

        This value is intended to be used in case it fails initializing with `system_base` option value.
        """
        return self.opts("actors", "actors.fallback_system_base", default_value=DEFAULT_FALLBACK_SYSTEM_BASE, mandatory=False)

    @fallback_system_base.setter
    def fallback_system_base(self, value: SystemBase) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.fallback_system_base", value)

    @property
    def ip(self) -> str:
        """The local host IP used to open the Thespian administrator service (only for multiprocTCPBase system base)."""
        return self.opts("actors", "actors.ip", default_value=DEFAULT_IP, mandatory=False).strip()

    @ip.setter
    def ip(self, value: str) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.ip", value.strip())

    @property
    def admin_ports(self) -> range:
        """The range of ports where to try opening one for the Thespian administrator service (only for multiprocTCPBase system base)."""
        return convert.to_port_range(self.opts("actors", "actors.admin_ports", default_value=DEFAULT_ADMIN_PORTS, mandatory=False))

    @admin_ports.setter
    def admin_ports(self, value: int | str | range) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.admin_ports", convert.to_port_range(value))

    @property
    def coordinator_ip(self) -> str:
        """The IP address of the host where rally coordinator actors are running.

        (only for multiprocTCPBase system base in a multi host configuration)."""
        return self.opts("actors", "actors.coordinator_ip", default_value=DEFAULT_COORDINATOR_IP, mandatory=False).strip()

    @coordinator_ip.setter
    def coordinator_ip(self, value: str) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.coordinator_ip", value.strip())

    @property
    def coordinator_port(self) -> int:
        """The port of the thespian actor confederation used to join a remote actor system on the rally coordinator host."""
        return int(self.opts("actors", "actors.coordinator_port", default_value=DEFAULT_COORDINATOR_PORT, mandatory=False))

    @coordinator_port.setter
    def coordinator_port(self, value: int) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.coordinator_port", int(value))

    @property
    def process_startup_method(self) -> ProcessStartupMethod:
        """The method used to starts actor sub-processes in Rally. By default, 'fork' is being used (which is the fastest).

        Others methods are being provided to overcome potential race conditions with the use of 'fork' in presence of threads.
        """
        return self.opts("actors", "actors.process_startup_method", default_value=DEFAULT_PROCESS_STARTUP_METHOD, mandatory=False)

    @process_startup_method.setter
    def process_startup_method(self, value: ProcessStartupMethod) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.process_startup_method", value)

    @property
    def try_join(self) -> bool:
        """It indicates if it should try joining an existing actor system already running on the current host.

        When running with 'multiprocTCPBase' system base (the default) it will try to connect to the smaller
        admin port in given range first, to look if there is an existing actor system running on it and join it.

        On the contrary it will pick random unused ports to avoid joining existing actor systems. In the case of running
        tests with short living actor systems this setting this to False should increase actor system initialization
        performance.
        """
        return convert.to_bool(self.opts("actors", "actors.try_join", default_value=DEFAULT_TRY_JOIN, mandatory=False))

    @try_join.setter
    def try_join(self, value: bool) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.try_join", bool(value))
