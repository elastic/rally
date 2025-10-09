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
import sys
from typing import Literal, cast, get_args

from esrally import config
from esrally.utils import convert

# SystemBase is the type of actor system to be created in the application.
SystemBase = Literal["multiprocQueueBase", "multiprocTCPBase"]

# ProcessStartupMethod values are used to specify the way actor processes have to be created.
ProcessStartupMethod = Literal[
    "fork",  # A call to fork function is called to create a new actor process.
    "spawn",  # A process is executed from scratch to create a new actor.
]

DEFAULT_SYSTEM_BASE: SystemBase | None = None
DEFAULT_FALLBACK_SYSTEM_BASE: SystemBase | None = None

DEFAULT_IP: str = "127.0.0.1"
DEFAULT_ADMIN_PORTS: range | None = None
DEFAULT_COORDINATOR_IP: str | None = None
DEFAULT_PROCESS_STARTUP_METHOD: ProcessStartupMethod | None = None
DEFAULT_LOOP_INTERVAL: float = 0.01


class ActorConfig(config.Config):
    """Configuration class defining properties to read and set '[actors'] section."""

    @property
    def system_base(self) -> SystemBase:
        """The actor system base used to initialize Thespian actor system"""
        value: str | None = self.opts("actors", "actors.system_base", default_value=DEFAULT_SYSTEM_BASE, mandatory=False)
        if isinstance(value, str):
            value = value.strip()
            if value:
                if value in get_args(SystemBase):
                    return cast(SystemBase, value)
                raise ValueError(f"Invalid value for 'actors.system_base': '{value}', it must be one of {get_args(SystemBase)}")
        return "multiprocTCPBase"

    @system_base.setter
    def system_base(self, value: SystemBase) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.system_base", value)

    @property
    def fallback_system_base(self) -> SystemBase:
        """The alternative system base used to initialize Thespian actor system.

        This value is intended to be used in case it fails initializing with `system_base` option value.
        """
        value = self.opts("actors", "actors.fallback_system_base", default_value=DEFAULT_FALLBACK_SYSTEM_BASE, mandatory=False)
        if isinstance(value, str):
            value = value.strip()
            if value:
                if value in get_args(SystemBase):
                    return cast(SystemBase, value)
                raise ValueError(f"Invalid value for 'actors.fallback_system_base': '{value}', it must be one of {get_args(SystemBase)}")
        if self.system_base == "multiprocQueueBase":
            return "multiprocTCPBase"
        return "multiprocQueueBase"

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
    def admin_ports(self) -> range | None:
        """The range of ports where to try opening one for the Thespian administrator service (only for multiprocTCPBase system base)."""
        value = self.opts("actors", "actors.admin_ports", default_value=DEFAULT_ADMIN_PORTS, mandatory=False)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return convert.to_port_range(value)
        if value:
            if isinstance(value, range):
                return value
            raise ValueError(f"Invalid value for 'actors.admin_ports' option: {value}")
        return None

    @admin_ports.setter
    def admin_ports(self, value: range | None) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.admin_ports", value)

    @property
    def coordinator_ip(self) -> str | None:
        """The IP address of the host where rally coordinator actors are running.

        (only for multiprocTCPBase system base in a multi host configuration)."""
        value = self.opts("actors", "actors.coordinator_ip", default_value=DEFAULT_COORDINATOR_IP, mandatory=False)
        if isinstance(value, str):
            value = value.strip()
            if value:
                return value
        return None

    @coordinator_ip.setter
    def coordinator_ip(self, value: str | None) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.coordinator_ip", value)

    @property
    def process_startup_method(self) -> ProcessStartupMethod | None:
        """The method used to starts actor sub-processes in Rally. By default, 'fork' is being used (which is the fastest).

        Others methods are being provided to overcome potential race conditions with the use of 'fork' in presence of threads.
        """
        value = self.opts("actors", "actors.process_startup_method", default_value=DEFAULT_PROCESS_STARTUP_METHOD, mandatory=False)
        if isinstance(value, str):
            value = value.strip()
            if value:
                if value in get_args(ProcessStartupMethod):
                    return cast(ProcessStartupMethod, value)
                raise ValueError(f"Invalid process startup method '{value}', must be one of {get_args(ProcessStartupMethod)}")
        if self.system_base == "multiprocQueueBase":
            # multiprocQueueBase is using threads so fork could create problems.
            return "spawn"

        if sys.platform == "darwin" and sys.version_info < (3, 12):
            # Old versions of Python on OSX have known problems with fork.
            return "spawn"

        # In general fork is expected to be the most performant tu be used.
        return "fork"

    @process_startup_method.setter
    def process_startup_method(self, value: ProcessStartupMethod | None) -> None:
        self.add(config.Scope.applicationOverride, "actors", "actors.process_startup_method", value)

    @property
    def loop_interval(self) -> float:
        """It specifies a maximum interval of time used to pool for response to an external request."""
        value = self.opts("actors", "actors.loop_interval", DEFAULT_LOOP_INTERVAL, False)
        if isinstance(value, str):
            value = value.strip()
        if value:
            return float(value)
        return DEFAULT_LOOP_INTERVAL

    @loop_interval.setter
    def loop_interval(self, value: float | None) -> None:
        if value is None:
            value = DEFAULT_LOOP_INTERVAL
        self.add(config.Scope.applicationOverride, "actors", "actors.loop_interval", value)
