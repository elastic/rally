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
import contextvars
import logging
import socket
import time
from typing import Any, get_args

from thespian import actors
from thespian.system.logdirector import ThespianLogForwarder
from typing_extensions import Self

from esrally import log, types
from esrally.actors._config import ActorConfig, ProcessStartupMethod, SystemBase
from esrally.utils import net

LOG = logging.getLogger(__name__)


class ActorSystem(actors.ActorSystem):

    @classmethod
    def from_config(cls, cfg: types.Config) -> Self:
        cfg = ActorConfig.from_config(cfg)
        first_error: Exception | None = None
        system_bases = [cfg.system_base]
        if cfg.fallback_system_base and cfg.fallback_system_base != cfg.system_base:
            system_bases.append(cfg.fallback_system_base)

        for sb in system_bases:
            try:
                return cls.create(
                    system_base=sb,
                    ip=cfg.ip,
                    admin_port=cfg.admin_port,
                    coordinator_ip=cfg.coordinator_ip,
                    coordinator_port=cfg.coordinator_port,
                    process_startup_method=cfg.process_startup_method,
                )
            except Exception as ex:
                LOG.exception("Failed setting up actor system with system base '%s'", sb)
                first_error = first_error or ex
        raise first_error or Exception(f"Could not initialize actor system with system base '{cfg.system_base}'")

    @classmethod
    def create(
        cls,
        system_base: SystemBase | None = None,
        ip: str | None = None,
        admin_port: int | None = None,
        coordinator_ip: str | None = None,
        coordinator_port: int | None = None,
        process_startup_method: str | None = None,
    ) -> Self:
        if system_base and system_base not in get_args(SystemBase):
            raise ValueError(f"invalid system base value: '{system_base}', valid options are: {get_args(SystemBase)}")

        capabilities: dict[str, Any] = {"coordinator": True}
        if system_base in ("multiprocTCPBase", "multiprocUDPBase"):
            proto = {"multiprocTCPBase": socket.IPPROTO_TCP, "multiprocUDPBase": socket.IPPROTO_UDP}[system_base]
            if ip:
                ip, admin_port = net.resolve(ip, port=admin_port, proto=proto)
                capabilities["ip"] = ip

            if admin_port:
                capabilities["Admin Port"] = admin_port

            if coordinator_ip:
                coordinator_ip, coordinator_port = net.resolve(coordinator_ip, coordinator_port)
                if coordinator_port:
                    coordinator_port = int(coordinator_port)
                    if coordinator_port:
                        coordinator_ip += f":{coordinator_port}"
                capabilities["Convention Address.IPv4"] = coordinator_ip
                if ip and coordinator_ip != ip:
                    capabilities["coordinator"] = False

        if system_base != "simpleSystemBase":
            if process_startup_method:
                if process_startup_method not in get_args(ProcessStartupMethod):
                    raise ValueError(
                        f"invalid process startup method value: '{process_startup_method}', valid options are: "
                        f"{get_args(ProcessStartupMethod)}"
                    )
                capabilities["Process Startup Method"] = process_startup_method

        log_defs = False
        if not isinstance(logging.root, ThespianLogForwarder):
            log_defs = log.load_configuration()
        LOG.debug(
            "Creating actor system:\n - systemBase: %r\n - capabilities: %r\n - logDefs: %r\n",
            system_base,
            capabilities,
            log_defs,
        )
        s = cls(systemBase=system_base, capabilities=capabilities, logDefs=log_defs, transientUnique=True)
        LOG.debug("Actor system created:\n - capabilities: %r\n", s.capabilities)
        return s


_CURRENT_SYSTEM = contextvars.ContextVar[actors.ActorSystem | None]("_CURRENT_SYSTEM", default=None)


def system() -> ActorSystem:
    s = _CURRENT_SYSTEM.get()
    if not s:
        raise RuntimeError("Actor system not initialized.") from None
    return s


def init_system(cfg: types.Config) -> ActorSystem:
    assert isinstance(cfg, types.Config)
    s = _CURRENT_SYSTEM.get()
    if s:
        raise RuntimeError("Actor system is already initialized.")

    LOG.warning("Initializing actor system...")
    _CURRENT_SYSTEM.set(s := ActorSystem.from_config(cfg))
    LOG.info("Actor system initialized.")
    return s


def shutdown_system() -> None:
    s = _CURRENT_SYSTEM.get()
    if s is None:
        return

    LOG.warning("Shutting down actor system...")
    _CURRENT_SYSTEM.set(None)
    s.shutdown()
    LOG.info("Actor system shut down.")


async def ask(actor_addr: actors.ActorAddress, msg: Any, timeout: float | None = None) -> Any:
    s = system()
    res = s.ask(actor_addr, msg)
    if res is not None:
        return res
    deadline = None
    if timeout is not None:
        deadline = time.monotonic() + timeout
    while deadline is None or deadline >= time.monotonic():
        res = s.listen(timeout=0.0)
        if res is not None:
            return res
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Request timed out: {msg}")
