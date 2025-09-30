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

import logging
import os
import socket
from collections.abc import Generator, Iterable
from typing import Any, get_args

from thespian import actors  # type: ignore[import-untyped]
from thespian.system.logdirector import (  # type: ignore[import-untyped]
    ThespianLogForwarder,
)

from esrally import log, types
from esrally.actors._config import ActorConfig, ProcessStartupMethod, SystemBase
from esrally.actors._context import (
    ActorContext,
    ActorContextError,
    get_actor_context,
    set_actor_context,
)
from esrally.utils import net

LOG = logging.getLogger(__name__)


def get_actor_system() -> actors.ActorSystem:
    system = get_actor_context().handler
    if not isinstance(system, actors.ActorSystem):
        raise ActorContextError("Context handler is not an ActorSystem")
    return system


def init_actor_system(cfg: types.Config | None = None) -> actors.ActorSystem:
    try:
        system = get_actor_system()
    except ActorContextError:
        pass
    else:
        LOG.warning("ActorSystem already initialized.")
        return system

    LOG.info("Initializing actor system...")
    ctx = context_from_config(cfg)
    if not isinstance(ctx.handler, actors.ActorSystem):
        raise ActorContextError("Context handler is not an ActorSystem")
    set_actor_context(ctx)
    LOG.info("Actor system initialized.")
    return ctx.handler


def context_from_config(cfg: types.Config | None = None) -> ActorContext:
    cfg = ActorConfig.from_config(cfg)
    system_bases = [cfg.system_base]
    if cfg.fallback_system_base and cfg.fallback_system_base != cfg.system_base:
        system_bases.append(cfg.fallback_system_base)

    first_error: Exception | None = None
    for system_base in system_bases:
        admin_ports: Iterable[int | None]
        if system_base == "multiprocTCPBase":
            if cfg.try_join:
                admin_ports = cfg.admin_ports
            else:
                admin_ports = iter_unused_random_ports()
        else:
            admin_ports = [None]

        for admin_port in admin_ports:
            try:
                system = create_system(
                    system_base=system_base,
                    ip=cfg.ip,
                    admin_port=admin_port,
                    coordinator_ip=cfg.coordinator_ip,
                    coordinator_port=cfg.coordinator_port,
                    process_startup_method=cfg.process_startup_method,
                )
                return ActorContext(handler=system)
            except actors.InvalidActorAddress as ex:
                first_error = first_error or ex
                if admin_port is not None:
                    LOG.exception("Failed setting up actor system with system base '%s' and admin port %s", system_base, admin_port)
                    continue  # It tries the next port
                break  # It tries the next system base
            except Exception as ex:
                LOG.exception("Failed setting up actor system with system base '%s'", system_base)
                first_error = first_error or ex
                break  # It tries the next system base

    raise first_error or RuntimeError(f"Could not initialize actor system with system base '{cfg.system_base}'")


def create_system(
    system_base: SystemBase | None = None,
    ip: str | None = None,
    admin_port: int | None = None,
    coordinator_ip: str | None = None,
    coordinator_port: int | None = None,
    process_startup_method: str | None = None,
) -> actors.ActorSystem:
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

        if process_startup_method not in get_args(ProcessStartupMethod):
            raise ValueError(
                f"invalid process startup method value: '{process_startup_method}', valid options are: " f"{get_args(ProcessStartupMethod)}"
            )
        capabilities["Process Startup Method"] = process_startup_method

    if system_base == "multiprocQueueBase":
        capabilities["Process Startup Method"] = "spawn"

    log_defs = False
    if not isinstance(logging.root, ThespianLogForwarder):
        conf_path = log.log_config_path()
        if os.path.isfile(conf_path):
            log_defs = log.load_configuration()
        else:
            LOG.warning("File not found: %s", conf_path)
    LOG.debug(
        "Creating actor system:\n - systemBase: %r\n - capabilities: %r\n - logDefs: %r\n",
        system_base,
        capabilities,
        log_defs,
    )
    system = actors.ActorSystem(systemBase=system_base, capabilities=capabilities, logDefs=log_defs, transientUnique=True)
    LOG.debug("Actor system created:\n - capabilities: %r\n", system.capabilities)
    return system


def iter_unused_random_ports() -> Generator[int]:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            _, port = sock.getsockname()
        yield port
