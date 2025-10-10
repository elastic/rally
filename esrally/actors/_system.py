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
import itertools
import logging
import os
import socket
from collections.abc import Iterable
from typing import Any, get_args

from thespian import actors  # type: ignore[import-untyped]
from thespian.system.logdirector import (  # type: ignore[import-untyped]
    ThespianLogForwarder,
)

from esrally import log, types
from esrally.actors._config import ActorConfig, SystemBase
from esrally.actors._context import (
    ActorContext,
    ActorContextError,
    get_actor_context,
    set_actor_context,
)
from esrally.utils import net

LOG = logging.getLogger(__name__)


def get_actor_system() -> actors.ActorSystem:
    """It returns the last actor system initialized using init_actor_system()"""
    return get_actor_context().actor_system


def init_actor_system(cfg: types.Config | None = None) -> actors.ActorSystem:
    """It initializes the actor system using given configuration.

    To provide a custom configuration create and customize one with ActorConfig class. Example:

        cfg = ActorConfig.from_config()
        cfg.system_base = "multiprocTCPBase"
        cfg.admin_ports = "1900"
        system = actors.init_actor_system(cfg)

    After the actor system is initialized, the actor system reference will be available using get_actor_system()
    function. For creating actors and sending messages, you can then use `create_actor`, `send` and `request` global
    functions.

        address = actors.create_actor(MyActorClass)
        response = await actors.request(address, MyRequest())

    To finally shut down the actor system, use the `shutdown` function.

        actors.shutdown()

    :param cfg: Optional configuration object.
    :return: An initialized actor system
    """
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

    # if sys.platform == "darwin" and sys.version_info < (3, 12):
    #     selectors.DefaultSelector = selectors.SelectSelector

    set_actor_context(ctx)
    LOG.info("Actor system initialized.")
    return ctx.handler


def context_from_config(cfg: types.Config | None = None) -> ActorContext:
    """Creates a new actor context, with its actor system, from given configuration.

    :param cfg: configuration object to be used.
    :return: Created actor context.
    """
    cfg = ActorConfig.from_config(cfg)

    # It will try configured system base first, fall back one later, if different.
    system_bases = [cfg.system_base]
    if cfg.fallback_system_base not in system_bases:
        system_bases.append(cfg.fallback_system_base)

    first_error: Exception | None = None
    for system_base in system_bases:

        admin_ports: Iterable[int | None] = [None]
        if system_base == "multiprocTCPBase" and cfg.admin_ports:
            # It will try using provided ports first, then a random one as fallback in case of issues.
            admin_ports = itertools.chain(cfg.admin_ports, admin_ports)

        for admin_port in admin_ports:
            try:
                system = create_system(
                    system_base=system_base,
                    ip=cfg.ip,
                    admin_port=admin_port,
                    coordinator_ip=cfg.coordinator_ip,
                    process_startup_method=cfg.process_startup_method,
                )
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

            # It succeeded crating an actor system. The configuration passed to the context will be forwarded to all
            # actors created within this context.
            return ActorContext(handler=system, cfg=cfg)

    raise first_error or RuntimeError(f"Could not initialize actor system with system base '{cfg.system_base}'")


def create_system(
    system_base: SystemBase | None = None,
    ip: str | None = None,
    admin_port: int | None = None,
    coordinator_ip: str | None = None,
    process_startup_method: str | None = None,
) -> actors.ActorSystem:
    """It creates a new actor system using given configuration.

    :param system_base:
    :param ip:
    :param admin_port:
    :param coordinator_ip:
    :param process_startup_method:
    :return: the new actor system from Thespian.
    """
    if system_base and system_base not in get_args(SystemBase):
        raise ValueError(f"invalid system base value: '{system_base}', valid options are: {get_args(SystemBase)}")

    capabilities: dict[str, Any] = {"coordinator": True}
    if system_base == "multiprocTCPBase":
        if ip:
            capabilities["ip"] = ip = resolve(ip)
            if admin_port is None:
                admin_port = find_unused_random_port(ip)

        if admin_port:
            capabilities["Admin Port"] = admin_port

        if coordinator_ip:
            coordinator_ip = resolve(coordinator_ip)
            capabilities["Convention Address.IPv4"] = coordinator_ip
            if ip and coordinator_ip != ip:
                capabilities["coordinator"] = False

        if process_startup_method:
            capabilities["Process Startup Method"] = process_startup_method

    if system_base == "multiprocQueueBase":
        if process_startup_method:
            capabilities["Process Startup Method"] = process_startup_method

    log_defs = None
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


def resolve(hostname_or_ip: str) -> str:
    resolved = net.resolve(hostname_or_ip)
    if not resolved:
        raise ValueError(f"Invalid hostname or ip address: '{hostname_or_ip}'")
    return resolved


def find_unused_random_port(ip: str) -> int:
    with socket.socket() as sock:
        try:
            sock.bind((ip, 0))
        except OSError:
            sock.bind(("0.0.0.0", 0))
        _, port = sock.getsockname()
    return port
