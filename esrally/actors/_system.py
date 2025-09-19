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
import dataclasses
import logging
import socket
import uuid
from collections.abc import Iterable
from typing import Any, get_args

from thespian import actors  # type: ignore[import-untyped]
from thespian.system.logdirector import (  # type: ignore[import-untyped]
    ThespianLogForwarder,
)
from typing_extensions import Self

from esrally import log, types
from esrally.actors._config import ActorConfig, ProcessStartupMethod, SystemBase
from esrally.actors._context import (
    ActorContext,
    ActorContextError,
    get_context,
    set_context,
)
from esrally.actors._proto import PoisonError, RequestMessage, ResponseMessage
from esrally.utils import net

LOG = logging.getLogger(__name__)


def get_system() -> actors.ActorSystem:
    return get_system_context().system


def init_system(cfg: types.Config | None = None) -> actors.ActorSystem:
    try:
        ctx = get_system_context()
        LOG.warning("ActorSystem already initialized.")
        return ctx.system
    except ActorContextError:
        pass

    LOG.info("Initializing actor system...")
    ctx = ActorSystemContext.from_config(cfg)
    set_context(ctx)
    LOG.info("Actor system initialized.")
    return ctx.system


def get_system_context() -> ActorSystemContext:
    ctx = get_context()
    if not isinstance(ctx, ActorSystemContext):
        raise TypeError("Context is not an ActorSystemContext")
    return ctx


@dataclasses.dataclass
class ActorSystemContext(ActorContext):

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        cfg = ActorConfig.from_config(cfg)
        system_bases = [cfg.system_base]
        if cfg.fallback_system_base and cfg.fallback_system_base != cfg.system_base:
            system_bases.append(cfg.fallback_system_base)

        first_error: Exception | None = None
        for system_base in system_bases:
            admin_ports: Iterable[int | None]
            if system_base in ["multiprocTCPBase", "multiprocUDPBase"]:
                admin_ports = cfg.admin_ports
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
                    return cls(system)
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

    system: actors.ActorSystem
    results: dict[str, asyncio.Future[Any]] = dataclasses.field(default_factory=dict)

    def create(
        self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None
    ) -> actors.ActorAddress:
        address = self.system.createActor(cls, requirements)
        if hasattr(cls, "receiveMsg_ActorConfig"):
            self.send(address, ActorConfig.from_config(cfg))
        return address

    def send(self, destination: actors.ActorAddress, message: Any) -> None:
        self.system.tell(destination, message)

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        req_id = ""
        while True:
            if req_id:
                future = self.results.get(req_id, None)
                if future is not None and future.done():
                    return future
            if message is None:
                got = self.system.listen(timeout)
            else:
                if not isinstance(message, RequestMessage):
                    message = RequestMessage(message)
                req_id = message.req_id
                if not req_id:
                    message.req_id = req_id = str(uuid.uuid4())
                if req_id not in self.results:
                    self.results[req_id] = asyncio.get_event_loop().create_future()
                got = self.system.ask(destination, message, timeout)
            if got is None:
                raise TimeoutError("No response from actor.")
            if isinstance(got, actors.PoisonMessage):
                error = PoisonError(got.details)
                if isinstance(got.poisonMessage, RequestMessage):
                    future = self.results.get(got.poisonMessage.req_id, None)
                    if future and not future.done():
                        future.set_exception(error)
                        continue
                    if req_id and req_id != got.poisonMessage.req_id:
                        LOG.warning("Ignoring poison message: %r\n%s", got.poisonMessage, got.details)
                        continue
                raise error
            if isinstance(got, ResponseMessage):
                if not got.req_id:
                    raise RuntimeError("No request ID in response message.")
                future = self.results.get(got.req_id, None)
                if future and not future.done():
                    try:
                        future.set_result(got.result())
                        continue
                    except Exception as error:
                        future.set_exception(error)
                        continue
                if req_id and req_id != got.req_id:
                    LOG.warning("Ignoring response message from actor: %r", got)
                    continue

    def shutdown(self):
        self.system.shutdown()


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
        try:
            log_defs = log.load_configuration()
        except FileNotFoundError:
            LOG.exception("Failed to load logging configuration.")
    LOG.debug(
        "Creating actor system:\n - systemBase: %r\n - capabilities: %r\n - logDefs: %r\n",
        system_base,
        capabilities,
        log_defs,
    )
    system = actors.ActorSystem(systemBase=system_base, capabilities=capabilities, logDefs=log_defs, transientUnique=True)
    LOG.debug("Actor system created:\n - capabilities: %r\n", system.capabilities)
    return system
