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
from typing import Any, get_args

from thespian import actors
from thespian.system.logdirector import ThespianLogForwarder
from typing_extensions import Self

from esrally import log, types
from esrally.actors._config import ActorConfig, ProcessStartupMethod, SystemBase
from esrally.actors._context import Context, ContextError, get_context, set_context
from esrally.actors._proto import PoisonError, RequestMessage, ResponseMessage
from esrally.utils import net

LOG = logging.getLogger(__name__)


def get_system() -> actors.ActorSystem:
    return get_system_context().system


def init_system(cfg: types.AnyConfig = None) -> actors.ActorSystem:
    try:
        ctx = get_system_context()
        LOG.warning("ActorSystem already initialized.")
        return ctx.system
    except ContextError:
        pass

    LOG.info("Initializing actor system...")
    ctx = SystemContext.from_config(cfg)
    set_context(ctx)
    LOG.info("Actor system initialized.")
    return ctx.system


def get_system_context() -> SystemContext:
    ctx = get_context()
    if not isinstance(ctx, SystemContext):
        raise TypeError("Context is not a SystemContext")
    return ctx


@dataclasses.dataclass
class SystemContext(Context):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = ActorConfig.from_config(cfg)
        first_error: Exception | None = None
        system_bases = [cfg.system_base]
        if cfg.fallback_system_base and cfg.fallback_system_base != cfg.system_base:
            system_bases.append(cfg.fallback_system_base)
        for sb in system_bases:
            try:
                system = create_system(
                    system_base=sb,
                    ip=cfg.ip,
                    admin_port=cfg.admin_port,
                    coordinator_ip=cfg.coordinator_ip,
                    coordinator_port=cfg.coordinator_port,
                    process_startup_method=cfg.process_startup_method,
                )
                return cls(system)
            except Exception as ex:
                LOG.exception("Failed setting up actor system with system base '%s'", sb)
                first_error = first_error or ex

        raise first_error or RuntimeError(f"Could not initialize actor system with system base '{cfg.system_base}'")

    system: actors.ActorSystem
    results: dict[str, asyncio.Future[Any]] = dataclasses.field(default_factory=dict)

    def create(self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None) -> actors.ActorAddress:
        return self.system.createActor(cls, requirements)

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

    if process_startup_method:
        if process_startup_method not in get_args(ProcessStartupMethod):
            raise ValueError(
                f"invalid process startup method value: '{process_startup_method}', valid options are: " f"{get_args(ProcessStartupMethod)}"
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
    system = actors.ActorSystem(systemBase=system_base, capabilities=capabilities, logDefs=log_defs, transientUnique=True)
    LOG.debug("Actor system created:\n - capabilities: %r\n", system.capabilities)
    return system
