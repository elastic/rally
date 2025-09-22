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
import time
from collections.abc import Generator, Iterable
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
from esrally.actors._proto import (
    CancelMessage,
    PoisonError,
    RequestMessage,
    ResponseMessage,
    RunningMessage,
)
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
        request = RequestMessage.from_message(message, timeout=timeout)
        future = self.results.get(request.req_id, None)
        if future is None:
            self.results[request.req_id] = future = asyncio.get_event_loop().create_future()
            original_cancel = future.cancel

            def cancel_wrapper(msg: Any | None = None) -> bool:
                self.send(destination, CancelMessage(request.req_id))
                return original_cancel(msg)

            future.cancel = cancel_wrapper  # type: ignore[method-assign]

        responses = self._request(destination, request)
        for response in responses:
            if isinstance(response, RunningMessage):

                async def _receive_response():
                    for response in responses:
                        LOG.debug("Received response: %s", response)

                asyncio.get_event_loop().create_task(_receive_response())
                break

        return future

    def _request(self, destination: actors.ActorAddress, request: RequestMessage) -> Generator[ResponseMessage]:
        deadline: float | None = request.deadline
        timeout: float | None = None
        if deadline is not None:
            timeout = deadline - time.monotonic()
        future = self.results.get(request.req_id, None)
        if future is None or future.done():
            return

        response = self.system.ask(destination, request, timeout=timeout)
        while response is not None:
            self._receive_response(response)
            if response.req_id == request.req_id:
                yield response
            if future.done():
                return
            if deadline is not None:
                timeout = deadline - time.monotonic()
                if timeout < 0.0:
                    future.set_result(TimeoutError("No response for actor."))
                    return
            response = self.system.listen(timeout=timeout)

    def _receive_response(self, response: Any) -> None:
        if isinstance(response, actors.PoisonMessage):
            error = PoisonError(response.details)
            if isinstance(response.poisonMessage, RequestMessage):
                future = self.results.get(response.poisonMessage.req_id, None)
                if future and not future.done():
                    future.set_exception(error)
                    return
            LOG.warning("Ignoring poison message: %r\n%s", response.poisonMessage, response.details)
            return
        if isinstance(response, ResponseMessage):
            future = self.results.get(response.req_id, None)
            if future and not future.done():
                try:
                    future.set_result(response.result())
                except Exception as error:
                    future.set_exception(error)
                return
            LOG.warning("Ignoring response message from actor: %r", response)
            return
        LOG.warning("Ignoring message from actor: %r", response)

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
