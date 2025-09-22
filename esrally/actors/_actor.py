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
import dataclasses
import inspect
import logging
from collections.abc import Awaitable
from typing import Any

from thespian import actors  # type: ignore[import-untyped]

from esrally import types
from esrally.actors._config import ActorConfig
from esrally.actors._context import (
    ActorContext,
    enter_context,
    get_context,
    set_context,
)
from esrally.actors._proto import (
    PoisonError,
    RequestMessage,
    ResponseMessage,
    response_from_status,
)
from esrally.actors._system import get_system
from esrally.config import init_config

LOG = logging.getLogger(__name__)


def get_actor() -> AsyncActor:
    return get_request_context().actor


def get_request_context() -> ActorRequestContext:
    ctx = get_context()
    if not isinstance(ctx, ActorRequestContext):
        raise TypeError(f"Context is not a RequestContext: {ctx!r}")
    return ctx


def respond(*, status: Any = None, error: Exception | None = None) -> None:
    get_request_context().respond(status, error)


@dataclasses.dataclass
class ActorRequestContext(ActorContext):
    actor: AsyncActor
    sender: actors.ActorAddress | None = None
    req_id: str = ""
    responded: bool = False

    def create(
        self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None
    ) -> actors.ActorAddress:
        address = self.actor.createActor(cls, requirements)
        if hasattr(cls, "receiveMsg_ActorConfig"):
            self.send(address, ActorConfig.from_config(cfg))
        return address

    def send(self, destination: actors.ActorAddress, message: Any) -> actors.ActorAddress:
        return self.actor.send(destination, message)

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        return self.actor.request(destination, message)

    def shutdown(self) -> None:
        self.actor.send(self.actor.myAddress, actors.ActorExitRequest())

    def respond(self, status: Any = None, error: Exception | None = None) -> None:
        if error is None and inspect.isawaitable(status):
            f = self.actor.add_future(status)
            if not f.done():
                f.add_done_callback(self.respond)  # Call me back when you are done.
                return
            try:
                status = f.result()
            except Exception as e:
                error = e

        if self.req_id:
            response = response_from_status(self.req_id, status, error)
        elif error is not None:
            raise error  # The response will eventually reach requester in the form of a PoisonMessage
        else:
            response = status

        if self.responded:
            if status is not None:
                LOG.warning("Ignored request status: %r", status)
            return
        if response is not None:
            self.send(self.sender, response)
            self.responded = True


class AsyncActor(actors.ActorTypeDispatcher):

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> tuple[actors.ActorAddress, Any]:
        cfg = ActorConfig.from_config(cfg)
        system = get_system()
        address = system.createActor(cls)
        config_res = system.ask(address, cfg)
        return address, config_res

    def __init__(self) -> None:
        super().__init__()
        self._log: logging.Logger | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending_tasks: set[asyncio.Future] = set()
        self._request_futures: dict[str, asyncio.Future] = {}
        self._ctx = contextvars.copy_context()

    @property
    def log(self) -> logging.Logger:
        if self._log is None:
            self._log = self.logger(f"{type(self).__module__}.{type(self).__name__}")
        return self._log

    def receiveMessage(self, message: Any, sender: actors.ActorAddress) -> None:
        """It makes sure the message is handled with the actor context variables."""
        original_loop = asyncio.get_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._ctx.run(self._receive_message, message, sender)
        finally:
            asyncio.set_event_loop(original_loop)

    def _receive_message(self, message: Any, sender: actors.ActorAddress) -> None:
        """It makes sure the message is handled with the actor context variables."""
        with enter_context(ActorRequestContext(actor=self, sender=sender)) as ctx:
            try:
                ctx.respond(status=super().receiveMessage(message, sender))
            except Exception as error:
                ctx.respond(error=error)

    def receiveMsg_ActorConfig(self, cfg: ActorConfig, sender: actors.ActorAddress) -> None:
        """It adds the configuration to the actor context."""
        self.log.debug("Received configuration message: %s", cfg)
        init_config(cfg, force=True)

    def receiveMsg_ActorExitRequest(self, message: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        if self._pending_tasks:
            for t in self._pending_tasks:
                t.cancel()
            self._pending_tasks.clear()
        if self._loop is not None:
            self._loop.stop()
            self._loop = None
        set_context(None)

    def receiveMsg_WakeupMessage(self, message: actors.WakeupMessage, sender: actors.ActorAddress) -> Any:
        """It executes pending tasks on a scheduled time period."""
        if message.payload is self._RUN_PENDING_TASKS:
            self._run_pending_tasks()
            return None
        return self.SUPER

    _RUN_PENDING_TASKS = object()

    def _run_pending_tasks(self) -> None:
        self.loop.run_until_complete(nop())
        self._pending_tasks = {t for t in self._pending_tasks if not t.done()}
        self.wakeupAfter(0.001, self._RUN_PENDING_TASKS)

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        request = RequestMessage.from_message(message, timeout=timeout)
        future = self._request_futures.setdefault(request.req_id, asyncio.get_event_loop().create_future())
        self.send(destination, request)
        return future

    def receiveMsg_ResponseMessage(self, response: ResponseMessage, sender: actors.ActorAddress) -> None:
        future = self._request_futures.pop(response.req_id, None)
        if not future or future.done():
            LOG.debug("Ignore request response: %s", response)
            return
        try:
            future.set_result(response.result())
        except Exception as error:
            future.set_exception(error)

    def receiveMsg_PoisonMessage(self, message: actors.PoisonMessage, sender: actors.ActorAddress) -> None:
        if not isinstance(message.poisonMessage, RequestMessage):
            return self.SUPER
        future = self._request_futures.pop(message.poisonMessage.req_id)
        if not future or future.done():
            return self.SUPER
        future.set_exception(PoisonError(f"failing handling message: {message.poisonMessage!r}\n{message.details}"))

    def receiveMsg_RequestMessage(self, request: RequestMessage, sender: actors.ActorAddress) -> Any:
        get_request_context().req_id = request.req_id
        return super().receiveMessage(request.message, sender)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        loop = self._loop
        if loop is None:
            self._loop = loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._run_pending_tasks()
        return loop

    def create_future(self) -> asyncio.Future:
        return self.add_future(self.loop.create_future())

    def create_task(self, coro: Any, *, name: str | None = None) -> asyncio.Future:
        return self.add_future(self.loop.create_task(coro, name=name))

    def add_future(self, coro_or_future: Awaitable) -> asyncio.Future:
        future = asyncio.ensure_future(coro_or_future, loop=self.loop)
        self._pending_tasks.add(future)
        return future


async def nop() -> None: ...
