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
    Context,
    ContextError,
    enter_context,
    get_context,
    send,
    set_context,
)
from esrally.actors._proto import PoisonError, RequestMessage, ResponseMessage
from esrally.actors._system import get_system
from esrally.config import init_config

LOG = logging.getLogger(__name__)


def get_actor() -> AsyncActor:
    return get_actor_context().actor


def get_actor_context() -> ActorContext:
    ctx = get_context()
    if not isinstance(ctx, ActorContext):
        raise TypeError(f"Context is not an ActorContext: {ctx!r}")
    return ctx


@dataclasses.dataclass
class ActorContext(Context):
    actor: AsyncActor
    future: asyncio.Future | None = None
    request_sender: actors.ActorAddress | None = None
    request_id: str = ""

    def create(self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None) -> actors.ActorAddress:
        return self.actor.createActor(cls, requirements)

    def send(self, destination: actors.ActorAddress, message: Any) -> actors.ActorAddress:
        return self.actor.send(destination, message)

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        return self.actor.request(destination, message)

    def shutdown(self) -> None:
        self.actor.send(self.actor.myAddress, actors.ActorExitRequest())


def get_future() -> asyncio.Future:
    ctx = get_actor_context()
    if ctx.future is None:
        ctx.future = ctx.actor.create_future()
        ctx.future.add_done_callback(send_result_callback)
    return ctx.future


def send_result_callback(future: asyncio.Future[Any]) -> None:
    ctx = get_actor_context()
    send(ctx.request_sender, ResponseMessage.from_future(ctx.request_id, future))


def send_result(result: Any) -> None:
    try:
        ctx = get_actor_context()
    except ContextError:
        if result is None:
            return
        raise

    if ctx.future and ctx.future.done():
        if result is not None:
            LOG.warning("Ignore actor result: %r.", result)
        return
    if inspect.isawaitable(result):
        ctx.future = get_actor().add_future(result)
        ctx.future.add_done_callback(send_result_callback)
        return
    get_future().set_result(result)


def send_error(error: Exception) -> None:
    ctx = get_actor_context()
    if ctx.future and ctx.future.done():
        LOG.exception("Ignore actor error: %s.", exc_info=error)
        return
    get_future().set_exception(error)


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
            self._ctx.copy().run(self._receive_message, message, sender)
        finally:
            asyncio.set_event_loop(original_loop)

    def _receive_message(self, message: Any, sender: actors.ActorAddress) -> None:
        """It makes sure the message is handled with the actor context variables."""
        with enter_context(ActorContext(self)):
            try:
                send_result(super().receiveMessage(message, sender))
            except Exception as error:
                send_error(error)

    def receiveMsg_ActorConfig(self, cfg: ActorConfig, sender: actors.ActorAddress) -> None:
        """It adds the configuration to the actor context."""
        self.log.debug("Received configuration message: %s", cfg)
        self._ctx.run(init_config, cfg, force=True)

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
        return None

    def receiveMsg_RequestMessage(self, request: RequestMessage, sender: actors.ActorAddress) -> Any:
        ctx = get_actor_context()
        ctx.request_id = request.req_id
        ctx.request_sender = sender
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
