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
import contextlib
import contextvars
import dataclasses
import logging
from collections.abc import Coroutine, Generator
from typing import Any, Optional, TypeVar

from thespian import actors  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally import types
from esrally.actors._config import ActorConfig
from esrally.actors._proto import (
    CancelRequest,
    PingRequest,
    PoisonError,
    Request,
    Response,
    ResultResponse,
    RunningTaskResponse,
)

LOG = logging.getLogger(__name__)


CONTEXT = contextvars.ContextVar[Optional["ActorContext"]]("actors.context", default=None)


def create_actor(
    cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None
) -> actors.ActorAddress:
    return get_actor_context().create_actor(cls=cls, requirements=requirements, cfg=cfg)


R = TypeVar("R")


def create_task(coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
    """It creates a task and registers it for cancellation with the current request.

    :param coro: the task coroutine
    :param name: the task name
    :param context: the variables context of the task
    :return: created task.
    """
    return get_actor_context().create_task(coro, name=name)


def send(destination: actors.ActorAddress, message: Any) -> None:
    get_actor_context().send(destination, message)


def request(destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
    return get_actor_context().request(destination, message, timeout=timeout)


def ping(destination: actors.ActorAddress, *, message: Any = None, timeout: float | None = None) -> asyncio.Future[Any]:
    return request(destination, PingRequest(message=message), timeout=timeout)


def shutdown() -> None:
    try:
        ctx = get_actor_context()
    except ActorContextError:
        return
    set_actor_context(None)
    ctx.shutdown()


class ActorContextError(RuntimeError):
    pass


def get_actor_context() -> ActorContext:
    ctx = CONTEXT.get()
    if not ctx:
        raise ActorContextError("No actor context set.")
    return ctx


def set_actor_context(ctx: ActorContext | None) -> None:
    assert ctx is None or isinstance(ctx, ActorContext)
    CONTEXT.set(ctx)


C = TypeVar("C", bound="ActorContext")


@contextlib.contextmanager
def enter_actor_context(ctx: C) -> Generator[C]:
    try:
        original_loop = asyncio.get_event_loop()
    except RuntimeError:
        original_loop = None
    asyncio.set_event_loop(ctx.loop)
    token = CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        CONTEXT.reset(token)
        asyncio.set_event_loop(original_loop)


@dataclasses.dataclass
class ActorContext:
    handler: actors.ActorTypeDispatcher | actors.ActorSystem
    sent_requests: dict[str, asyncio.Future[Any]] = dataclasses.field(default_factory=dict)
    loop: asyncio.AbstractEventLoop = dataclasses.field(default_factory=asyncio.get_event_loop)
    log: logging.Logger = LOG
    sender: actors.ActorAddress | None = None

    @contextlib.contextmanager
    def enter(self) -> Generator[Self]:
        try:
            original_loop = asyncio.get_event_loop()
        except RuntimeError:
            original_loop = None
        asyncio.set_event_loop(self.loop)
        token = CONTEXT.set(self)
        try:
            yield self
        except BaseException:
            self.log.exception("Exception while in the context")
        finally:
            CONTEXT.reset(token)
            asyncio.set_event_loop(original_loop)

    def create_actor(
        self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None
    ) -> actors.ActorAddress:
        self.log.debug("Creating actor of type %s (requirements=%r)...", cls, requirements)
        address = self.handler.createActor(cls, requirements)
        if hasattr(cls, "receiveMsg_ActorConfig"):
            self.send(address, ActorConfig.from_config(cfg))
        self.log.debug("Created actor of type %s.", cls)
        return address

    def create_task(self, coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
        return self.loop.create_task(coro, name=name)

    def send(self, destination: actors.ActorAddress, message: Any) -> None:
        if isinstance(self.handler, actors.ActorSystem):
            return self.handler.tell(destination, message)
        if isinstance(self.handler, actors.Actor):
            return self.handler.send(destination, message)
        raise NotImplementedError(f"Handler type {type(self.handler)} not implemented.")

    def shutdown(self):
        if isinstance(self.handler, actors.ActorSystem):
            self.log.warning("Shutting down actor system (handler=%r)...", self.handler)
            self.handler.shutdown()
            return

        if isinstance(self.handler, actors.Actor):
            self.log.warning("Shutting down actor (handler=%r)...", self.handler)
            self.handler.send(self.handler.myAddress, actors.ActorExitRequest())
            return

        raise NotImplementedError("Cannot shutdown actor context")

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future:
        request = Request.from_message(message, timeout=timeout)
        future = self.sent_requests.get(request.req_id)
        if future is None:
            self.sent_requests[request.req_id] = future = self.loop.create_future()
            original_cancel = future.cancel

            def cancel_wrapper(msg: Any | None = None) -> bool:
                self.sent_requests.pop(request.req_id, None)
                self.send(destination, CancelRequest(message=msg, req_id=request.req_id))
                return original_cancel(msg)

            future.cancel = cancel_wrapper  # type: ignore[method-assign]
            future.add_done_callback(lambda f: self.sent_requests.pop(request.req_id, None))

        if future.done():
            return future

        if timeout is not None:
            task_name = f"request({destination!s}, {message!r}, timeout={timeout!r})"
            future = self.create_task(wait_for(future, timeout=timeout, cancel_message=task_name), name=task_name)
        else:
            task_name = f"request({destination!s}, {message!r}"

        if isinstance(self.handler, actors.Actor):
            self.send(destination, request)
            return future

        if isinstance(self.handler, actors.ActorSystem):
            response = self.handler.ask(destination, request, timeout=min_timeout(0.1, request.timeout))
            self.receive_message(response, destination)
            if future.done():
                return future

            async def listen_for_result() -> Any:
                while not future.done():
                    response = self.handler.listen(timeout=min_timeout(0.1, request.timeout))
                    self.receive_message(response, destination)
                    await asyncio.sleep(0)
                return await future

            return self.create_task(listen_for_result(), name=task_name)

        raise NotImplementedError(f"Cannot send request to actor: invalid handler: {self.handler}.")

    def receive_message(self, message: Any, sender: actors.ActorAddress) -> bool:
        if message is None:
            return True

        if isinstance(message, Response) and self.receive_response(message, sender):
            return True

        if isinstance(message, actors.PoisonMessage) and self.receive_poison_message(message, sender):
            return True

        return False

    def receive_poison_message(self, message: actors.PoisonMessage, sender: actors.ActorAddress) -> bool:
        if isinstance(message.poisonMessage, Request):
            future = self.sent_requests.pop(message.poisonMessage.req_id, None)
            if future and not future.done():
                future.set_exception(PoisonError.from_poison_message(message))
                return True
        return False

    def receive_response(self, response: Response, sender: actors.ActorAddress) -> bool:
        if isinstance(response, ResultResponse) and self.receive_result_response(response, sender):
            return True
        if isinstance(response, RunningTaskResponse) and self.receive_running_task_response(response, sender):
            return True
        return False

    def receive_result_response(self, response: ResultResponse, sender: actors.ActorAddress) -> bool:
        future = self.sent_requests.pop(response.req_id, None)
        if future and not future.done():
            try:
                future.set_result(response.result())
            except Exception as error:
                future.set_exception(error)
        return True

    def receive_running_task_response(self, response: RunningTaskResponse, sender: actors.ActorAddress) -> bool:
        if response.req_id in self.sent_requests:
            LOG.debug("Waiting for actor %s task completion: %s", sender, response.name)
        return True


async def wait_for(future: asyncio.Future[R], *, timeout: float | None = None, cancel_message: Any = None) -> R:
    if future.done() or timeout is None:
        return await future

    await asyncio.wait([future], timeout=timeout)
    if not future.done():
        future.cancel(cancel_message)
        raise TimeoutError(str(cancel_message))

    return await future


def min_timeout(*timeouts: float | None) -> float | None:
    real_timeouts = [t for t in timeouts if t is not None]
    if real_timeouts:
        return min(real_timeouts)
    return None
