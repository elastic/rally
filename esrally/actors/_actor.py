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
import collections
import contextvars
import dataclasses
import inspect
import logging
from collections.abc import Awaitable, Coroutine
from typing import Any, TypeVar

from thespian import actors  # type: ignore[import-untyped]

from esrally.actors._config import ActorConfig
from esrally.actors._context import (
    ActorContext,
    ActorContextError,
    get_actor_context,
    set_actor_context,
)
from esrally.actors._proto import (
    CancelledResponse,
    CancelRequest,
    PingRequest,
    PongResponse,
    Request,
    ResultResponse,
    RunningTaskResponse,
)
from esrally.config import init_config

LOG = logging.getLogger(__name__)


def get_actor() -> AsyncActor:
    """It returns the actor where the actual message is being received."""
    return get_actor_request_context().actor


def get_actor_request_context() -> ActorRequestContext:
    """It retrieve details about the context where the actual message is being received."""
    ctx = get_actor_context()
    if not isinstance(ctx, ActorRequestContext):
        raise ActorContextError(f"Context is not a AsyncActorContext: {ctx!r}")
    return ctx


def respond(status: Any = None, error: Exception | None = None) -> None:
    """It sends a response message to the sender actor."""
    get_actor_request_context().respond(status=status, error=error)


R = TypeVar("R")


@dataclasses.dataclass
class ActorRequestContext(ActorContext):

    req_id: str = ""
    request_tasks: dict[str, set[asyncio.Task]] = dataclasses.field(default_factory=lambda: collections.defaultdict(set))
    responded: bool = False

    @property
    def actor(self) -> AsyncActor:
        assert isinstance(self.handler, AsyncActor)
        return self.handler

    def create_task(self, coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
        task = super().create_task(coro, name=name)
        if self.req_id:
            self.request_tasks[self.req_id].add(task)
            self.send(self.sender, RunningTaskResponse(req_id=self.req_id, name=task.get_name()))
        return task

    def receive_message(self, message: Any, sender: actors.ActorAddress) -> bool:
        """Invoke by the actor when receiving a new message."""

        if isinstance(message, Request) and self.receive_request(message, sender):
            return True

        if isinstance(message, ActorConfig) and self.receive_actor_config(message, sender):
            return True

        if super().receive_message(message, sender):
            return True

        actors.ActorTypeDispatcher.receiveMessage(self.actor, message, sender)
        return True

    def receive_actor_config(self, cfg: ActorConfig, sender: actors.ActorAddress) -> bool:
        """It adds the configuration to the actor context."""
        self.log.debug("Received configuration message: %s", cfg)
        init_config(cfg, force=True)
        return False

    def receive_request(self, request: Request, sender: actors.ActorAddress) -> bool:
        try:
            ctx = get_actor_context()
        except ActorContextError:
            ctx = None
        if ctx is not self:
            with self.enter():
                return self.receive_request(request, sender)

        self.log.debug("Received request from actor %s: %s", sender, request)
        self.sender = sender
        self.req_id = request.req_id
        try:
            if isinstance(request, CancelRequest) and self.cancel_request(request.message):
                return True

            if isinstance(request, PingRequest) and self.receive_ping_request(request):
                return True

            self.respond(actors.ActorTypeDispatcher.receiveMessage(self.actor, request.message, sender))
            return True

        except Exception as error:
            self.respond(error=error)
            return True

    def receive_cancel_request(self, request: CancelRequest) -> bool:
        self.cancel_request(request.message)
        return True

    def receive_ping_request(self, request: PingRequest) -> bool:
        if request.destination in [None, self.actor.myAddress]:
            # It responds to the ping request.
            self.respond(PongResponse(request.req_id, request.message))
        else:
            # It forwards the request to the destination actor.
            self.respond(self.request(request.destination, request))
        return True

    def cancel_request(self, message: Any = None) -> bool:
        for t in self.request_tasks.pop(self.req_id, []):
            if not t.done():
                t.cancel(message)
        if not self.responded:
            self.respond(CancelledResponse(self.req_id, message))
        return True

    def respond(self, status: Any = None, error: Exception | None = None) -> None:
        """It sends a response message to the sender actor."""
        # It ensures a request gets responded only once in the scope of this context.
        if self.responded:
            if error is not None:
                # The error will eventually reach requester in the form of a PoisonMessage
                raise error
            if status is not None:
                raise ActorContextError("Already responded.")

        if error is None and inspect.isawaitable(status):
            # It schedules a task to wait for the actual status before responding.
            # Please note that the final status could be another awaitable that would
            # make it spawning another task to wait for it. This should ensure there
            # should always be a response for every request which processing come to
            # an end. All tasks will be cancelled in case of a CancelRequest message is received.
            self.create_task(self.respond_later(status), name=str(status))
            return

        if self.req_id:
            # The status or the error will eventually reach requester in the form of a Response message,
            # so that the requester can match the target future by using its req_id.
            response = ResultResponse.from_status(self.req_id, status, error)
        elif error is None:
            # The status will eventually reach requester in the form of a standalone message without any req_id.
            response = status
        else:
            # The error will eventually reach requester in the form of a PoisonMessage
            raise error

        if response is not None:
            # It finally sends a response.
            self.send(self.sender, response)

        self.responded = True
        LOG.debug("Sent response to actor %s: %r", self.sender, response)
        self.cancel_request()
        return

    async def respond_later(self, status: Awaitable) -> None:
        try:
            self.respond(status=await status)
        except Exception as error:
            self.respond(error=error)
        except asyncio.CancelledError as error:
            self.respond(CancelledResponse(message=str(error), req_id=self.req_id))
            raise


class AsyncActor(actors.ActorTypeDispatcher):
    """Override the thespian ActorTypeDispatcher with some few additional features.

    Additional features include:
    - It uses its own `asyncio` event loop to run asynchronous tasks (co-routines). The loop is set as current during
      messages processing.
    - It periodically run pending tasks from the loop.
    - The methods processing a message type can be async coroutines, on which case they will be scheduled for execution
      as an async task of the actor event loop. While messages are being processed by these co-routines, other messages
      and loop events can be processed from the actor, making the actor truly asynchronous.
    - It implements `request` method, an awaitable version of `Actor.send` method. It sends a message with a unique
      request ID, and until a response with the same ID is received, from the target actor, other messages and loop
      events are being processed from the actor.
    - When receiving an ActorConfig message it inits context configuration with it.
    - When receiving a PoisonErrorMessage as response of `request` method, it translates it to a PoisonError and raises
      it as a possible outcome of waiting for a response.
    - When receiving an ActorExitRequest, it cancels all pending tasks from the loop, then stops the loop, so that
      requesters actors should receive CancelledError while waiting for a response.
    - It creates its own logger.
    """

    def __init__(self) -> None:
        super().__init__()
        self._log: logging.Logger | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sent_requests: dict[str, asyncio.Future] = {}
        self._request_tasks: dict[str, set[asyncio.Task]] = collections.defaultdict(set)
        self._ctx: contextvars.Context = contextvars.copy_context()

    @property
    def log(self) -> logging.Logger:
        """It returns the logger of this actor."""
        if self._log is None:
            self._log = self.logger(f"{type(self).__module__}.{type(self).__name__}")
        return self._log

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """It returns the event loop of this actor."""
        loop = self._loop
        if loop is None:
            self._loop = loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # It schedules the periodic execution of actor's asyncio tasks.
            self._run_pending_tasks()
        return loop

    def receiveMessage(self, message: Any, sender: actors.ActorAddress) -> None:
        """It makes sure the message is handled with the actor context variables."""
        ctx = ActorRequestContext(
            handler=self, sent_requests=self._sent_requests, loop=self.loop, log=self.log, request_tasks=self._request_tasks
        )
        self._ctx.run(ctx.receive_message, message, sender)

    def receiveMsg_ActorConfig(self, message: ActorConfig, sender: actors.ActorAddress) -> None:
        self.log.debug("Received actor config: %r", message)

    def receiveMsg_ActorExitRequest(self, message: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        """It cancels all pending tasks in the event loop, then stops the loop and finally unlink the current context."""
        while self._request_tasks:
            _, tasks = self._request_tasks.popitem()
            for task in tasks:
                task.cancel("Actor exit.")
        if self._loop is not None:
            self._loop.stop()
            self._loop = None
        set_actor_context(None)

    _RUN_PENDING_TASKS = "RunPendingTasks"

    def receiveMsg_WakeupMessage(self, message: actors.WakeupMessage, sender: actors.ActorAddress) -> None:
        """It executes pending tasks on a scheduled time period."""
        if message.payload is None:
            return None
        if message.payload == self._RUN_PENDING_TASKS:
            self._run_pending_tasks()
            return None
        super().receiveMessage(message.payload, sender)

    def _run_pending_tasks(self) -> None:
        try:
            self.loop.run_until_complete(nop())
        finally:
            self.wakeupAfter(0.001, self._RUN_PENDING_TASKS)


async def nop() -> None: ...
