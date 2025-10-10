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
import asyncio
import collections
import dataclasses
import inspect
import logging
import uuid
from collections.abc import Awaitable, Coroutine
from typing import Any, TypeVar

from thespian import actors  # type: ignore[import-untyped]

from esrally import config, exceptions
from esrally.actors._config import ActorConfig
from esrally.actors._context import (
    ActorContext,
    ActorContextError,
    enter_actor_context,
    get_actor_context,
    set_actor_context,
)
from esrally.actors._proto import (
    ActorInitRequest,
    CancelledResponse,
    CancelRequest,
    MessageRequest,
    PingRequest,
    PongResponse,
    Request,
    Response,
    RunningTaskResponse,
)

LOG = logging.getLogger(__name__)


def get_actor() -> actors.Actor:
    """It returns the actor where the actual message is being received."""
    return get_actor_context().actor


def get_actor_request_context() -> "ActorRequestContext":
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
    """Actor context being used while some received messages is being processed by an actor."""

    # sender represents the address of the actor that sent current request message.
    sender: actors.ActorAddress | None = None

    # req_id represents the unique request ID carried by current request message.
    req_id: str = ""

    # pending_tasks contains sets of asyncio tasks (indexed by req_id) to be cancelled in case of a CancelRequest is
    # received.
    pending_tasks: dict[str, set[asyncio.Task]] = dataclasses.field(default_factory=lambda: collections.defaultdict(set))

    # responded will be true after current request message has been responded. The purpose of this flag is avoiding
    # returning multiple responses to the same 'req_id'.
    responded: bool = False

    @property
    def actor(self) -> "AsyncActor":
        """actor property returns the local actor where the current messages are being received."""
        assert isinstance(self.handler, AsyncActor)
        return self.handler

    @property
    def details(self) -> dict[str, Any]:
        return {"sender": self.sender, "address": self.actor.myAddress, "req_id": self.req_id, **super().details}

    def create_task(self, coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
        """create_task is a wrapper around asyncio.AbstractEventLoop.create_task

        While processing a request message will register task for cancellation in case a CancelRequest message
        is received.

        Please note that while processing a request from inside an actor, all tasks created by calling this method will
        be cancelled in case of a CancelRequest message. This could also include requests that have been forwarded to
        other actors. To prevent this to happen, please use asyncio.create_task function instead.

        :param coro: The coroutine to wrap.
        :param name: The name of the task.
        :return: The wrapper task.
        """
        task = super().create_task(coro, name=name)
        if self.req_id:

            def remove_task(f: asyncio.Task) -> None:
                tasks = self.pending_tasks.get(self.req_id)
                if tasks is not None:
                    tasks.discard(f)
                    if not tasks:
                        del self.pending_tasks[self.req_id]

            task.add_done_callback(remove_task)

            self.pending_tasks[self.req_id].add(task)
            self.send(self.sender, RunningTaskResponse(req_id=self.req_id, name=task.get_name()))
        return task

    def receive_message(self, message: Any, sender: actors.ActorAddress) -> bool:
        """receive_message is called by the actor when receiving a new message."""
        # It processes responses.
        if super().receive_message(message, sender):
            return True

        assert get_actor_context() is self, "Actor context not registered."
        assert not (self.req_id or self.sender or self.responded), "Actor context already used."

        self.sender = sender  # Destination address for `actors.respond` function.
        try:
            if isinstance(message, Request):
                # It handles the request message.
                response = self.receive_request(message)
            else:
                # It dispatches the message back to the actor.
                response = self.dispatch_message(message)

            # This allows AsyncActors to answer even if request message was sent without calling request method.
            # This is intended for AsyncActor to be able to answer requests from non async actors too.
            self.respond(response)
        except Exception as error:
            # In case req_id is not set, it expects Thespian to send back a PoisonMessage as a response.
            self.respond(error=error)
        return True

    def receive_request(self, request: Request) -> Any:
        """It processes a request message."""
        # This will be later used from respond method for creating a response.
        self.req_id = request.req_id

        if isinstance(request, CancelRequest):
            # It cancels all tasks of a previous request.
            self.cancel_request(request.message)
            # It sends confirmation back.
            return CancelledResponse(req_id=self.req_id, status=request.message)

        if isinstance(request, PingRequest):
            # It processes a ping request.
            if request.destination in [None, self.actor.myAddress]:
                # It responds to the ping request.
                return PongResponse(req_id=request.req_id, status=request.message)
            # It forwards the ping request to the destination actor.
            return self.request(request.destination, request)

        if isinstance(request, ActorInitRequest):
            # It receives configuration after actor creation.
            response = self.dispatch_message(request.cfg)
            if response is not None:
                raise TypeError(f"Unexpected response from actor configuration handler: {response!r}, want None")

        if isinstance(request, MessageRequest):
            # It dispatches the inner request message (if any).
            return self.dispatch_message(request.message)

        return None

    def dispatch_message(self, message: Any) -> Any:
        """It dispatches the message back to the actor."""
        if message is None:
            return None
        return actors.ActorTypeDispatcher.receiveMessage(self.actor, message, self.sender)

    def respond(self, status: Any = None, error: Exception | None = None) -> None:
        """It sends a response message to the sender actor."""
        # It ensures a request gets responded only once in the scope of this context.
        if self.responded:
            if error is not None:
                # The error will eventually reach requester in the form of a PoisonMessage.
                raise error
            if status is not None:
                raise ActorContextError("Already responded.")

        if error is None and inspect.isawaitable(status):
            # It schedules a task to wait for the actual status before responding.
            # Please note that the final status could be another awaitable that would
            # make it spawning another task to wait for it. This should ensure there
            # should always be a response for every request which processing come to
            # an end. All tasks will be cancelled in case of a CancelRequest message is received
            # or once this task finishes running.
            self.create_task(self.respond_later(status), name=f"respond_later({status!r})")
            return

        if self.req_id:
            # The status or the error will eventually reach requester in the form of a Response message,
            # so that the requester can match the target future by using its req_id.
            response = Response.from_status(self.req_id, status, error)
        elif error is None:
            # The status will eventually reach requester in the form of a standalone message without any Response
            # message envelope.
            response = status
        else:
            # The error will eventually reach requester in the form of a PoisonMessage.
            raise error

        if response is not None:
            # It finally sends a response.
            self.send(self.sender, response)
            LOG.debug("Sent response to actor %s: %r", self.sender, response)

        # Reaching this point it mean there is nothing more to respond to the request and all pending tasks can be
        # cancelled.
        self.responded = True
        self.cancel_request()

    def cancel_request(self, message: Any = None) -> None:
        """It cancels all pending tasks of the current request."""
        for t in self.pending_tasks.pop(self.req_id, []):
            if not t.done():
                t.cancel(message)

    async def respond_later(self, status: Awaitable) -> None:
        """respond_later awaits for a response to get ready.

        It makes sures a response is always sent to the request sender, even if the awaited task is cancelled.
        """
        try:
            self.respond(status=await status)
        except Exception as error:
            self.respond(error=error)
        except asyncio.CancelledError as error:
            self.respond(CancelledResponse(status=str(error), req_id=self.req_id))
            raise


class AsyncActor(actors.ActorTypeDispatcher):
    """Override the thespian ActorTypeDispatcher with some few additional features.

    Additional features include:
    - It uses its own `asyncio` event loop to run asynchronous tasks (co-routines). The loop is set as current during
      messages processing.
    - It periodically run pending tasks from its event loop.
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
      request senders should receive CancelledError while waiting for a response.
    - It creates its own logger.
    """

    def __init__(self) -> None:
        super().__init__()
        self._pending_results: dict[str, asyncio.Future] = {}
        self._pending_tasks: dict[str, set[asyncio.Task]] = collections.defaultdict(set)
        self._pending_task_timer_id: str = ""
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop() or asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        logging.setLogRecordFactory(ActorLogRecord)
        # A default configuration is required when using "spawn" process creation method for parts that requires it
        # (like ActorRequestContext). When using "fork" the parent process already set one for us.
        try:
            config.get_config()
        except exceptions.ConfigError:
            config.init_config(ActorConfig())

    def __str__(self) -> str:
        return f"{self.__class__.__name__}@{self.myAddress}"

    def receiveMessage(self, message: Any, sender: actors.ActorAddress) -> Any:
        """It receives actor messages inside a fresh new ActorRequestContext which will process it before dispatching.

        :param message:
        :param sender:
        :return:
        """
        ctx = ActorRequestContext(handler=self, pending_results=self._pending_results, loop=self._loop, pending_tasks=self._pending_tasks)
        with enter_actor_context(ctx) as ctx:
            ctx.receive_message(message, sender)

    def receiveUnrecognizedMessage(self, message: Any, sender: actors.ActorAddress) -> None:
        """This will eventually let know sender actor his message reached the wrong destination."""
        raise TypeError(f"Received unrecognized message: {message}")

    def receiveMsg_ActorConfig(self, cfg: ActorConfig, sender: actors.ActorAddress) -> None:
        """It receives configuration from an actor initialization message.

        It registers the configuration for the current process.
        """
        LOG.debug("Received configuration message: %s", cfg)
        config.init_config(cfg, force=True)

        # It starts the pending task timer.
        self._pending_task_timer_id = f"pending_tasks_timer:{uuid.uuid4()}"
        self.wakeupAfter(cfg.loop_interval, self._pending_task_timer_id)

    def receiveMsg_WakeupMessage(self, message: actors.WakeupMessage, sender: actors.ActorAddress) -> None:
        """It executes pending tasks on a scheduled time period."""
        if message.payload is None:
            return None
        if isinstance(message.payload, str) and message.payload.startswith("pending_tasks_timer:"):
            if message.payload == self._pending_task_timer_id:
                try:
                    self._loop.run_until_complete(nop())
                finally:
                    self.wakeupAfter(message.delayPeriod, self._pending_task_timer_id)
            return None
        # It dispatches payload message as a standalone message.
        self.receiveMessage(message.payload, sender)

    def receiveMsg_ActorExitRequest(self, request: actors.ActorExitRequest, sender: actors.ActorAddress) -> None:
        """It cancels all pending tasks in the event loop, then stops the loop and finally unlink the current context."""
        LOG.debug("Received ActorExitRequest message.")

        # It stops pending task timer.
        self._pending_task_timer_id = ""

        # It cancels every request pending task one by one.
        while self._pending_tasks:
            _, tasks = self._pending_tasks.popitem()
            for task in tasks:
                task.cancel("Actor exit request.")

        # It stops the event loop.
        if self._loop is not None:
            self._loop.stop()
            asyncio.set_event_loop(None)

        # It cleans up the actor context.
        logging.setLogRecordFactory(logging.LogRecord)
        set_actor_context(None)


async def nop() -> None: ...


class ActorLogRecord(logging.LogRecord):
    """Logging record with actor context name extra field."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.actor_context: dict[str, Any] = {}
        try:
            self.actor_context.update(get_actor_context().details)
        except ActorContextError:
            pass
