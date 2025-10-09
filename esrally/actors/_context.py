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
import contextlib
import contextvars
import dataclasses
import logging
import uuid
from collections.abc import Coroutine, Generator
from typing import Any, Optional, TypeVar

from thespian import actors  # type: ignore[import-untyped]

from esrally import types
from esrally.actors._config import DEFAULT_LOOP_INTERVAL, ActorConfig
from esrally.actors._proto import (
    ActorInitRequest,
    CancelRequest,
    DoneResponse,
    PingRequest,
    PoisonError,
    Request,
    Response,
    RunningTaskResponse,
)

LOG = logging.getLogger(__name__)


CONTEXT = contextvars.ContextVar[Optional["ActorContext"]]("actors.context", default=None)


async def create_actor(cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, **kwargs: Any) -> actors.ActorAddress:
    return await get_actor_context().create_actor(cls=cls, requirements=requirements, **kwargs)


R = TypeVar("R")


def create_task(coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
    """It creates a task and registers it for cancellation with the current request.

    :param coro: the task coroutine
    :param name: the task name
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


def get_actor_context() -> "ActorContext":
    ctx = CONTEXT.get()
    if not ctx:
        raise ActorContextError("No actor context set.")
    return ctx


def set_actor_context(ctx: Optional["ActorContext"]) -> contextvars.Token:
    return CONTEXT.set(ctx)


def reset_actor_context(token: contextvars.Token) -> None:
    CONTEXT.reset(token)


C = TypeVar("C", bound="ActorContext")


@contextlib.contextmanager
def enter_actor_context(ctx: C) -> Generator[C]:
    token = CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        CONTEXT.reset(token)


@dataclasses.dataclass
class ActorContext:
    # handler represents the target actor or actor system this context is about.
    handler: actors.ActorTypeDispatcher | actors.ActorSystem

    # pending_results maps the future results by request ID so used for waiting request responses.
    pending_results: dict[str, asyncio.Future[Any]] = dataclasses.field(default_factory=dict)

    # loop represents the event loop to be used while this context is active.
    loop: asyncio.AbstractEventLoop = dataclasses.field(default_factory=asyncio.get_event_loop)

    # external_request_poll_interval represents a configurable time duration to be used for interrupting
    # ActorSystem.ask or ActorSystem.listen methods execution for running the event loop.
    loop_interval: float = DEFAULT_LOOP_INTERVAL

    @property
    def actor(self) -> actors.Actor:
        if not isinstance(self.handler, actors.Actor):
            raise ActorContextError("Actor context handler is not an actor")
        return self.handler

    @property
    def actor_system(self) -> actors.ActorSystem:
        if not isinstance(self.handler, actors.ActorSystem):
            raise ActorContextError("Actor context handler is not an actor system")
        return self.handler

    @property
    def details(self) -> dict[str, Any]:
        return {"cls": type(self.handler).__name__}

    def shutdown(self):
        """It shuts down the actor o actor system handler of this context."""
        if isinstance(self.handler, actors.ActorSystem):
            LOG.warning("Shutting down actor system: %s...", self.handler)
            self.handler.shutdown()
            return

        if isinstance(self.handler, actors.Actor):
            LOG.warning("Shutting down actor: %s...", self.handler)
            self.handler.send(self.handler.myAddress, actors.ActorExitRequest())
            return

        raise NotImplementedError("Cannot shutdown actor context")

    async def create_actor(
        self,
        cls: type[actors.Actor],
        *,
        requirements: dict[str, Any] | None = None,
        cfg: types.Config | None = None,
        message: Any | None = None,
    ) -> actors.ActorAddress:
        """It creates a new actor in the current actor context."""
        LOG.debug("Creating actor of type %s (requirements=%r)...", cls, requirements)
        address = self.handler.createActor(cls, requirements)
        if hasattr(cls, "receiveMsg_Config"):
            try:
                self.send(address, ActorInitRequest(cfg=ActorConfig.from_config(cfg), message=message))
            except BaseException:
                self.send(address, actors.ActorExitRequest())
                LOG.exception("Error initializing actor %s (requirements=%r)...", cls, requirements, exc_info=True)
                raise
        LOG.debug("Created actor of type %s.", cls)
        return address

    def create_task(self, coro: Coroutine[None, None, R], *, name: str | None = None) -> asyncio.Task[R]:
        """It creates an async task in the event loop of the current actor context."""
        return self.loop.create_task(coro, name=f"{name or coro}@{self.handler}")

    def send(self, destination: actors.ActorAddress, message: Any) -> None:
        """It sends a message using the actor or actor system handler of this context."""
        if isinstance(self.handler, actors.ActorSystem):
            return self.handler.tell(destination, message)
        if isinstance(self.handler, actors.Actor):
            return self.handler.send(destination, message)
        raise NotImplementedError(f"Handler type {type(self.handler)} not implemented.")

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future:
        """It sends a request using the actor or actor system handler of this context and returns an awaitable future result."""
        req_id = getattr(message, "req_id", "") or str(uuid.uuid4())
        request: Request = Request.from_message(message, timeout=timeout, req_id=req_id)
        future = self.pending_results.get(req_id)
        if future is None:
            self.pending_results[req_id] = future = self.loop.create_future()

            original_cancel = future.cancel

            def cancel_wrapper(msg: Any | None = None) -> bool:
                if self.pending_results.pop(request.req_id, None):
                    self.send(destination, CancelRequest(message=msg, req_id=request.req_id))
                return original_cancel(msg)

            future.cancel = cancel_wrapper  # type: ignore[method-assign]
            future.add_done_callback(lambda f: self.pending_results.pop(request.req_id, None))

        if future.done():
            # It could be this request has been already sent before and for some reason it is retrying it again.
            # Being the request done, it avoids resending it. To send a new request and avoid race condition it is
            # expected the req_id to be different from previous ones. In this way it works around the situation caller
            # retries to execute a request that eventually is already in progress.
            return future

        if timeout is not None:
            # It implements the timeout by wrapping the future with a waiter async task so that it can raise a
            # TimeoutError on the requester side, while sending a CancelRequest to the destination actor so it will
            # eventually cancel async tasks created while processing this request.
            task_name = f"request({destination!s}, {message!r}, timeout={timeout!r})"
            future = self.create_task(wait_for(future, timeout=timeout, cancel_message=task_name), name=task_name)
        else:
            task_name = f"request({destination!s}, {message!r}"

        if isinstance(self.handler, actors.Actor):
            # When running inside an actor, it relies on the asynchronous thespian actor implementation. The event
            # loop will be run using a periodic actor wakeup message.
            self.send(destination, request)
            return future

        if isinstance(self.handler, actors.ActorSystem):
            # It avoids blocking in SystemActor.ask and SystemActor.listen methods by setting a timeout long enough to
            # send the request, but short enough to periodically run the event loop and process request timeouts, async
            # tasks and futures callbacks.
            response = self.handler.ask(destination, request, timeout=min_timeout(request.timeout, self.loop_interval))
            self.receive_message(response, destination)
            if future.done():
                return future

            async def listen_for_result() -> Any:
                # It consumes response messages or poison errors until we get the response for this request.
                while not future.done():
                    await asyncio.sleep(0)  # It runs the event loop before listening for messages again.
                    response = self.handler.listen(timeout=min_timeout(request.timeout, self.loop_interval))
                    self.receive_message(response, destination)
                return await future

            # It will listen for incoming messages later in the event loop the next time the caller will await for some
            # incoming event. This should allow gathering multiple request responses ant the same time.
            self.create_task(listen_for_result(), name=task_name)
            return future

        raise NotImplementedError(f"Cannot send request to actor: invalid handler: {self.handler}.")

    def receive_message(self, message: Any, sender: actors.ActorAddress) -> bool:
        """It dispatches the handling of a message received from an Actor or an external ActorSystem."""
        if message is None:
            return True
        if isinstance(message, Response) and self.receive_response(message, sender):
            return True
        if isinstance(message, actors.PoisonMessage) and self.receive_poison_message(message, sender):
            return True
        if isinstance(self.handler, actors.ActorSystem):
            LOG.warning("Ignored message from actor %s while waiting for response: %r.", sender, message)
            return True
        # The message hasn't been consumed.
        return False

    def receive_poison_message(self, message: actors.PoisonMessage, sender: actors.ActorAddress) -> bool:
        """It handles a poison message from an Actor or an external ActorSystem matching it with a pending request response."""
        if isinstance(message.poisonMessage, Request):
            future = self.pending_results.pop(message.poisonMessage.req_id, None)
            if future and not future.done():
                future.set_exception(PoisonError.from_poison_message(message))
                return True
        # The message hasn't been consumed.
        return False

    def receive_response(self, response: Response, sender: actors.ActorAddress) -> bool:
        """It looks for a pending future result matching this request response."""
        if isinstance(response, DoneResponse) and self.receive_result_response(response, sender):
            return True
        if isinstance(response, RunningTaskResponse) and self.receive_running_task_response(response, sender):
            return True
        # The message hasn't been consumed.
        return False

    def receive_result_response(self, response: DoneResponse, sender: actors.ActorAddress) -> bool:
        """It looks for a pending future result matching this request response."""
        future = self.pending_results.pop(response.req_id, None)
        if future and not future.done():
            # It extracts the result from the response and transfer it to the async future object.
            try:
                future.set_result(response.result())
            except Exception as error:
                future.set_exception(error)
            return True
        # The message hasn't been consumed.
        return False

    def receive_running_task_response(self, response: RunningTaskResponse, sender: actors.ActorAddress) -> bool:
        if response.req_id in self.pending_results:
            LOG.debug("Waiting for actor %s task completion: %s", sender, response.name)
            return True
        # The message hasn't been consumed.
        return False

    def __str__(self) -> str:
        details = ", ".join((f"{k}={v}" for k, v in sorted(self.details.items()) if v is not None))
        return f"{type(self).__name__}<{details}>"


async def wait_for(future: asyncio.Future[R], *, timeout: float | None = None, cancel: bool = True, cancel_message: Any = None) -> R:
    """It waits for a future results and extract its value.

    In the case no value is available before timeout seconds, then it cancels the future (if not done) and raises TimeoutError

    :param future: The future to wait for.
    :param timeout: The timeout to wait for (in seconds).
    :param cancel: If True, it will cancel the future before raising TimeoutException.
    :param cancel_message: The message to be used for cancelling the future.
    :return: the future result value if any is set before timeout.
    :raises TimeoutError: if timeout is reached before the future is done.
    :raises Exception: if the future has been set with an exception value.
    """
    if future.done() or timeout is None:
        return await future

    await asyncio.wait([future], timeout=timeout)

    if not future.done():
        if cancel:
            future.cancel(cancel_message)
        raise TimeoutError(str(cancel_message))

    return await future


def min_timeout(*timeouts: float | None) -> float | None:
    """It picks the minimal non None timeout time duration between given values."""
    real_timeouts = [t for t in timeouts if t is not None]
    if real_timeouts:
        return max(0.0, min(real_timeouts))
    return None
