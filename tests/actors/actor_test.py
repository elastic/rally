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
import dataclasses
import functools
import random
from collections.abc import Callable, Generator
from typing import Any, Literal, get_args

import pytest

from esrally import actors, config, types
from esrally.actors import ActorConfig, _actor, _context, _proto
from esrally.utils import cases


@pytest.fixture(scope="function", autouse=True)
def event_loop() -> Generator[asyncio.AbstractEventLoop, Any, None]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture(scope="function", params=get_args(actors.SystemBase))
def system_base(request) -> Generator[actors.SystemBase]:
    yield request.param


@pytest.fixture(scope="function", autouse=True)
def cfg(system_base: actors.SystemBase) -> Generator[types.Config]:
    cfg = actors.ActorConfig()
    cfg.system_base = system_base
    cfg.try_join = False
    config.init_config(cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="function", autouse=True)
def system() -> Generator[actors.ActorSystem]:
    system = actors.init_actor_system()
    yield system
    actors.shutdown()


@pytest.fixture(scope="function")
def dummy_actor() -> Generator[actors.ActorAddress]:
    address = actors.create_actor(DummyActor)
    yield address
    actors.send(address, actors.ActorExitRequest())


# It indicates if a test case has to be executed from an outside or from inside an actor.
ExecuteFrom = Literal["from_external", "from_actor"]


# This fixtures patches the test function so that its being executed from inside an AsyncActor in case param is "from_actor".
@pytest.fixture(scope="function", params=get_args(ExecuteFrom))
def execute_from(request, monkeypatch) -> Generator[ExecuteFrom, None, None]:
    if request.param == "from_actor":
        actor = actors.create_actor(ExecutorActor)
        request.addfinalizer(lambda: actors.send(actor, actors.ActorExitRequest()))

        original_func = getattr(request.module, request.function.__name__)

        @functools.wraps(request.function)
        async def func_wrapper(*args, **kwargs):
            return await actors.request(actor, ExecuteRequest(original_func, args=args, kwargs=kwargs))

        assert isinstance(request.node, pytest.Function)
        request.node.obj = func_wrapper

    yield request.param


@pytest.mark.asyncio
async def test_get_actor_context(execute_from: ExecuteFrom):
    ctx = actors.get_actor_context()
    assert isinstance(ctx, actors.ActorContext)
    assert ctx.pending_results == {}

    if execute_from == "from_external":
        assert not isinstance(ctx, _actor.ActorRequestContext)
        assert isinstance(ctx.handler, actors.ActorSystem)
        assert ctx.handler is actors.get_actor_system()
        assert str(ctx) == "ActorContext[ActorSystem]", f"invalid value: 'str(ctx)' -> {ctx}"

    elif execute_from == "from_actor":
        assert isinstance(ctx, _actor.ActorRequestContext), "Actor request context initialized."
        assert ctx.handler is ctx.actor, "Actor request context initialized."
        assert isinstance(ctx.actor, ExecutorActor), "This is being executed from inside a executor actor."
        assert ctx.actor is actors.get_actor(), "Actor request context initialized."
        assert ctx.req_id
        assert ctx.sender is not None
        assert not ctx.responded, "Actor request is not responded."
        actors.respond()
        assert ctx.responded, "Actor request is responded."
        assert ctx.pending_tasks == {}
        assert ctx.name == f"ExecutorActor|{ctx.req_id}"
        assert str(ctx) == f"ActorContext[ExecutorActor|{ctx.req_id}]", f"invalid value: 'str(ctx)' -> {ctx}"

    else:
        raise NotImplementedError()


@pytest.mark.asyncio
async def test_return_result(dummy_actor: actors.ActorAddress) -> None:
    result = random.random()
    assert result == await actors.request(dummy_actor, ResponseRequest(result))


@pytest.mark.asyncio
async def test_respond_status(dummy_actor: actors.ActorAddress) -> None:
    result = random.random()
    assert result == await actors.request(dummy_actor, ResponseRequest(result, explicit=True))


@pytest.mark.asyncio
async def test_raises_error(dummy_actor: actors.ActorAddress) -> None:
    error = RuntimeError(f"some ramdom error: {random.random()}")
    with pytest.raises(RuntimeError):
        await actors.request(dummy_actor, ResponseRequest(error=error))


@pytest.mark.asyncio
async def test_respond_error(dummy_actor: actors.ActorAddress) -> None:
    error = ValueError(f"some ramdom error: {random.random()}")
    with pytest.raises(ValueError):
        await actors.request(dummy_actor, ResponseRequest(error=error, explicit=True))


@pytest.mark.asyncio
async def test_timeout_request(execute_from: ExecuteFrom, dummy_actor: actors.ActorAddress) -> None:
    """This verifies a blocking task would not block the actor from processing further messages."""
    future = actors.request(dummy_actor, BlockingRequest(timeout=10.0), timeout=0.2)
    value = random.random()
    assert value == await actors.ping(dummy_actor, message=value)
    with pytest.raises(TimeoutError):
        await future


@pytest.mark.asyncio
async def test_cancel_request(execute_from: ExecuteFrom, dummy_actor: actors.ActorAddress) -> None:
    ctx = actors.get_actor_context()
    assert ctx.pending_results == {}

    blocking = actors.request(dummy_actor, BlockingRequest(timeout=300.0))
    assert not blocking.done()
    if execute_from == "from_actor":
        assert not blocking.done()
        assert len(ctx.pending_results) == 1

    value = random.random()
    assert value == await actors.ping(dummy_actor, message=value)

    blocking.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocking

    assert blocking.cancelled()
    assert ctx.pending_results == {}


@pytest.mark.asyncio
async def test_blocking_request(execute_from: ExecuteFrom, dummy_actor: actors.ActorAddress) -> None:
    blocking = actors.request(dummy_actor, BlockingRequest(timeout=300.0))
    value = random.random()
    assert value == await actors.ping(dummy_actor, message=value)
    with pytest.raises(TimeoutError):
        await actors.wait_for(blocking, timeout=0.1, cancel_message="too much time!")


@pytest.mark.asyncio
async def test_actor_config(execute_from: ExecuteFrom, dummy_actor: actors.ActorAddress) -> None:
    assert config.get_config() == await actors.request(
        dummy_actor, GetConfigRequest()
    ), "Parent actor received configuration from system context."


@pytest.mark.asyncio
async def test_request(execute_from: ExecuteFrom, dummy_actor: actors.ActorAddress) -> None:
    ctx = actors.get_actor_context()
    assert ctx.pending_results == {}

    request = _proto.PingRequest(message=random.random())
    future = actors.request(dummy_actor, request)
    if execute_from == "from_actor":
        assert not future.done()
        assert len(ctx.pending_results) == 1

    response = await future
    assert response == request.message
    assert ctx.pending_results == {}


@pytest.mark.asyncio
async def test_create_actor(execute_from: ExecuteFrom) -> None:
    # It uses this special configuration object to check it is going to be propagated to child actor as it is.
    special_config = actors.ActorConfig()
    special_config.admin_ports = range(100, 200)
    actor_address = actors.create_actor(DummyActor, cfg=special_config)
    try:
        actor_config = await actors.request(actor_address, GetConfigRequest())
        assert isinstance(actor_config, ActorConfig)
        assert actor_config.admin_ports == special_config.admin_ports
    finally:
        actors.send(actor_address, actors.ActorExitRequest())


@dataclasses.dataclass
class CreateTaskCase:
    task_name: str | None = None


@cases.cases(
    default=CreateTaskCase(),
    name=CreateTaskCase(task_name="some-task-name"),
)
@pytest.mark.asyncio
async def test_create_task(case: CreateTaskCase, execute_from: ExecuteFrom) -> None:
    result = asyncio.get_event_loop().create_future()

    async def task() -> Any:
        return await result

    coro = task()
    task = actors.create_task(coro, name=case.task_name)
    assert isinstance(task, asyncio.Task)
    assert not task.cancelled()
    assert not task.done()

    if case.task_name is None:
        assert task.get_name() == f"{coro}@{actors.get_actor_context().name}", f"unexpected task name: {task.get_name()}"
    else:
        assert task.get_name() == f"{case.task_name}@{actors.get_actor_context().name}", f"unexpected task name: {task.get_name()}"

    if execute_from == "from_actor":
        ctx = actors.get_actor_context()
        assert isinstance(ctx, _actor.ActorRequestContext)
        assert task in ctx.pending_tasks[ctx.req_id]

    result.set_result(random.random())
    assert await result == await task
    assert not task.cancelled()
    assert task.done()

    if execute_from == "from_actor":
        ctx = actors.get_actor_context()
        assert isinstance(ctx, _actor.ActorRequestContext)
        assert task not in ctx.pending_tasks[ctx.req_id]


class CheckActorContextRequest:
    pass


@dataclasses.dataclass
class ResponseRequest:
    status: Any = None
    error: Exception | None = None
    explicit: bool = False


class CreateChildRequest:
    pass


class GetConfigRequest:
    pass


@dataclasses.dataclass
class BlockingRequest:
    timeout: float
    destination: actors.ActorAddress | None = None


class DummyActor(actors.AsyncActor):

    def receiveMsg_ResponseRequest(self, request: ResponseRequest, sender: actors.ActorAddress) -> Any:
        if request.explicit:
            ctx = _context.get_actor_context()
            assert isinstance(ctx, _actor.ActorRequestContext), "Actor request context initialized."
            assert not ctx.responded, "Actor responded flag not set."
            actors.respond(status=request.status, error=request.error)
            assert ctx.responded, "Actor responded flag set."
        elif request.error is not None:
            raise request.error
        else:
            return request.status

    def receiveMsg_CreateChildRequest(self, message: GetConfigRequest, sender: actors.ActorAddress) -> actors.ActorAddress:
        return actors.create_actor(DummyActor)

    def receiveMsg_GetConfigRequest(self, message: GetConfigRequest, sender: actors.ActorAddress) -> types.Config:
        return config.get_config()

    async def receiveMsg_BlockingRequest(self, request: BlockingRequest, sender: actors.ActorAddress) -> None:
        if request.destination in [None, self.myAddress]:
            await asyncio.sleep(request.timeout)
            return
        return await actors.request(request.destination, request)


@dataclasses.dataclass
class ExecuteRequest:
    func: Callable
    args: tuple = tuple()
    kwargs: dict = dataclasses.field(default_factory=dict)

    def __call__(self) -> Any:
        return self.func(*self.args, **self.kwargs)


class ExecutorActor(actors.AsyncActor):

    async def receiveMsg_ExecuteRequest(self, request: ExecuteRequest, sender: actors.ActorAddress) -> Any:
        return request()
