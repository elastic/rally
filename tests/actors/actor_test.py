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
import random
from collections.abc import Generator
from typing import Any, get_args

import pytest

from esrally import actors, config, types
from esrally.actors import _actor, _context, _proto


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
    config.init_config(cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="function", autouse=True)
def system() -> Generator[actors.ActorSystem]:
    system = actors.init_actor_system()
    yield system
    actors.shutdown()


@pytest.fixture(scope="function")
def parent_actor() -> Generator[actors.ActorAddress]:
    address = actors.create_actor(DummyActor)
    yield address
    actors.send(address, actors.ActorExitRequest())


@pytest.fixture(scope="function")
def child_actor(parent_actor: actors.ActorAddress) -> Generator[actors.ActorAddress]:
    child = asyncio.get_event_loop().run_until_complete(actors.request(parent_actor, CreateChildRequest()))
    yield child
    actors.send(child, actors.ActorExitRequest())


@pytest.mark.asyncio
async def test_actor_context(parent_actor: actors.ActorAddress) -> None:
    await actors.request(parent_actor, CheckActorContextRequest())


@pytest.mark.asyncio
async def test_child_actor_context(child_actor: actors.ActorAddress) -> None:
    await actors.request(child_actor, CheckActorContextRequest())


@pytest.mark.asyncio
async def test_return_result(parent_actor: actors.ActorAddress) -> None:
    result = random.random()
    assert result == await actors.request(parent_actor, ResponseRequest(result))


@pytest.mark.asyncio
async def test_respond_status(parent_actor: actors.ActorAddress) -> None:
    result = random.random()
    assert result == await actors.request(parent_actor, ResponseRequest(result, explicit=True))


@pytest.mark.asyncio
async def test_raises_error(parent_actor: actors.ActorAddress) -> None:
    error = RuntimeError(f"some ramdom error: {random.random()}")
    with pytest.raises(RuntimeError):
        await actors.request(parent_actor, ResponseRequest(error=error))


@pytest.mark.asyncio
async def test_respond_error(parent_actor: actors.ActorAddress) -> None:
    error = ValueError(f"some ramdom error: {random.random()}")
    with pytest.raises(ValueError):
        await actors.request(parent_actor, ResponseRequest(error=error, explicit=True))


@pytest.mark.asyncio
async def test_timeout_request(parent_actor: actors.ActorAddress) -> None:
    """This verifies a blocking task would not block the actor from processing further messages."""
    future = actors.request(parent_actor, BlockingRequest(timeout=10.0), timeout=0.2)
    value = random.random()
    assert value == await actors.ping(parent_actor, message=value)
    with pytest.raises(TimeoutError):
        await future


@pytest.mark.asyncio
async def test_timeout_nested_request(parent_actor: actors.ActorAddress, child_actor: actors.ActorAddress) -> None:
    """This verifies a blocking task would not block the actor from processing further messages."""
    future = actors.request(parent_actor, BlockingRequest(timeout=10.0, destination=child_actor), timeout=0.2)
    value = random.random()
    assert value == await actors.ping(parent_actor, message=value)
    with pytest.raises(TimeoutError):
        await future


@pytest.mark.asyncio
async def test_cancel_request(parent_actor: actors.ActorAddress) -> None:
    blocking = actors.request(parent_actor, BlockingRequest(timeout=300.0))
    blocking.cancel("some reason")
    value = random.random()
    assert value == await actors.ping(parent_actor, message=value)


@pytest.mark.asyncio
async def test_blocking_request(parent_actor: actors.ActorAddress) -> None:
    blocking = actors.request(parent_actor, BlockingRequest(timeout=300.0))
    value = random.random()
    assert value == await actors.ping(parent_actor, message=value)
    with pytest.raises(TimeoutError):
        await actors.wait_for(blocking, timeout=0.1, cancel_message="too much time!")


@pytest.mark.asyncio
async def test_parent_config(parent_actor: actors.ActorAddress) -> None:
    assert config.get_config() == await actors.request(
        parent_actor, GetConfigRequest()
    ), "Parent actor received configuration from system context."


@pytest.mark.asyncio
async def test_child_config(child_actor: actors.ActorAddress) -> None:
    assert config.get_config() == await actors.request(
        child_actor, GetConfigRequest()
    ), "Child actor received configuration from parent context."


@pytest.mark.asyncio
async def test_request(parent_actor: actors.ActorAddress, child_actor: actors.ActorAddress) -> None:
    request = _proto.PingRequest(destination=child_actor, message=random.random())
    response = await actors.request(parent_actor, request)
    assert response == request.message


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

    def receiveMsg_CheckActorContext(self, request: CheckActorContextRequest) -> None:
        ctx = _context.get_context()
        assert isinstance(ctx, _context.ActorContext), "Actor context initialized."
        assert isinstance(ctx, _actor.ActorRequestContext), "Actor request context initialized."
        assert ctx.actor is self, "Actor request context initialized."
        assert ctx.req_id, "Actor request ID set."
        assert ctx.sender, "Actor request sender is set."
        assert not ctx.responded, "Actor request is not responded."
        assert actors.get_actor() is self, "Actor context initialized."

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
