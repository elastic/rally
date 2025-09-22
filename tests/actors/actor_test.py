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
import uuid
from collections.abc import Generator
from typing import Any, Literal, get_args

import pytest

from esrally import actors, config, types
from esrally.actors import _actor, _context


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
    system = actors.init_system()
    yield system
    actors.shutdown()


@pytest.fixture(scope="function")
def parent_actor() -> Generator[actors.ActorAddress]:
    address = actors.create(DummyActor)
    yield address
    actors.send(address, actors.ActorExitRequest())


@pytest.fixture(scope="function")
def children_actors(parent_actor: actors.ActorAddress) -> Generator[list[actors.ActorAddress]]:
    tasks = [actors.request(parent_actor, CreateChildRequest()) for _ in range(3)]
    children = asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
    yield children
    for child in children:
        actors.send(child, actors.ActorExitRequest())


@pytest.mark.asyncio
async def test_actor_context(parent_actor: actors.ActorAddress) -> None:
    await actors.request(parent_actor, CheckActorContextRequest())


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
async def test_blocking_task(parent_actor: actors.ActorAddress) -> None:
    """This verifies a blocking task would not block the actor from processing further messages."""
    actors.send(parent_actor, BlockingRequest(timeout=300.0))
    ping = PingRequest()
    response = await actors.request(parent_actor, ping)
    assert response == PongResponse(ping)


@pytest.mark.asyncio
async def test_cancel_request(parent_actor: actors.ActorAddress) -> None:
    blocking = actors.request(parent_actor, BlockingRequest(timeout=300.0))
    blocking.cancel()

    ping = PingRequest()
    response = await actors.request(parent_actor, ping)
    assert response == PongResponse(ping)


@pytest.mark.asyncio
async def test_parent_config(parent_actor: actors.ActorAddress) -> None:
    assert config.get_config() == await actors.request(
        parent_actor, GetConfigRequest()
    ), "Parent actor received configuration from system context."


@pytest.mark.asyncio
async def test_children_config(children_actors: list[actors.ActorAddress]) -> None:
    for child in children_actors:
        assert config.get_config() == await actors.request(
            child, GetConfigRequest()
        ), "Child actor received configuration from parent context."


SendMethod = Literal["send", "request"]


@pytest.fixture(scope="function", params=get_args(SendMethod))
def send_method(request) -> Generator[SendMethod]:
    yield request.param


@pytest.mark.asyncio
async def test_send(parent_actor: actors.ActorAddress, children_actors: list[actors.ActorAddress], send_method: SendMethod) -> None:
    requests = [PingRequest(destination=child, send_method=send_method) for child in children_actors]
    responses = await asyncio.gather(*[actors.request(parent_actor, r) for r in requests])
    assert responses == [PongResponse(r) for r in requests]


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


@dataclasses.dataclass
class PingRequest:
    destination: actors.ActorAddress | None = None
    send_method: SendMethod = "send"
    uuid: str = dataclasses.field(default_factory=uuid.uuid4)


@dataclasses.dataclass
class PongResponse:
    ping: PingRequest


class DummyActor(actors.AsyncActor):

    def __init__(self) -> None:
        super().__init__()
        self.pending_pongs: dict[str, asyncio.Future[PongResponse]] = {}

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
            ctx = _context.get_context()
            assert isinstance(ctx, _actor.ActorRequestContext), "Actor request context initialized."
            assert not ctx.responded, "Actor responded flag not set."
            actors.respond(status=request.status, error=request.error)
            assert ctx.responded, "Actor responded flag set."
        elif request.error is not None:
            raise request.error
        else:
            return request.status

    def receiveMsg_CreateChildRequest(self, message: GetConfigRequest, sender: actors.ActorAddress) -> actors.ActorAddress:
        return actors.create(DummyActor)

    def receiveMsg_GetConfigRequest(self, message: GetConfigRequest, sender: actors.ActorAddress) -> types.Config:
        return config.get_config()

    async def receiveMsg_PingRequest(self, request: PingRequest, sender: actors.ActorAddress) -> PongResponse:
        if request.destination in [None, self.myAddress]:
            return PongResponse(request)

        if request.send_method == "request":
            return await actors.request(request.destination, request)

        assert request.send_method == "send"
        future: asyncio.Future[PongResponse] = self.create_future()
        self.pending_pongs[request.uuid] = future
        actors.send(request.destination, request)
        return await future

    async def receiveMsg_BlockingRequest(self, request: BlockingRequest, sender: actors.ActorAddress) -> None:
        if request.destination in [None, self.myAddress]:
            await asyncio.sleep(request.timeout)
            return
        return await actors.request(request.destination, request)

    async def receiveMsg_PongResponse(self, response: PongResponse, sender: actors.ActorAddress) -> None:
        self.pending_pongs[response.ping.uuid].set_result(response)
