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
import logging
from collections.abc import Generator

import pytest

from esrally import actors, config

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def actor_system() -> Generator[actors.ActorSystem]:
    config.init_config(config.Config())
    system = actors.init_actor_system()
    LOG.info("Actor system initialized: capabilities {%s}", system.capabilities)

    yield system

    actors.shutdown()
    LOG.info("Actor system shut down.")


@pytest.mark.asyncio
async def test_example():
    """It creates an actor, it sends it a request, and it finally waits for a response."""

    parent = actors.create_actor(ParentActor)

    response = await actors.request(parent, CreateChildrenRequest(["qui", "quo", "qua"]))

    assert response == "All OK"


@dataclasses.dataclass
class CreateChildrenRequest:
    names: list[str]


@dataclasses.dataclass
class AssignNameRequest:
    name: str


@dataclasses.dataclass
class AskNameRequest:
    pass


@dataclasses.dataclass
class GreetSiblingRequest:
    sibling: dict[str, actors.ActorAddress]


@dataclasses.dataclass
class GreetMessage:
    sender: str
    receiver: str


class ParentActor(actors.AsyncActor):

    async def receiveMsg_CreateChildrenRequest(self, request: CreateChildrenRequest, sender: actors.ActorAddress) -> str:
        """It creates some child actors, teach each one its name, and it verifies if he learned it.

        It performs the operations for every child actor in parallel, without blocking the actor before returning the final message.

        The purpose is to demonstrate how simple become implementing a workflow involving multiple actors interactions
        with Async actor class without falling in the complexity of keeping the workflow state in actors variables.

        :param request:
        :param sender:
        :return:
        """
        # Create the children actors and map them by name.
        children = {name: actors.create_actor(ChildActor) for name in request.names}

        # It assigns each of them a name. I don't actually really need to wait for request to complete.
        for name, child in children.items():
            actors.send(child, AssignNameRequest(name))

        # It verifies all of them learned their names.
        assert request.names == await asyncio.gather(*[actors.request(child, AskNameRequest()) for child in children.values()])

        # It says each child to greet his sibling (including itself) and tell what they hear.
        assert await asyncio.gather(*[actors.request(child, GreetSiblingRequest(children)) for child in children.values()]) == [
            [f"Hello {name2}, it's {name1}" for name1 in children] for name2 in children
        ]

        return "All OK"


class ChildActor(actors.AsyncActor):
    def __int__(self):
        super().__init__()
        self.name: str | None = None

    def receiveMsg_AssignNameRequest(self, request: AssignNameRequest, sender: actors.ActorAddress) -> None:
        """It receives the actor name."""
        self.name = request.name

    def receiveMsg_AskNameRequest(self, request: AskNameRequest, sender: actors.ActorAddress) -> str:
        """It gets the actor name."""
        assert self.name is not None
        return self.name

    def receiveMsg_GreetSiblingRequest(self, request: GreetSiblingRequest, sender: actors.ActorAddress) -> asyncio.Future[list[str]]:
        """It GreetMessage to all sibling actors (including itself)."""
        # This demonstrates an async task will be created to gather all the answers from the requests sent here.
        # No actor will be blocked waiting at any time while this is in execution.
        assert isinstance(self.name, str)
        return asyncio.gather(
            *[actors.request(sibling, GreetMessage(sender=self.name, receiver=name)) for name, sibling in request.sibling.items()]
        )

    def receiveMsg_GreetMessage(self, message: GreetMessage, sender: actors.ActorAddress) -> str:
        assert self.name == message.receiver
        return f"Hello {message.sender}, it's {self.name}"
