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

from esrally import actors, config


@pytest.fixture(scope="function", autouse=True)
def event_loop() -> Generator[asyncio.AbstractEventLoop, Any, None]:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture(scope="function", autouse=True, params=get_args(actors.SystemBase))
def system(request) -> Generator[actors.ActorSystem, None, None]:
    cfg = actors.ActorConfig()
    cfg.system_base = request.param
    config.init_config(cfg)
    system = actors.init_system()
    yield system
    actors.shutdown()
    config.clear_config()


def test_actor(system: actors.ActorSystem, event_loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        assert actors.get_actor()

    address = actors.create(SomeActor)
    with pytest.raises(TypeError):
        assert actors.get_actor()

    value = random.random()
    future = actors.request(address, SomeMessage(value))
    event_loop.run_until_complete(future)
    assert future.result() == value

    with pytest.raises(TypeError):
        assert actors.get_actor()


class SomeActor(actors.AsyncActor):

    async def receiveMsg_SomeMessage(self, message: SomeMessage, sender: actors.ActorAddress) -> Any:
        assert actors.get_actor() is self
        assert asyncio.get_event_loop() is self.loop, "loop not set"

        destination = actors.create(RepeatActor)

        values = [random.random() for _ in range(10)]
        futures = [actors.request(destination, RepeatMessage(v)) for v in values]

        got = list(await asyncio.gather(*futures))
        assert got == values

        return message.return_value


@dataclasses.dataclass
class SomeMessage:
    return_value: Any


@dataclasses.dataclass
class RepeatMessage:
    message: Any


class RepeatActor(actors.AsyncActor):

    def receiveMsg_RepeatMessage(self, repeat: RepeatMessage, sender: actors.ActorAddress) -> None:
        actors.send_result(repeat.message)
