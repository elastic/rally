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
"""
Fakes for Rally's actors so that unit tests can exercise actor code without a Ray runtime.

Rally's actor classes are plain Python classes (see ``esrally.actor``). Tests instantiate them directly and use
``FakeHandle`` instead of Ray actor handles.
"""

import asyncio
import concurrent.futures
import dataclasses
from typing import Any

import pytest

from esrally import actor


class FakeObjectRef:
    """
    Stands in for ``ray.ObjectRef``: the result of calling ``handle.method.remote()``. It can be awaited.
    """

    def __init__(self, result: Any = None, exception: BaseException | None = None, pending: bool = False):
        self._future: concurrent.futures.Future = concurrent.futures.Future()
        if pending:
            return
        if exception is not None:
            self._future.set_exception(exception)
        else:
            self._future.set_result(result)

    def future(self) -> concurrent.futures.Future:
        return self._future

    def __await__(self):
        return asyncio.wrap_future(self._future).__await__()


class _FakeMethod:
    def __init__(self, handle: "FakeHandle", name: str):
        self._handle = handle
        self._name = name

    def remote(self, *args: Any, **kwargs: Any) -> FakeObjectRef:
        self._handle.calls.append((self._name, args, kwargs))
        behavior = self._handle.behaviors.get(self._name)
        if behavior is None:
            return FakeObjectRef()
        if isinstance(behavior, FakeObjectRef):
            return behavior
        if isinstance(behavior, BaseException):
            return FakeObjectRef(exception=behavior)
        if callable(behavior):
            try:
                result = behavior(*args, **kwargs)
            except BaseException as e:  # pylint: disable=broad-exception-caught
                return FakeObjectRef(exception=e)
            return result if isinstance(result, FakeObjectRef) else FakeObjectRef(result=result)
        return FakeObjectRef(result=behavior)


class FakeHandle:
    """
    Stands in for a Ray actor handle. Records all method calls in ``calls``.

    ``behaviors`` maps method names to what a call returns: a value, an exception to raise, a ``FakeObjectRef``, or a
    callable that computes the result from the call's arguments.
    """

    def __init__(self, name: str = "actor", behaviors: dict[str, Any] | None = None):
        self.name = name
        self.behaviors: dict[str, Any] = dict(behaviors or {})
        self.calls: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name: str) -> _FakeMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        return _FakeMethod(self, name)

    def calls_to(self, method: str) -> list[tuple[tuple, dict]]:
        return [(args, kwargs) for name, args, kwargs in self.calls if name == method]

    def __repr__(self) -> str:
        return f"FakeHandle({self.name})"


@dataclasses.dataclass
class CreatedActor:
    cls: type
    args: tuple
    kwargs: dict
    handle: FakeHandle


class FakeRay:
    """
    Replaces the functions of ``esrally.actor`` that talk to Ray.
    """

    def __init__(self):
        self.created: list[CreatedActor] = []
        self.killed: list[FakeHandle] = []
        self.required_hosts: list[str] = []
        # maps actor classes to behaviors of the handles that are created for them
        self.behaviors: dict[type, dict[str, Any]] = {}

    def create_actor(self, cls: type, *args: Any, **kwargs: Any) -> FakeHandle:
        handle = FakeHandle(kwargs.get("name") or cls.__name__, self.behaviors.get(cls))
        self.created.append(CreatedActor(cls, args, kwargs, handle))
        return handle

    def kill_actor(self, handle: FakeHandle) -> None:
        self.killed.append(handle)

    async def require_node_async(self, host: str, timeout: float = 0) -> None:
        self.required_hosts.append(host)

    def created_of(self, cls: type) -> list[CreatedActor]:
        return [c for c in self.created if c.cls is cls]


@pytest.fixture
def fake_ray(monkeypatch: pytest.MonkeyPatch) -> FakeRay:
    fake = FakeRay()
    monkeypatch.setattr(actor, "create_actor", fake.create_actor)
    monkeypatch.setattr(actor, "kill_actor", fake.kill_actor)
    monkeypatch.setattr(actor, "require_node_async", fake.require_node_async)
    monkeypatch.setattr(actor, "LOG_FORWARDING_GRACE_PERIOD", 0)
    return fake


@pytest.fixture
def actor_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Allows to instantiate actor classes in tests: they would otherwise configure logging and the console of this process.
    """
    monkeypatch.setattr(actor.log, "configure_actor_logging", lambda name: None)
    monkeypatch.setattr(actor.console, "init", lambda **kwargs: None)


def set_self_handle(instance: Any, handle: FakeHandle | None = None) -> FakeHandle:
    """
    Sets the handle that an actor instance passes to actors that it creates (``RallyActorBase.self_handle``).
    """
    handle = handle or FakeHandle(type(instance).__name__)
    instance.__dict__["self_handle"] = handle
    return handle
