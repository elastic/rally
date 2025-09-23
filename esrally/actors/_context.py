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
import logging
from collections.abc import Generator
from typing import Any, Optional, Protocol, TypeVar, runtime_checkable

from thespian import actors  # type: ignore[import-untyped]

from esrally import types
from esrally.actors._proto import PingRequest

LOG = logging.getLogger(__name__)


CONTEXT = contextvars.ContextVar[Optional["ActorContext"]]("actors.context", default=None)


def create(cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None) -> actors.ActorAddress:
    return get_context().create(cls, requirements=requirements, cfg=cfg)


def send(destination: actors.ActorAddress, message: Any) -> None:
    get_context().send(destination, message)


def request(destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
    return get_context().request(destination, message, timeout=timeout)


def ping(destination: actors.ActorAddress, *, message: Any = None, timeout: float | None = None) -> asyncio.Future[Any]:
    return get_context().request(destination, PingRequest(message=message), timeout=timeout)


def shutdown() -> None:
    try:
        ctx = get_context()
    except ActorContextError:
        return
    set_context(None)
    ctx.shutdown()


class ActorContextError(RuntimeError):
    pass


def get_context() -> ActorContext:
    ctx = CONTEXT.get()
    if not ctx:
        raise ActorContextError("No actor context set.")
    return ctx


def set_context(ctx: ActorContext | None) -> None:
    assert ctx is None or isinstance(ctx, ActorContext)
    CONTEXT.set(ctx)


C = TypeVar("C", bound="ActorContext")


@contextlib.contextmanager
def enter_context(ctx: C) -> Generator[C]:
    token = CONTEXT.set(ctx)
    try:
        yield ctx
    finally:
        CONTEXT.reset(token)


@runtime_checkable
class ActorContext(Protocol):

    def create(
        self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None
    ) -> actors.ActorAddress:
        raise NotImplementedError

    def send(self, destination: actors.ActorAddress, message: Any) -> None:
        raise NotImplementedError

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError
