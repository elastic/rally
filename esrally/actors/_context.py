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
from typing import Any, Optional, Protocol, runtime_checkable

from thespian import actors  # type: ignore[import-untyped]

from esrally import types
from esrally.actors._config import ActorConfig

LOG = logging.getLogger(__name__)


CONTEXT = contextvars.ContextVar[Optional["Context"]]("actors.context", default=None)


def create(cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None, cfg: types.Config | None = None) -> actors.ActorAddress:
    ctx = get_context()
    address = ctx.create(cls, requirements=requirements)
    if hasattr(cls, "receiveMsg_ActorConfig"):
        cfg = ActorConfig.from_config(cfg)
        send(address, cfg)
    return address


def send(destination: actors.ActorAddress, message: Any) -> None:
    get_context().send(destination, message)


def request(destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
    return get_context().request(destination, message, timeout=timeout)


def shutdown() -> None:
    try:
        ctx = get_context()
    except ContextError:
        return
    set_context(None)
    ctx.shutdown()


class ContextError(RuntimeError):
    pass


def get_context() -> Context:
    ctx = CONTEXT.get()
    if not ctx:
        raise ContextError("No actor context set.")
    return ctx


def set_context(ctx: Context | None) -> None:
    assert ctx is None or isinstance(ctx, Context)
    CONTEXT.set(ctx)


@contextlib.contextmanager
def enter_context(ctx: Context | None) -> Generator[None]:
    token = CONTEXT.set(ctx)
    try:
        yield
    finally:
        CONTEXT.reset(token)


@runtime_checkable
class Context(Protocol):

    def create(self, cls: type[actors.Actor], *, requirements: dict[str, Any] | None = None) -> actors.ActorAddress:
        raise NotImplementedError

    def send(self, destination: actors.ActorAddress, message: Any) -> None:
        raise NotImplementedError

    def request(self, destination: actors.ActorAddress, message: Any, *, timeout: float | None = None) -> asyncio.Future[Any]:
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError
