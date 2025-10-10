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
import asyncio.exceptions
import copy
import dataclasses
import time
import traceback
from typing import Any

from thespian import actors  # type: ignore[import-untyped]
from typing_extensions import Self

from esrally.actors._config import ActorConfig


@dataclasses.dataclass
class Request:
    req_id: str = ""
    deadline: float | None = None

    @classmethod
    def from_message(cls, message: Any, *, timeout: float | None = None, req_id: str | None = None) -> Self | "MessageRequest":
        deadline: float | None = None
        if timeout is not None:
            deadline = max(0.0, time.monotonic() + timeout)
        if req_id is None:
            req_id = ""
        req_id = req_id.strip()
        if not isinstance(message, cls):
            return MessageRequest(message=message, deadline=deadline, req_id=req_id)
        if message.deadline != deadline or message.req_id != req_id:
            message = copy.deepcopy(message)
            message.deadline = deadline
            message.req_id = req_id
        return message

    @property
    def timeout(self) -> float | None:
        if self.deadline is None:
            return None
        return self.deadline - time.monotonic()


@dataclasses.dataclass
class MessageRequest(Request):
    """Message envelope sent to async actors requiring a response.

    As soon the actor receive this request it sends a PendingResponse with the same req_id to notify it.
    Then after processing carried message, it will send one of the following responses:
    - DoneResponse: in case the message processing succeeded without any resulting status.
    - StatusResponse: in case the message processing succeeded with a non-None resulting status.
    - ErrorResponse: in case the message processing failed with an exception message.
    All response messages send by target actor will have the same req_id as this message.
    """

    message: Any = None


@dataclasses.dataclass
class ActorInitRequest(MessageRequest):
    """It is sent by create_actor function just after an actor accepting ActorInitRequest is created."""

    cfg: ActorConfig = dataclasses.field(default_factory=ActorConfig.from_config)


@dataclasses.dataclass
class CancelRequest(MessageRequest):
    """It is sent to an actor to cancel all pending asynchronous tasks started by a request with the same req_id."""


@dataclasses.dataclass
class Response:
    """It is sent by the actor to notify sender has just finished processing a request."""

    # req_id is required to match the request.
    req_id: str

    @classmethod
    def from_status(cls, req_id: str, status: Any = None, error: Exception | None = None) -> "Response":
        """Given a status and an error it will return a Response of one of the following types:
        - ErrorResponse: in case error is not None
        - StatusResponse: in case error is None and status is not None
        - DoneResponse: in case error and status are both None
        """
        if error is not None:
            return ErrorResponse(req_id, error, traceback.format_exc())
        if isinstance(status, cls):
            if req_id != status.req_id:
                status = copy.deepcopy(status)
                status.req_id = req_id
            return status
        if status is None:
            return DoneResponse(req_id)
        return ResultResponse(req_id, status)


@dataclasses.dataclass
class RunningTaskResponse(Response):
    """It is sent by actor after a background task is created as a result of processing a request."""

    name: str


class DoneResponse(Response):

    def result(self) -> Any:
        return None


@dataclasses.dataclass
class ResultResponse(DoneResponse):
    """It is sent by the actor to notify sender has just finished processing a response with a non-none resulting status."""

    status: Any

    def result(self) -> Any:
        return self.status


@dataclasses.dataclass
class CancelledResponse(ResultResponse):
    """CancelledResponse is sent by the actor to notify sender that the request has been cancelled."""

    def result(self) -> Any:
        raise asyncio.exceptions.CancelledError(self.status is not None and str(self.status) or "")


@dataclasses.dataclass
class ErrorResponse(DoneResponse):
    """It is sent by the actor to notify sender has just finished processing a response raising an Exception."""

    error: Exception
    details: str

    def result(self) -> Any:
        cause: BaseException | None = None
        if self.details:
            # Attach error details (which it should include original error traceback) as a cause of the error,
            # so that both stack traces (local and remote) will be visible when logging the exception.
            message = f"{self.error}\n{self.details}"
            cause = type(self.error)(message)
        raise self.error from cause


class ActorRequestError(Exception):
    pass


class PoisonError(ActorRequestError):
    """It is used to translate a PoisonMessage to an Exception instance."""

    @classmethod
    def from_poison_message(cls, poison: actors.PoisonMessage) -> Self:
        return cls(f"poison message: {poison.poisonMessage!r}, details:\n{poison.details!r}")


@dataclasses.dataclass
class PingRequest(MessageRequest):
    destination: actors.ActorAddress | None = None


@dataclasses.dataclass
class PongResponse(ResultResponse):
    pass
