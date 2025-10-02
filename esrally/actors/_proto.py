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
import uuid
from typing import Any

from thespian import actors  # type: ignore[import-untyped]
from typing_extensions import Self


@dataclasses.dataclass
class Request:
    """Message envelope sent to async actors requiring a response.

    As soon the actor receive this request it sends a PendingResponse with the same req_id to notify it.
    Then after processing carried message, it will send one of the following responses:
    - DoneResponse: in case the message processing succeeded without any resulting status.
    - StatusResponse: in case the message processing succeeded with a non-None resulting status.
    - ErrorResponse: in case the message processing failed with an exception message.
    All response messages send by target actor will have the same req_id as this message.
    """

    message: Any = None
    deadline: float | None = None
    req_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

    @classmethod
    def from_message(cls, message: Any, *, timeout: float | None = None) -> Self:
        deadline: float | None = None
        if timeout is not None:
            deadline = max(0.0, time.monotonic() + timeout)
        if isinstance(message, cls):
            if deadline is not None:
                if message.deadline is None or deadline < message.deadline:
                    message = copy.deepcopy(message)
                    message.deadline = deadline
            return message
        return cls(message=message, deadline=deadline)

    @property
    def timeout(self) -> float | None:
        if self.deadline is None:
            return None
        return self.deadline - time.monotonic()


@dataclasses.dataclass
class CancelRequest(Request):
    """It is sent to an actor to cancel all pending asynchronous tasks started by a request with the same req_id."""


@dataclasses.dataclass
class Response:
    """It is sent by the actor to notify sender has just finished processing a request."""

    # req_id is required to match the request.
    req_id: str


@dataclasses.dataclass
class RunningTaskResponse(Response):
    """It is sent by actor after a background task is created as a result of processing a request."""

    name: str


class ResultResponse(Response):

    @classmethod
    def from_status(cls, req_id: str, status: Any = None, error: Exception | None = None) -> "ResultResponse":
        """Given a status and an error it will return a Response of one of the following types:
        - ErrorResponse: in case error is not None
        - StatusResponse: in case error is None and status is not None
        - DoneResponse: in case error and status are both None
        """
        if error is not None:
            return ErrorResponse(req_id, error=error, details=traceback.format_exc())
        if isinstance(status, cls):
            if req_id != status.req_id:
                status = copy.deepcopy(status)
                status.req_id = req_id
            return status
        if status is not None:
            return StatusResponse(req_id, status)
        return ResultResponse(req_id)

    def result(self) -> Any:
        return None


@dataclasses.dataclass
class CancelledResponse(ResultResponse):
    """CancelledResponse is sent by the actor to notify sender that the request has been cancelled."""

    message: Any = None

    def result(self) -> Any:
        raise asyncio.exceptions.CancelledError(self.message is not None and str(self.message) or "")


@dataclasses.dataclass
class StatusResponse(ResultResponse):
    """It is sent by the actor to notify sender has just finished processing a response with a non-none resulting status."""

    status: Any

    def result(self) -> Any:
        return self.status


@dataclasses.dataclass
class ErrorResponse(ResultResponse):
    """It is sent by the actor to notify sender has just finished processing a response raising an Exception."""

    error: Exception
    details: str

    def result(self) -> Any:
        raise self.error


class ActorRequestError(Exception):
    pass


class PoisonError(ActorRequestError):
    """It is used to translate a PoisonMessage to an Exception instance."""

    @classmethod
    def from_poison_message(cls, poison: actors.PoisonMessage) -> Self:
        return cls(f"poison message: {poison.poisonMessage!r}, details:\n{poison.details!r}")


@dataclasses.dataclass
class PingRequest(Request):
    destination: actors.ActorAddress | None = None


@dataclasses.dataclass
class PongResponse(StatusResponse):
    pass
