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

import dataclasses
import time
import uuid
from typing import Any


@dataclasses.dataclass
class RequestMessage:

    message: Any
    req_id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    deadline: float | None = None

    @classmethod
    def from_message(cls, message: Any, *, timeout: float | None = None) -> RequestMessage:
        if isinstance(message, RequestMessage):
            return message
        deadline: float | None = None
        if timeout is not None:
            deadline = max(0.0, time.monotonic() + timeout)
        return cls(message=message, deadline=deadline)


def response_from_status(req_id: str, status: Any = None, error: Exception | None = None) -> Any:
    if error is not None:
        return ErrorMessage(req_id, status, error)
    if status is not None:
        return StatusMessage(req_id, status)
    return ResponseMessage(req_id)


@dataclasses.dataclass
class ResponseMessage:
    req_id: str

    def result(self) -> Any:
        return None


@dataclasses.dataclass
class StatusMessage(ResponseMessage):
    status: Any

    def result(self) -> Any:
        return self.status


@dataclasses.dataclass
class ErrorMessage(StatusMessage):
    error: Exception

    def result(self) -> Any:
        raise self.error


class PoisonError(RuntimeError):
    pass
