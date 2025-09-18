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


@dataclasses.dataclass
class ResponseMessage:
    req_id: str

    @classmethod
    def from_future(cls, req_id: str, future: asyncio.Future) -> ResponseMessage:
        try:
            result = future.result()
            if result is None:
                return ResponseMessage(req_id)
            return ResultMessage(req_id, result)
        except Exception as error:
            return ErrorMessage(req_id, error)

    def result(self) -> Any:
        return None


@dataclasses.dataclass
class ResultMessage(ResponseMessage):
    _result: Any

    def result(self) -> Any:
        return self._result


@dataclasses.dataclass
class ErrorMessage(ResponseMessage):
    _error: Exception

    def result(self) -> Any:
        raise self._error


class PoisonError(RuntimeError):
    pass
