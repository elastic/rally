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

import copy
import dataclasses
from collections.abc import Generator

from typing_extensions import Self

from esrally import types
from esrally.storage import NO_RANGE, Adapter, Executor, GetResponse, Head, RangeSet


@dataclasses.dataclass
class DummyAdapter(Adapter):
    heads: dict[str, Head] = dataclasses.field(default_factory=dict)
    data: dict[str, bytes] = dataclasses.field(default_factory=dict)

    @classmethod
    def match_url(cls, url: str) -> bool:
        return True

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        return cls()

    def head(self, url: str) -> Head:
        try:
            return copy.copy(self.heads[url])
        except KeyError:
            raise FileNotFoundError from None

    def get(self, url: str, *, check_head: Head | None = None) -> GetResponse:
        ranges: RangeSet = NO_RANGE
        if check_head is not None:
            ranges = check_head.ranges
            if len(ranges) > 1:
                raise NotImplementedError("len(head.ranges) > 1")

        data = self.data[url]
        if ranges:
            data = data[ranges.start : ranges.end]

        head = Head(url, content_length=ranges.size, ranges=ranges, document_length=len(data))

        def iter_chunks() -> Generator[bytes]:
            yield from (data,)

        return GetResponse(head, iter_chunks())


class DummyExecutor(Executor):

    def __init__(self):
        self.tasks: list[tuple] | None = []

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        if self.tasks is None:
            raise RuntimeError("Executor already closed")
        self.tasks.append((fn, args, kwargs))

    def execute_tasks(self):
        if self.tasks is None:
            raise RuntimeError("Executor already closed")
        tasks, self.tasks = self.tasks, []
        for fn, args, kwargs in tasks:
            fn(*args, **kwargs)

    def shutdown(self):
        self.tasks = None
