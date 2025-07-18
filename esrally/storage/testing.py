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
from collections.abc import Iterable

from esrally.storage._adapter import Adapter, Head, Writable
from esrally.storage._range import NO_RANGE, RangeSet


class DummyAdapter(Adapter):

    HEADS: Iterable[Head] = tuple()
    DATA: dict[str, bytes] = {}

    @classmethod
    def match_url(cls, url: str) -> str:
        return url

    def __init__(self, heads: Iterable[Head] | None = None, data: dict[str, bytes] | None = None) -> None:
        if heads is None:
            heads = self.HEADS
        if data is None:
            data = self.DATA
        self.heads: dict[str, Head] = {h.url: h for h in heads if h.url is not None}
        self.data: dict[str, bytes] = copy.deepcopy(data)

    def head(self, url: str) -> Head:
        try:
            return copy.copy(self.heads[url])
        except KeyError:
            raise FileNotFoundError from None

    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        ranges: RangeSet = NO_RANGE
        if head is not None:
            ranges = head.ranges
            if len(ranges) > 1:
                raise NotImplementedError("len(head.ranges) > 1")
        data = self.data[url]
        if ranges:
            stream.write(data[ranges.start : ranges.end])
            return Head(url, content_length=ranges.size, ranges=ranges, document_length=len(data))

        stream.write(data)
        return Head(url, content_length=len(data))
