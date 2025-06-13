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

from dataclasses import dataclass
from unittest.mock import create_autospec

import pytest

from esrally.config import Config, Scope
from esrally.storage._adapter import Adapter, Head, Writable
from esrally.storage._client import MAX_CONNECTIONS, Client
from esrally.storage._range import NO_RANGE, RangeSet, rangeset
from esrally.types import Key
from esrally.utils.cases import cases

URL = "https://example.com"
HEAD = Head(url=URL, content_length=20, accept_ranges=True)
DATA = b"<!doctype html>\n<html>\n<head>\n"


class HTTPSAdapter(Adapter):

    __adapter_prefixes__ = ("https:",)

    def __init__(self):
        super().__init__()
        self._head = HEAD
        self._data = DATA

    def head(self, url: str) -> Head:
        return self._head

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        if ranges:
            stream.write(self._data)
            return Head(url, content_length=len(self._data), accept_ranges=True, ranges=ranges)
        else:
            return Head(url, content_length=648, accept_ranges=True)


@pytest.fixture
def cfg() -> Config:
    cfg = Config()
    cfg.add(Scope.application, "storage", "storage.adapters", f"{__name__}:HTTPSAdapter")
    return cfg


@pytest.fixture
def client(cfg: Config) -> Client:
    return Client.from_config(cfg)


@dataclass()
class HeadCase:
    url: str
    want: Head
    ttl: float | None = None


@cases(default=HeadCase(URL, HEAD), ttl=HeadCase(URL, HEAD, ttl=300.0))
def test_head(case: HeadCase, client: Client) -> None:
    got = client.head(url=case.url, ttl=case.ttl)
    assert got == case.want
    if case.ttl is not None:
        assert got is client.head(url=case.url, ttl=case.ttl)


@dataclass()
class GetCase:
    url: str
    want: Head
    ranges: RangeSet = NO_RANGE
    want_data: bytes | None = None


@cases(
    regular=GetCase(url=URL, want=Head(URL, 648, True)),
    range=GetCase(URL, ranges=rangeset("0-29"), want=Head(URL, 30, True, rangeset("0-29")), want_data=DATA),
)
def test_get(case: GetCase, client: Client) -> None:
    stream = create_autospec(Writable, spec_set=True, instance=True)
    got = client.get(case.url, stream, case.ranges)
    assert got == case.want
    if case.want_data is not None:
        stream.write.assert_called_once_with(case.want_data)


@dataclass()
class FromConfigCase:
    opts: dict[Key, str]
    want_max_connections: int = MAX_CONNECTIONS


@cases(
    default=FromConfigCase({}),
    max_connections=FromConfigCase({"storage.max_connections": "2"}, want_max_connections=2),
)
def test_from_config(case: FromConfigCase) -> None:
    cfg = Config()
    for k, v in case.opts.items():
        cfg.add(Scope.application, "storage", k, v)
    client = Client.from_config(cfg)
    assert isinstance(client, Client)
    assert client._connections["some"].max_count == case.want_max_connections  # pylint: disable=protected-access
