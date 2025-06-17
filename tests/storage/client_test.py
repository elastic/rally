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

import os
import random
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
DATA = b"<!doctype html>\n<html>\n<head>\n"
HEAD = Head(url=URL, content_length=len(DATA), accept_ranges=True)

MIRROR_FILES = os.path.join(os.path.dirname(__file__), "mirrors.json")
MIRRORING_URL = "https://rally-tracks.elastic.co/apm/span.json.bz2"
MIRRORED_URL = "https://rally-tracks-eu-central-1.s3.eu-central-1.amazonaws.com/apm/span.json.bz2"
NOT_FOUND_URL = "https://rally-tracks-us-west-1.s3.us-west-1.amazonaws.com/apm/span.json.bz2"
NO_RANGES_URL = f"{MIRRORING_URL}/no/ranges.json.bz2"


class HTTPSAdapter(Adapter):

    __adapter_URL_prefixes__ = ("https:",)

    def __init__(self):
        super().__init__()
        self._head = HEAD
        self._data = DATA

    def head(self, url: str) -> Head:
        if url.startswith("https://rally-tracks-us-west-1.s3.us-west-1.amazonaws.com/"):
            # This simulates the case which a mirror server answers with 404 status
            raise FileNotFoundError
        accept_ranges = True
        if url.endswith("/no/ranges.json.bz2"):
            accept_ranges = False
        return Head(url=url, content_length=len(self._data), accept_ranges=accept_ranges)

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        if ranges:
            stream.write(self._data)
            return Head(url, content_length=len(self._data), accept_ranges=True, ranges=ranges)
        else:
            return Head(url, content_length=648, accept_ranges=True)


@pytest.fixture
def cfg() -> Config:
    cfg = Config()
    cfg.add(Scope.application, "storage", "storage.mirrors_files", MIRROR_FILES)
    cfg.add(Scope.application, "storage", "storage.adapters", f"{__name__}:HTTPSAdapter")
    cfg.add(Scope.application, "storage", "storage.random_seed", 42)
    return cfg


@pytest.fixture(scope="function")
def client(cfg: Config) -> Client:
    return Client.from_config(cfg)


@dataclass()
class HeadCase:
    url: str
    want: Head
    ttl: float | None = None


@dataclass()
class FromConfigCase:
    opts: dict[Key, str]
    want_max_connections: int = MAX_CONNECTIONS
    want_random: random.Random | None = None


@cases(
    default=FromConfigCase({}),
    max_connections=FromConfigCase({"storage.max_connections": "2"}, want_max_connections=2),
    random_seed=FromConfigCase({"storage.random_seed": "42"}, want_random=random.Random("42")),
)
def test_from_config(case: FromConfigCase) -> None:
    # pylint: disable=protected-access
    cfg = Config()
    for k, v in case.opts.items():
        cfg.add(Scope.application, "storage", k, v)
    client = Client.from_config(cfg)
    assert isinstance(client, Client)
    assert client._connections["some"].max_count == case.want_max_connections
    if case.want_random is not None:
        assert client._random.random() == case.want_random.random()


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
    mirrors=GetCase(MIRRORING_URL, want=Head(MIRRORED_URL, 648, True)),
)
def test_get(case: GetCase, client: Client) -> None:
    stream = create_autospec(Writable, spec_set=True, instance=True)
    got = client.get(case.url, stream, case.ranges)
    assert got == case.want
    if case.want_data is not None:
        stream.write.assert_called_once_with(case.want_data)


@dataclass()
class ResolveCase:
    url: str
    want: set[Head]
    content_length: int | None = None
    accept_ranges: bool = False
    ttl: float = 60.0


@cases(
    unmirrored=ResolveCase(url=URL, want={Head(URL, 30, True)}),
    mirrored=ResolveCase(url=MIRRORING_URL, want={Head(MIRRORED_URL, 30, True)}),
    content_length=ResolveCase(url=MIRRORING_URL, content_length=30, want={Head(MIRRORED_URL, 30, True)}),
    mismatching_content_length=ResolveCase(url=MIRRORING_URL, content_length=10, want=set()),
    accept_ranges=ResolveCase(url=MIRRORING_URL, accept_ranges=True, want={Head(MIRRORED_URL, 30, True)}),
    reject_ranges=ResolveCase(url=NO_RANGES_URL, accept_ranges=True, want=set()),
    zero_ttl=ResolveCase(url=URL, ttl=0.0, want={Head(URL, 30, True)}),
)
def test_resolve(case: ResolveCase, client: Client) -> None:
    got = set(client.resolve(case.url, content_length=case.content_length, accept_ranges=case.accept_ranges, ttl=case.ttl))
    assert got == case.want, "unexpected resolve result"
    for g in got:
        if case.ttl > 0.0:
            assert g is client.head(url=g.url, ttl=case.ttl), "obtained head wasn't cached"
        else:
            assert g is not client.head(url=g.url, ttl=case.ttl), "obtained head was cached"
