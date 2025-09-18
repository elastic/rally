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

import json
import os
import random
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from os import PathLike
from unittest.mock import create_autospec

import pytest

from esrally.config import Config, Scope
from esrally.storage._adapter import Head, Writable
from esrally.storage._client import CachedHeadError, Client
from esrally.storage._config import DEFAULT_STORAGE_CONFIG
from esrally.storage._range import NO_RANGE, RangeSet, rangeset
from esrally.storage.testing import DummyAdapter
from esrally.types import Key
from esrally.utils.cases import cases

BASE_URL = "https://example.com"

SOME_URL = f"{BASE_URL}/some/file.json"
SOME_BODY = b"<!doctype html>\n<html>\n<head>\n"
SOME_HEAD = Head(url=SOME_URL, content_length=len(SOME_BODY), accept_ranges=True)
NO_RANGES_URL = f"{BASE_URL}/no/ranges.json.bz2"
NO_RANGE_HEAD = Head(url=NO_RANGES_URL, content_length=len(SOME_BODY), accept_ranges=False)
MIRRORING_BASE_URL = f"{BASE_URL}/mirroring"
MIRRORING_URL = f"{MIRRORING_BASE_URL}/apm/span.json.bz2"
MIRRORING_HEAD = Head(url=MIRRORING_URL, content_length=len(SOME_BODY), accept_ranges=True)
MIRRORED_BASE_URL = f"{BASE_URL}/mirrored"
MIRRORED_URL = f"{MIRRORED_BASE_URL}/apm/span.json.bz2"
MIRRORED_HEAD = Head(url=MIRRORED_URL, content_length=len(SOME_BODY), accept_ranges=True)

MIRRORED_NO_RANGE_BASE_URL = f"{BASE_URL}/mirrored-no-range"
MIRRORED_NO_RANGE_URL = f"{MIRRORED_NO_RANGE_BASE_URL}/apm/span.json.bz2"
MIRRORED_NO_RANGE_HEAD = Head(url=MIRRORED_NO_RANGE_URL, content_length=len(SOME_BODY), accept_ranges=False)

NOT_FOUND_BASE_URL = "https://example.com/not-found"

HEADS = (
    SOME_HEAD,
    NO_RANGE_HEAD,
    MIRRORING_HEAD,
    MIRRORED_HEAD,
    MIRRORED_NO_RANGE_HEAD,
)

MIRROR_FILES = os.path.join(os.path.dirname(__file__), "mirrors.json")
MIRRORS = {
    "mirrors": [
        {
            "sources": [MIRRORING_BASE_URL],
            "destinations": [
                MIRRORED_BASE_URL,
                MIRRORED_NO_RANGE_BASE_URL,
                NOT_FOUND_BASE_URL,
            ],
        }
    ]
}


class StorageAdapter(DummyAdapter):
    HEADS = HEADS
    DATA: tuple[str, bytes] = defaultdict(lambda: SOME_BODY)


@pytest.fixture(scope="function")
def mirror_files(tmpdir: PathLike) -> Iterator[str]:
    mirror_file = os.path.join(str(tmpdir), "mirrors.json")
    with open(mirror_file, "w") as fd:
        json.dump(MIRRORS, fd)
    yield mirror_file
    if os.path.exists(mirror_file):
        os.remove(mirror_file)


@pytest.fixture
def cfg(mirror_files: str) -> Config:
    cfg = Config()
    cfg.add(Scope.application, "storage", "storage.mirror_files", mirror_files)
    cfg.add(Scope.application, "storage", "storage.random_seed", 42)
    cfg.add(Scope.application, "storage", "storage.adapters", f"{__name__}:StorageAdapter")
    return cfg


@pytest.fixture(scope="function")
def client(cfg: Config) -> Client:
    return Client.from_config(cfg)


@dataclass()
class FromConfigCase:
    opts: dict[Key, str]
    want_max_connections: int = DEFAULT_STORAGE_CONFIG.max_connections
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


@dataclass()
class HeadCase:
    url: str
    want: Head | None = None
    want_error: type[Exception] | None = None
    ttl: float | None = None
    want_cached: bool | None = None


@cases(
    default=HeadCase(SOME_URL, SOME_HEAD),
    ttl=HeadCase(SOME_URL, SOME_HEAD, ttl=300.0, want_cached=True),
    zero_ttl=HeadCase(SOME_URL, SOME_HEAD, ttl=0.0, want_cached=False),
    negative_ttl=HeadCase(SOME_URL, SOME_HEAD, ttl=-1.0, want_cached=False),
    error=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError),
    error_ttl=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError, ttl=300.0, want_cached=True),
    error_zero_ttl=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError, ttl=0.0, want_cached=False),
)
def test_head(case: HeadCase, client: Client) -> None:
    # pylint: disable=catching-non-exception
    err: Exception | None = None
    want_error = tuple(filter(None, [case.want_error]))
    try:
        got = client.head(url=case.url, ttl=case.ttl)
    except want_error as e:
        got = None
        err = e
    else:
        assert got == case.want
    if case.want_cached is not None:
        try:
            assert (got is client.head(url=case.url, ttl=case.ttl)) == case.want_cached
        except CachedHeadError as ex:
            assert case.want_cached
            assert err is ex.__cause__
        except want_error:
            assert not case.want_cached


@dataclass()
class ResolveCase:
    url: str
    want: list[Head]
    content_length: int | None = None
    accept_ranges: bool | None = None
    ttl: float = 60.0


@cases(
    unmirrored=ResolveCase(url=SOME_URL, want=[SOME_HEAD]),
    mirrored=ResolveCase(url=MIRRORING_URL, want=[MIRRORED_HEAD, MIRRORED_NO_RANGE_HEAD, MIRRORING_HEAD]),
    document_length=ResolveCase(
        url=MIRRORING_URL, content_length=len(SOME_BODY), want=[MIRRORED_HEAD, MIRRORED_NO_RANGE_HEAD, MIRRORING_HEAD]
    ),
    mismatching_document_length=ResolveCase(url=MIRRORING_URL, content_length=10, want=[]),
    accept_ranges=ResolveCase(url=MIRRORING_URL, accept_ranges=True, want=[MIRRORING_HEAD, MIRRORED_HEAD]),
    reject_ranges=ResolveCase(url=NO_RANGES_URL, accept_ranges=True, want=[]),
    zero_ttl=ResolveCase(url=SOME_URL, ttl=0.0, want=[SOME_HEAD]),
)
def test_resolve(case: ResolveCase, client: Client) -> None:
    check = Head(content_length=case.content_length, accept_ranges=case.accept_ranges)
    got = sorted(client.resolve(case.url, want=check, ttl=case.ttl), key=lambda h: str(h.url))
    want = sorted(case.want, key=lambda h: str(h.url))
    assert got == want, "unexpected resolve result"
    for g in got:
        assert g.url is not None, "unexpected resolve result"
        if case.ttl > 0.0:
            assert g is client.head(url=g.url, ttl=case.ttl), "obtained head wasn't cached"
        else:
            assert g is not client.head(url=g.url, ttl=case.ttl), "obtained head was cached"


@dataclass()
class GetCase:
    url: str
    want_any: list[Head]
    ranges: RangeSet = NO_RANGE
    document_length: int = None
    want_data: bytes | None = None


@cases(
    regular=GetCase(
        SOME_URL,
        [Head(url=SOME_URL, content_length=len(SOME_BODY), document_length=len(SOME_BODY))],
        want_data=SOME_BODY,
    ),
    range=GetCase(
        SOME_URL,
        [Head(SOME_URL, content_length=30, accept_ranges=True, ranges=rangeset("0-29"), document_length=len(SOME_BODY))],
        ranges=rangeset("0-29"),
        want_data=SOME_BODY,
    ),
    mirrors=GetCase(
        MIRRORING_URL,
        [
            MIRRORED_HEAD,
            MIRRORED_NO_RANGE_HEAD,
        ],
        want_data=SOME_BODY,
    ),
)
def test_get(case: GetCase, client: Client) -> None:
    stream = create_autospec(Writable, spec_set=True, instance=True)
    got = client.get(case.url, stream, want=Head(ranges=case.ranges, document_length=case.document_length))
    assert [] != check_any(got, case.want_any)
    if case.want_data is not None:
        stream.write.assert_called_once_with(case.want_data)


def check_any(head: Head, any_head: list[Head]) -> list[Head]:
    ret: list[Head] = []
    for h in any_head:
        try:
            h.check(head)
        except ValueError:
            continue
        ret.append(h)
    return ret
