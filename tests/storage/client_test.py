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

import collections
import dataclasses
import json
import os
import random
from collections.abc import Iterator
from os import PathLike
from typing import Any

import pytest

from esrally.config import Config
from esrally.storage._adapter import DummyAdapter, Head
from esrally.storage._client import CachedHeadError, Client, MirrorFailure
from esrally.storage._config import StorageConfig
from esrally.storage._range import NO_RANGE, RangeSet, rangeset
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
NOT_FOUND_URL = f"{NOT_FOUND_BASE_URL}/apm/span.json.bz2"

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


def default_heads() -> dict[str, Head]:
    return {h.url: h for h in HEADS}


def default_data() -> dict[str, bytes]:
    return collections.defaultdict(lambda: SOME_BODY)


@dataclasses.dataclass
class StorageAdapter(DummyAdapter):
    heads: dict[str, Head] = dataclasses.field(default_factory=default_heads)
    data: dict[str, bytes] = dataclasses.field(default_factory=default_data)


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
    cfg = StorageConfig()
    cfg.mirror_files = mirror_files
    cfg.random_seed = 42
    cfg.adapters = (f"{__name__}:StorageAdapter",)
    return cfg


@pytest.fixture(scope="function")
def client(cfg: Config) -> Client:
    return Client.from_config(cfg)


def storage_config(**kwargs: Any) -> StorageConfig:
    cfg = StorageConfig()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


@dataclasses.dataclass
class FromConfigCase:
    cfg: StorageConfig
    want_max_connections: int = StorageConfig.DEFAULT_MAX_CONNECTIONS
    want_random: random.Random | None = None


@cases(
    default=FromConfigCase(storage_config()),
    max_connections=FromConfigCase(storage_config(max_connections=42), want_max_connections=42),
    random_seed=FromConfigCase(storage_config(random_seed="24"), want_random=random.Random("24")),
)
def test_from_config(case: FromConfigCase) -> None:
    # pylint: disable=protected-access
    client = Client.from_config(case.cfg)
    assert isinstance(client, Client)
    assert client._connections["some"].max_count == case.want_max_connections
    if case.want_random is not None:
        assert client._random.random() == case.want_random.random()


@dataclasses.dataclass
class HeadCase:
    url: str
    want_head: Head | None = None
    want_error: type[Exception] | None = None
    cache_ttl: float | None = None
    want_cached: bool | None = None


@cases(
    default=HeadCase(SOME_URL, SOME_HEAD),
    cache_ttl=HeadCase(SOME_URL, SOME_HEAD, cache_ttl=300.0, want_cached=True),
    no_cache_ttl=HeadCase(SOME_URL, SOME_HEAD, cache_ttl=0.0, want_cached=False),
    negative_cache_ttl=HeadCase(SOME_URL, SOME_HEAD, cache_ttl=-1.0, want_cached=False),
    error=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError),
    error_cache_ttl=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError, cache_ttl=300.0, want_cached=True),
    error_no_cache_ttl=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError, cache_ttl=0.0, want_cached=False),
    error_negagive_cache_ttl=HeadCase(NOT_FOUND_BASE_URL, want_error=FileNotFoundError, cache_ttl=-1.0, want_cached=False),
)
def test_head(case: HeadCase, client: Client) -> None:
    # pylint: disable=catching-non-exception
    err: Exception | None = None
    want_error = tuple(filter(None, [case.want_error]))
    try:
        got = client.head(url=case.url, cache_ttl=case.cache_ttl)
    except want_error as e:
        got = None
        err = e
    else:
        assert got == case.want_head
    if case.want_cached is not None:
        try:
            assert (got is client.head(url=case.url, cache_ttl=case.cache_ttl)) == case.want_cached
        except CachedHeadError as ex:
            assert case.want_cached
            assert err is ex.__cause__
        except want_error:
            assert not case.want_cached


@dataclasses.dataclass
class ResolveCase:
    url: str
    want: list[Head]
    content_length: int | None = None
    accept_ranges: bool | None = None
    cache_ttl: float = 60.0
    want_mirror_failures: list = dataclasses.field(default_factory=list)


@cases(
    unmirrored=ResolveCase(url=SOME_URL, want=[SOME_HEAD]),
    mirrored=ResolveCase(
        url=MIRRORING_URL,
        want=[MIRRORED_HEAD, MIRRORED_NO_RANGE_HEAD, MIRRORING_HEAD],
        want_mirror_failures=[MirrorFailure(url=MIRRORING_URL, mirror_url=NOT_FOUND_URL, error="FileNotFoundError:")],
    ),
    document_length=ResolveCase(
        url=MIRRORING_URL,
        content_length=len(SOME_BODY),
        want=[MIRRORED_HEAD, MIRRORED_NO_RANGE_HEAD, MIRRORING_HEAD],
        want_mirror_failures=[MirrorFailure(url=MIRRORING_URL, mirror_url=NOT_FOUND_URL, error="FileNotFoundError:")],
    ),
    mismatching_document_length=ResolveCase(
        url=MIRRORING_URL,
        content_length=10,
        want=[],
        want_mirror_failures=[
            MirrorFailure(
                url=MIRRORING_URL,
                mirror_url=MIRRORED_NO_RANGE_URL,
                error="ValueError:unexpected 'content_length': got 30, want 10",
            ),
            MirrorFailure(url=MIRRORING_URL, mirror_url=MIRRORED_URL, error="ValueError:unexpected 'content_length': got 30, want 10"),
            MirrorFailure(url=MIRRORING_URL, mirror_url=NOT_FOUND_URL, error="FileNotFoundError:"),
        ],
    ),
    accept_ranges=ResolveCase(
        url=MIRRORING_URL,
        accept_ranges=True,
        want=[MIRRORING_HEAD, MIRRORED_HEAD],
        want_mirror_failures=[
            MirrorFailure(
                url=MIRRORING_URL,
                mirror_url=MIRRORED_NO_RANGE_URL,
                error="ValueError:unexpected 'accept_ranges': got False, want True",
            ),
            MirrorFailure(url=MIRRORING_URL, mirror_url=NOT_FOUND_URL, error="FileNotFoundError:"),
        ],
    ),
    reject_ranges=ResolveCase(url=NO_RANGES_URL, accept_ranges=True, want=[]),
    zero_ttl=ResolveCase(url=SOME_URL, cache_ttl=0.0, want=[SOME_HEAD]),
)
def test_resolve(case: ResolveCase, client: Client, monkeypatch: pytest.MonkeyPatch) -> None:
    check_head = Head(content_length=case.content_length, accept_ranges=case.accept_ranges)
    got = sorted(client.resolve(case.url, check_head=check_head, cache_ttl=case.cache_ttl), key=lambda h: str(h.url))
    want = sorted(case.want, key=lambda h: str(h.url))
    assert got == want, "unexpected resolve result"
    for g in got:
        if case.cache_ttl > 0.0:
            assert g is client.head(url=g.url, cache_ttl=case.cache_ttl), "obtained head wasn't cached"
        else:
            assert g is not client.head(url=g.url, cache_ttl=case.cache_ttl), "obtained head was cached"
    got_mirror_failures = client.mirror_failures(case.url)
    for f in got_mirror_failures:
        f.timestamp = 0.0
    assert sorted(client.mirror_failures(case.url), key=str) == case.want_mirror_failures


@dataclasses.dataclass
class GetCase:
    url: str
    want_any: list[Head]
    ranges: RangeSet = NO_RANGE
    document_length: int = None
    want_chunks: list[bytes] = dataclasses.field(default_factory=list)


@cases(
    regular=GetCase(
        SOME_URL,
        [Head(url=SOME_URL, content_length=len(SOME_BODY), document_length=len(SOME_BODY))],
        want_chunks=[SOME_BODY],
    ),
    range=GetCase(
        SOME_URL,
        [Head(SOME_URL, content_length=30, accept_ranges=True, ranges=rangeset("0-29"), document_length=len(SOME_BODY))],
        ranges=rangeset("0-29"),
        want_chunks=[SOME_BODY],
    ),
    mirrors=GetCase(
        MIRRORING_URL,
        [
            MIRRORED_HEAD,
            MIRRORED_NO_RANGE_HEAD,
        ],
        want_chunks=[SOME_BODY],
    ),
)
def test_get(case: GetCase, client: Client) -> None:
    with client.get(case.url, check_head=Head(ranges=case.ranges, document_length=case.document_length)) as got:
        assert check_any(got.head, case.want_any) != []
        assert list(got.chunks) == case.want_chunks


def check_any(head: Head, any_head: list[Head]) -> list[Head]:
    ret: list[Head] = []
    for h in any_head:
        try:
            h.check(head)
        except ValueError:
            continue
        ret.append(h)
    return ret
