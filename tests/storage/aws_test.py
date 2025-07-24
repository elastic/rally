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

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from unittest.mock import call, create_autospec

import boto3
import pytest

from esrally.config import Config, Scope
from esrally.storage._adapter import Head, Readable, Writable
from esrally.storage._aws import S3Adapter, S3Client, head_from_response
from esrally.storage._http import CHUNK_SIZE
from esrally.storage._range import rangeset
from esrally.types import Key
from esrally.utils.cases import cases

SOME_BUCKET = "some-example"
SOME_KEY = "some/key"
SOME_URL = f"s3://{SOME_BUCKET}/{SOME_KEY}"

ACCEPT_RANGES_HEADERS = {"AcceptRanges": "bytes"}
CONTENT_LENGTH_HEADERS = {"ContentLength": 512}
CONTENT_RANGE_HEADERS = {"ContentRange": "bytes 3-20/128", "ContentLength": 18}


@dataclass()
class HeadCase:
    response: dict[str, Any]
    want: Head
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_key: str = SOME_KEY


@pytest.fixture
def s3_client() -> S3Client:
    cls = type(boto3.Session().client("s3"))
    return create_autospec(cls, instance=True)


@cases(
    empty=HeadCase({}, Head(SOME_URL)),
    accept_ranges=HeadCase(ACCEPT_RANGES_HEADERS, Head(SOME_URL, accept_ranges=True)),
    content_length=HeadCase(CONTENT_LENGTH_HEADERS, Head(SOME_URL, content_length=512)),
    content_range=HeadCase(
        CONTENT_RANGE_HEADERS,
        Head(SOME_URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
    ),
)
def test_head(case: HeadCase, s3_client) -> None:
    s3_client.head_object.return_value = case.response
    adapter = S3Adapter(s3_client=s3_client)
    head = adapter.head(case.url)
    assert head == case.want
    s3_client.head_object.assert_called_with(Bucket=case.want_bucket, Key=case.want_key)


class DummyBody:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def iter_chunks(self, chunk_size: int) -> Iterable[bytes]:
        while self.body:
            yield self.body[:chunk_size]
            self.body = self.body[chunk_size:]


SOME_DATA = b"some-data"
SOME_DATA_HEADERS = {"ContentLength": len(SOME_DATA), "Body": DummyBody(SOME_DATA)}


@dataclass()
class GetCase:
    response: dict[str, Any]
    want: Head
    content_length: int | None = None
    ranges: str = ""
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_key: str = SOME_KEY
    want_range: str = ""
    want_write_data: Iterable[bytes] = tuple()


@cases(
    empty=GetCase({}, Head(SOME_URL)),
    accept_ranges=GetCase(ACCEPT_RANGES_HEADERS, Head(SOME_URL, accept_ranges=True)),
    content_length=GetCase(CONTENT_LENGTH_HEADERS, Head(SOME_URL, content_length=512)),
    read_data=GetCase(SOME_DATA_HEADERS, Head(SOME_URL, content_length=len(SOME_DATA)), want_write_data=[SOME_DATA]),
    ranges=GetCase(
        CONTENT_RANGE_HEADERS,
        Head(SOME_URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
        ranges="3-20",
        want_range="bytes=3-20",
    ),
)
def test_get(case: GetCase, s3_client) -> None:
    case.response.setdefault("Body", DummyBody(b""))
    s3_client.get_object.return_value = case.response
    adapter = S3Adapter(s3_client=s3_client)
    stream = create_autospec(Writable, spec_set=True, instance=True)
    head = adapter.get(case.url, stream, head=Head(content_length=case.content_length, ranges=rangeset(case.ranges)))
    assert head == case.want
    kwargs = {}
    if case.want_range:
        kwargs["Range"] = f"bytes={case.ranges}"
    s3_client.get_object.assert_called_once_with(Bucket=case.want_bucket, Key=case.want_key, **kwargs)
    assert [call(data) for data in case.want_write_data] == stream.write.mock_calls


@dataclass()
class PutCase:
    response: dict[str, Any]
    want: Head
    url: str = SOME_URL
    ranges: str = ""
    content_length: int | None = None
    want_bucket: str = SOME_BUCKET
    want_key: str = SOME_KEY
    want_range: str = ""


@cases(
    empty=PutCase({}, Head(SOME_URL)),
    accept_ranges=PutCase(ACCEPT_RANGES_HEADERS, Head(SOME_URL, accept_ranges=True)),
    content_length=PutCase(CONTENT_LENGTH_HEADERS, Head(SOME_URL, content_length=512)),
    read_data=PutCase(SOME_DATA_HEADERS, Head(SOME_URL, content_length=len(SOME_DATA))),
)
def test_put(case: PutCase, s3_client):
    s3_client.head_object.return_value = case.response
    adapter = S3Adapter(s3_client=s3_client)
    stream = create_autospec(Readable, spec_set=True, instance=True)
    got = adapter.put(stream, case.url, head=Head(content_length=case.content_length, ranges=rangeset(case.ranges)))
    kwargs = {}
    assert got == case.want
    if case.want_range:
        kwargs["Range"] = f"bytes={case.ranges}"
    s3_client.upload_fileobj.assert_called_once_with(stream, case.want_bucket, case.want_key)


@dataclass()
class HeadFromResponseCase:
    response: dict[str, Any]
    want_head: Head | None = None
    url: str = SOME_URL
    want_errors: tuple[type[Exception], ...] = tuple()


@cases(
    empty=HeadFromResponseCase({}, Head(SOME_URL)),
    content_length=HeadFromResponseCase(CONTENT_LENGTH_HEADERS, Head(SOME_URL, content_length=512)),
    accept_ranges=HeadFromResponseCase(ACCEPT_RANGES_HEADERS, Head(SOME_URL, accept_ranges=True)),
    ranges=HeadFromResponseCase(
        CONTENT_RANGE_HEADERS,
        Head(SOME_URL, ranges=rangeset("3-20"), content_length=18, document_length=128),
    ),
)
def test_head_from_response(case: HeadFromResponseCase):
    try:
        got = head_from_response(url=case.url, response=case.response)
    except case.want_errors:
        return
    assert got == case.want_head


@dataclass()
class FromConfigCase:
    opts: dict[Key, str]
    want_aws_profile: str = None
    want_chunk_size: int = CHUNK_SIZE


@cases(
    default=FromConfigCase({}),
    chunk_size=FromConfigCase({"storage.http.chunk_size": "10"}, want_chunk_size=10),
    aws_profile=FromConfigCase({"storage.aws.profile": "foo"}, want_aws_profile="foo"),
)
def test_from_config(case: FromConfigCase) -> None:
    cfg = Config()
    for k, v in case.opts.items():
        cfg.add(Scope.application, "storage", k, v)
    adapter = S3Adapter.from_config(cfg)
    assert isinstance(adapter, S3Adapter)
    assert adapter.chunk_size == case.want_chunk_size
    assert adapter.aws_profile == case.want_aws_profile
