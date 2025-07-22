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
from typing import Any
from unittest.mock import create_autospec

import boto3
import pytest

from esrally.storage._adapter import Head
from esrally.storage._aws import S3Adapter
from esrally.storage._range import rangeset
from esrally.utils.cases import cases

URL = "s3://example.com/some/path.json"

ACCEPT_RANGES_HEADER = {"AcceptRanges": "bytes"}
CONTENT_LENGTH_HEADER = {"ContentLength": "512"}
CONTENT_RANGE_HEADER = {"ContentRange": "bytes 3-20/128"}

DATA = "some-data"


@dataclass()
class HeadCase:
    response: dict[str, Any]
    want: Head
    url: str = URL


@pytest.fixture
def s3_client():
    cls = type(boto3.Session().client("s3"))
    return create_autospec(cls, instance=True)


@cases(
    empty=HeadCase({}, Head(URL)),
    accept_ranges=HeadCase(ACCEPT_RANGES_HEADER, Head(URL, accept_ranges=True)),
    content_length=HeadCase(CONTENT_LENGTH_HEADER, Head(URL, content_length=512, document_length=512)),
    content_range=HeadCase(
        CONTENT_RANGE_HEADER,
        Head(URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
    ),
)
def test_head(case: HeadCase, s3_client) -> None:
    s3_client.head_object.return_value = case.response
    adapter = S3Adapter(s3_client=s3_client)
    head = adapter.head(case.url)
    assert head == case.want


# @dataclass()
# class GetCase:
#     response: Response
#     want: Head
#     url: str = URL
#     ranges: str = ""
#     want_data: str = ""
#     want_request_range: str = ""
#
#
# @cases(
#     default=GetCase(response(), Head(URL)),
#     accept_ranges=GetCase(response(ACCEPT_RANGES_HEADER), Head(URL, accept_ranges=True)),
#     content_length=GetCase(response(CONTENT_LENGTH_HEADER), Head(URL, content_length=512, document_length=512)),
#     read_data=GetCase(response(data="some_data"), Head(URL), want_data="some_data"),
#     ranges=GetCase(
#         response(CONTENT_RANGE_HEADER),
#         Head(URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
#         ranges="3-20",
#         want_request_range="bytes=3-20",
#     ),
# )
# def test_get(case: GetCase, session: Session) -> None:
#     adapter = S3Adapter(session=session)
#     session.get.return_value = case.response
#     stream = create_autospec(Writable, spec_set=True, instance=True)
#     head = adapter.get(case.url, stream, head=Head.create(ranges=rangeset(case.ranges)))
#     assert head == case.want
#     if case.want_data:
#         stream.write.assert_called_once_with(case.want_data)
#     else:
#         assert not stream.write.called
#     want_request_headers = {}
#     if case.want_request_range:
#         want_request_headers["range"] = case.want_request_range
#     session.get.assert_called_once_with(case.url, stream=True, allow_redirects=True, headers=want_request_headers)
#
#
# @dataclass()
# class RangesToHeadersCase:
#     ranges: str
#     want: dict[str, str] | type[Exception]
#
#
# @cases(
#     no_ranges=RangesToHeadersCase("", {}),
#     range=RangesToHeadersCase("10-20", {"Range": "bytes=10-20"}),
#     open_left=RangesToHeadersCase("-20", {"Range": "bytes=0-20"}),
#     open_right=RangesToHeadersCase("10-", {"Range": "bytes=10-"}),
#     multipart=RangesToHeadersCase("1-5,7-10", NotImplementedError),
# )
# def test_ranges_to_headers(case: RangesToHeadersCase) -> None:
#     # pylint: disable=protected-access
#     got = CaseInsensitiveDict()
#     try:
#         S3Adapter._ranges_to_headers(rangeset(case.ranges), got)
#     except Exception as ex:
#         got = ex
#     if isinstance(case.want, type):
#         assert isinstance(got, case.want)
#     else:
#         assert got == CaseInsensitiveDict(case.want)
#
#
# @dataclass()
# class HeadFromHeadersCase:
#     headers: dict[str, str]
#     want: Head | Exception
#     url: str = URL
#
#
# @cases(
#     empty=HeadFromHeadersCase({}, Head(URL)),
#     content_length=HeadFromHeadersCase(CONTENT_LENGTH_HEADER, Head(URL, content_length=512, document_length=512)),
#     accept_ranges=HeadFromHeadersCase(ACCEPT_RANGES_HEADER, Head(URL, accept_ranges=True)),
#     ranges=HeadFromHeadersCase(
#         CONTENT_RANGE_HEADER,
#         Head(URL, ranges=rangeset("3-20"), content_length=18, document_length=128),
#     ),
# )
# def test_make_head(case: HeadFromHeadersCase):
#     # pylint: disable=protected-access
#     try:
#         got = S3Adapter._make_head(url=case.url, headers=CaseInsensitiveDict(case.headers))
#     except Exception as ex:
#         got = ex
#     if isinstance(case.want, type):
#         assert isinstance(got, case.want)
#     else:
#         assert got == case.want
#
#
# @dataclass()
# class FromConfigCase:
#     opts: dict[Key, str]
#     want_chunk_size: int = CHUNK_SIZE
#     want_max_retries: int = MAX_RETRIES
#     want_backoff_factor: int = 0
#
#
# @cases(
#     default=FromConfigCase({}),
#     chunk_size=FromConfigCase({"storage.http.chunk_size": "10"}, want_chunk_size=10),
#     max_retries=FromConfigCase({"storage.http.max_retries": "3"}, want_max_retries=3),
#     max_retries_yml=FromConfigCase(
#         {"storage.http.max_retries": '{"total": 5, "backoff_factor": 5}'}, want_max_retries=5, want_backoff_factor=5
#     ),
# )
# def test_from_config(case: FromConfigCase) -> None:
#     cfg = Config()
#     for k, v in case.opts.items():
#         cfg.add(Scope.application, "storage", k, v)
#     adapter = HTTPAdapter.from_config(cfg)
#     assert isinstance(adapter, HTTPAdapter)
#     assert adapter.chunk_size == case.want_chunk_size
#     retry = adapter.session.adapters["https://"].max_retries
#     assert retry.total == case.want_max_retries
#     assert retry.backoff_factor == case.want_backoff_factor
