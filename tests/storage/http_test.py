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

import io
from dataclasses import dataclass
from typing import Final
from unittest.mock import create_autospec

import pytest
from requests import Response, Session
from requests.structures import CaseInsensitiveDict

from esrally.storage import Head, Writable, rangeset
from esrally.storage._http import HTTPAdapter
from esrally.utils.cases import cases


@pytest.fixture()
def session() -> Session:
    return create_autospec(Session, spec_set=True, instance=True)


@dataclass()
class HeadCase:
    url: str
    response: Response
    want: Head


def response(
    status_code: int = 200, accept_ranges: bool = False, content_length: int | None = None, data: str = "", content_range: str = ""
) -> Response:
    ret = Response()
    ret.raw = io.StringIO(data)
    ret.status_code = status_code
    ret.headers = CaseInsensitiveDict()
    if accept_ranges:
        ret.headers["Accept-Ranges"] = "bytes"
    if content_length is not None:
        ret.headers["Content-Length"] = str(content_length)
    if content_range:
        ret.headers["Content-Range"] = content_range
    return ret


@cases(
    "case",
    simple=HeadCase("https://www.example.com", response(), Head(url="https://www.example.com")),
    accept_ranges=HeadCase(
        "https://www.example.com", response(accept_ranges=True), Head(url="https://www.example.com", accept_ranges=True)
    ),
    content_length=HeadCase(
        "https://www.example.com", response(content_length=512), Head(url="https://www.example.com", content_length=512)
    ),
)
def test_head(case: HeadCase, session: Session) -> None:
    adapter = HTTPAdapter(session=session)
    session.head.return_value = case.response
    head = adapter.head(case.url)
    assert head == case.want


NO_HEADERS: Final[dict] = {}


@dataclass()
class GetCase:
    url: str
    response: Response
    want: Head
    ranges: str = ""
    want_data: str = ""
    want_request_range: str = ""


@cases(
    "case",
    simple=GetCase("https://www.example.com", response(), Head(url="https://www.example.com")),
    accept_ranges=GetCase("https://www.example.com", response(accept_ranges=True), Head(url="https://www.example.com", accept_ranges=True)),
    content_length=GetCase(
        "https://www.example.com", response(content_length=512), Head(url="https://www.example.com", content_length=512)
    ),
    read_data=GetCase("https://www.example.com", response(data="some_data"), Head(url="https://www.example.com"), want_data="some_data"),
    ranges=GetCase(
        "https://www.example.com",
        response(content_range="bytes 10-20/30"),
        Head(url="https://www.example.com", content_length=30, ranges=rangeset("10-20")),
        ranges="10-20",
        want_request_range="bytes=10-20",
    ),
)
def test_get(case: GetCase, session: Session) -> None:
    adapter = HTTPAdapter(session=session)
    session.get.return_value = case.response
    stream = create_autospec(Writable, spec_set=True, instance=True)
    head = adapter.get(case.url, stream, ranges=rangeset(case.ranges))
    assert head == case.want
    if case.want_data:
        stream.write.assert_called_once_with(case.want_data)
    else:
        assert not stream.write.called
    want_request_headers = {}
    if case.want_request_range:
        want_request_headers["range"] = case.want_request_range
    session.get.assert_called_once_with(case.url, stream=True, allow_redirects=True, headers=want_request_headers)
