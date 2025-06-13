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

from esrally.config import Config, Scope
from esrally.storage._adapter import Head, Writable
from esrally.storage._http import (
    CHUNK_SIZE,
    MAX_RETRIES,
    HTTPAdapter,
    head_from_headers,
    ranges_to_headers,
)
from esrally.storage._range import rangeset
from esrally.types import Key
from esrally.utils.cases import cases

URL = "https://example.com"


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
    simple=HeadCase(URL, response(), Head(url=URL)),
    accept_ranges=HeadCase(URL, response(accept_ranges=True), Head(url=URL, accept_ranges=True)),
    content_length=HeadCase(URL, response(content_length=512), Head(URL, content_length=512)),
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
    simple=GetCase(URL, response(), Head(url=URL)),
    accept_ranges=GetCase(URL, response(accept_ranges=True), Head(url=URL, accept_ranges=True)),
    content_length=GetCase(URL, response(content_length=512), Head(url=URL, content_length=512)),
    read_data=GetCase(URL, response(data="some_data"), Head(url=URL), want_data="some_data"),
    ranges=GetCase(
        URL,
        response(content_range="bytes 10-20/30"),
        Head(url=URL, ranges=rangeset("10-20"), accept_ranges=True),
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


@dataclass()
class RangesToHeadersCase:
    ranges: str
    want: dict[str, str] | type[Exception]


@cases(
    no_ranges=RangesToHeadersCase("", {}),
    range=RangesToHeadersCase("10-20", {"Range": "bytes=10-20"}),
    open_left=RangesToHeadersCase("-20", {"Range": "bytes=0-20"}),
    open_right=RangesToHeadersCase("10-", {"Range": "bytes=10-"}),
    multipart=RangesToHeadersCase("1-5,7-10", NotImplementedError),
)
def test_ranges_to_headers(case: RangesToHeadersCase) -> None:
    try:
        got = ranges_to_headers(rangeset(case.ranges))
    except Exception as ex:
        got = ex
    if isinstance(case.want, type):
        assert isinstance(got, case.want)
    else:
        assert got == CaseInsensitiveDict(case.want)


@dataclass()
class HeadFromHeadersCase:
    url: str
    headers: dict[str, str]
    want: Head | Exception


@cases(
    empty=HeadFromHeadersCase(URL, {}, Head(URL)),
    content_length=HeadFromHeadersCase(URL, {"Content-Length": "10"}, Head(URL, content_length=10)),
    accept_ranges=HeadFromHeadersCase(URL, {"Accept-Ranges": "bytes"}, Head(URL, accept_ranges=True)),
    ranges=HeadFromHeadersCase(URL, {"Content-Range": "bytes 512-1023/146515"}, Head(URL, ranges=rangeset("512-1023"), accept_ranges=True)),
)
def test_head_from_headers(case: HeadFromHeadersCase):
    try:
        got = head_from_headers(url=case.url, headers=CaseInsensitiveDict(case.headers))
    except Exception as ex:
        got = ex
    if isinstance(case.want, type):
        assert isinstance(got, case.want)
    else:
        assert got == case.want


@dataclass()
class FromConfigCase:
    opts: dict[Key, str]
    want_chunk_size: int = CHUNK_SIZE
    want_max_retries: int = MAX_RETRIES


@cases(
    default=FromConfigCase({}),
    chunk_size=FromConfigCase({"storage.http.chunk_size": "10"}, want_chunk_size=10),
    max_retries=FromConfigCase({"storage.http.max_retries": "3"}, want_max_retries=3),
    max_retries_yml=FromConfigCase({"storage.http.max_retries": '{"total":5}'}, want_max_retries=5),
)
def test_from_config(case: FromConfigCase) -> None:
    cfg = Config()
    for k, v in case.opts.items():
        cfg.add(Scope.application, "storage", k, v)
    adapter = HTTPAdapter.from_config(cfg)
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.chunk_size == case.want_chunk_size
    assert adapter.session.adapters["https://"].max_retries.total == case.want_max_retries
