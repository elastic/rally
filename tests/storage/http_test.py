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
import dataclasses
import io
from dataclasses import dataclass
from typing import Any
from unittest.mock import create_autospec

import pytest
from requests import Response, Session
from requests.structures import CaseInsensitiveDict

from esrally.storage._adapter import Head
from esrally.storage._config import StorageConfig
from esrally.storage._range import rangeset
from esrally.storage.http import HTTPAdapter, head_from_headers, ranges_to_headers
from esrally.utils.cases import cases

URL = "https://example.com"

ACCEPT_RANGES_HEADER = {"Accept-Ranges": "bytes"}
CONTENT_LENGTH_HEADER = {"Content-Length": "512"}
CONTENT_RANGE_HEADER = {"Content-Range": "bytes 3-20/128", "Content-Length": "18"}
X_GOOG_HASH_CRC32C_HEADER = {"X-Goog-Hash": "crc32c=some-checksum"}
X_AMZ_CHECKSUM_CRC32C_HEADER = {"x-amz-checksum-crc32c": "some-checksum"}


@pytest.fixture()
def session() -> Session:
    return create_autospec(Session, spec_set=True, instance=True)


def response(
    headers: dict[str, str] | None = None,
    status_code: int = 200,
    data: bytes = b"",
):
    res = Response()
    res.raw = io.BytesIO(data)
    res.status_code = status_code
    res.headers = CaseInsensitiveDict()
    if headers is not None:
        res.headers.update(headers)
    return res


@dataclass()
class HeadCase:
    response: Response
    want: Head
    url: str = URL


@cases(
    simple=HeadCase(response(), Head(URL)),
    accept_ranges=HeadCase(response(ACCEPT_RANGES_HEADER), Head(URL, accept_ranges=True)),
    content_length=HeadCase(response(CONTENT_LENGTH_HEADER), Head(URL, content_length=512)),
    content_range=HeadCase(
        response(CONTENT_RANGE_HEADER),
        Head(URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
    ),
    x_goog_hash=HeadCase(response(X_GOOG_HASH_CRC32C_HEADER), Head(URL, crc32c="some-checksum")),
    x_amz_checksum=HeadCase(response(X_AMZ_CHECKSUM_CRC32C_HEADER), Head(URL, crc32c="some-checksum")),
)
def test_head(case: HeadCase, session: Session) -> None:
    adapter = HTTPAdapter(session=session)
    session.head.return_value = case.response
    head = adapter.head(case.url)
    assert head == case.want


@dataclasses.dataclass
class GetCase:
    response: Response
    want_head: Head
    url: str = URL
    ranges: str = ""
    want_data: list[bytes] = dataclasses.field(default_factory=list)
    want_request_range: str = ""


@cases(
    default=GetCase(response(), Head(URL)),
    accept_ranges=GetCase(response(ACCEPT_RANGES_HEADER), Head(URL, accept_ranges=True)),
    content_length=GetCase(response(CONTENT_LENGTH_HEADER), Head(URL, content_length=512)),
    read_data=GetCase(response(data=b"some_data"), Head(URL), want_data=[b"some_data"]),
    ranges=GetCase(
        response(CONTENT_RANGE_HEADER),
        Head(URL, content_length=18, ranges=rangeset("3-20"), document_length=128),
        ranges="3-20",
        want_request_range="bytes=3-20",
    ),
    x_goog_hash=GetCase(response(X_GOOG_HASH_CRC32C_HEADER), Head(URL, crc32c="some-checksum")),
    x_amz_checksum=GetCase(response(X_AMZ_CHECKSUM_CRC32C_HEADER), Head(URL, crc32c="some-checksum")),
)
def test_get(case: GetCase, session: Session) -> None:
    adapter = HTTPAdapter(session=session)
    session.get.return_value = case.response

    with adapter.get(case.url, check_head=Head(ranges=rangeset(case.ranges))) as got:
        assert got.head == case.want_head
        assert list(got.chunks) == case.want_data

    want_request_headers = {}
    if case.want_request_range:
        want_request_headers["range"] = case.want_request_range
    session.get.assert_called_once_with(case.url, stream=True, allow_redirects=True, headers=want_request_headers)


@dataclass()
class RangesToHeadersCase:
    ranges: str
    want_headers: dict[str, str] | None = None
    want_errors: tuple[type[Exception], ...] = tuple()


@cases(
    no_ranges=RangesToHeadersCase("", {}),
    range=RangesToHeadersCase("10-20", {"Range": "bytes=10-20"}),
    open_left=RangesToHeadersCase("-20", {"Range": "bytes=0-20"}),
    open_right=RangesToHeadersCase("10-", {"Range": "bytes=10-"}),
    multipart=RangesToHeadersCase("1-5,7-10", want_errors=(NotImplementedError,)),
)
def test_ranges_to_headers(case: RangesToHeadersCase) -> None:
    got: dict[str, Any] = {}
    try:
        ranges_to_headers(rangeset(case.ranges), got)
    except case.want_errors:
        return

    assert got == case.want_headers


@dataclass()
class HeadFromHeadersCase:
    headers: dict[str, str]
    want: Head | Exception
    url: str = URL


@cases(
    empty=HeadFromHeadersCase({}, Head(URL)),
    content_length=HeadFromHeadersCase(CONTENT_LENGTH_HEADER, Head(URL, content_length=512)),
    accept_ranges=HeadFromHeadersCase(ACCEPT_RANGES_HEADER, Head(URL, accept_ranges=True)),
    ranges=HeadFromHeadersCase(
        CONTENT_RANGE_HEADER,
        Head(URL, ranges=rangeset("3-20"), content_length=18, document_length=128),
    ),
    x_goog_hash=HeadFromHeadersCase(X_GOOG_HASH_CRC32C_HEADER, Head(URL, crc32c="some-checksum")),
    x_amz_checksum=HeadFromHeadersCase(X_AMZ_CHECKSUM_CRC32C_HEADER, Head(URL, crc32c="some-checksum")),
)
def test_head_from_headers(case: HeadFromHeadersCase):
    try:
        got = head_from_headers(url=case.url, headers=case.headers)
    except Exception as ex:
        got = ex
    if isinstance(case.want, type):
        assert isinstance(got, case.want)
    else:
        assert got == case.want


def storage_config(**kwargs: Any) -> StorageConfig:
    cfg = StorageConfig()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


@dataclass()
class FromConfigCase:
    cfg: StorageConfig
    want_chunk_size: int = StorageConfig.DEFAULT_CHUNK_SIZE
    want_max_retries: int = StorageConfig.DEFAULT_MAX_RETRIES
    want_backoff_factor: int = 0


@cases(
    default=FromConfigCase(storage_config()),
    chunk_size=FromConfigCase(storage_config(chunk_size=10), want_chunk_size=10),
    max_retries=FromConfigCase(storage_config(max_retries="3"), want_max_retries=3),
    max_retries_yml=FromConfigCase(
        storage_config(max_retries='{"total": 5, "backoff_factor": 5}'), want_max_retries=5, want_backoff_factor=5
    ),
)
def test_from_config(case: FromConfigCase) -> None:
    adapter = HTTPAdapter.from_config(case.cfg)
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.chunk_size == case.want_chunk_size
    retry = adapter.session.adapters["https://"].max_retries
    assert retry.total == case.want_max_retries
    assert retry.backoff_factor == case.want_backoff_factor
