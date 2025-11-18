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
from collections.abc import Generator
from typing import Any
from unittest import mock

import google.auth.credentials
import pytest
import requests
from google.cloud import storage as gcs
from typing_extensions import Self

from esrally import storage
from esrally.storage import gc
from esrally.utils.cases import cases

SOME_PROJECT = "elastic-performance-testing"
SOME_BUCKET = "rally-tracks"
SOME_BLOB = "apm/documents-1k.ndjson.bz2"
SOME_URL = f"gs://{SOME_BUCKET}/{SOME_BLOB}"
SOME_SIZE = 63458
SOME_CRC32C = "83jA8A=="

SOME_HEAD = storage.Head(
    url=SOME_URL,
    content_length=SOME_SIZE,
    accept_ranges=True,
    crc32c=SOME_CRC32C,
)


DEFAULT_RESPONSE = {
    # pylint: disable=line-too-long
    "bucket": SOME_BUCKET,
    "contentType": "application/octet-stream",
    "crc32c": SOME_CRC32C,
    "etag": "CIyixP2h5IQDEAE=",
    "generation": "1709887141777676",
    "id": "rally-tracks/apm/documents-1k.ndjson.bz2/1709887141777676",
    "kind": "storage#object",
    "md5Hash": "L1ESqWBTuflUGNe89oDGIQ==",
    "mediaLink": "https://www.googleapis.com/download/storage/v1/b/rally-tracks/o/apm%2Fdocuments-1k.ndjson.bz2?generation=1709887141777676&alt=media",
    "metageneration": "1",
    "name": "apm/documents-1k.ndjson.bz2",
    "selfLink": "https://www.googleapis.com/storage/v1/b/rally-tracks/o/apm%2Fdocuments-1k.ndjson.bz2",
    "size": str(SOME_SIZE),
    "storageClass": "STANDARD",
    "timeCreated": "2024-03-08T08:39:01.854Z",
    "timeFinalized": "2024-03-08T08:39:01.854Z",
    "timeStorageClassUpdated": "2024-03-08T08:39:01.854Z",
    "updated": "2024-03-08T08:39:01.854Z",
}


@dataclasses.dataclass
class HeadCase:
    response: dict[str, Any]
    want: storage.Head
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_blob: str = SOME_BLOB


def response(**kwargs) -> dict[str, Any]:
    res = dict(DEFAULT_RESPONSE)
    res.update(kwargs)
    return res


@pytest.fixture
def client() -> gcs.Client:
    return gcs.Client(project=SOME_PROJECT)


@pytest.fixture(autouse=True)
def default_credentials(monkeypatch: pytest.MonkeyPatch):
    credentials = mock.create_autospec(google.auth.credentials.Credentials, instance=True, universe_domain="googleapis.com")
    monkeypatch.setattr(google.auth, "default", mock.create_autospec(google.auth.default, return_value=[credentials, None]))


@cases(
    empty=HeadCase(response(), SOME_HEAD),
    content_length=HeadCase(response(size=1243), storage.Head(url=SOME_URL, content_length=1243, accept_ranges=True, crc32c=SOME_CRC32C)),
    crc32c=HeadCase(
        response(crc32c="some-crc32c"), storage.Head(url=SOME_URL, crc32c="some-crc32c", content_length=SOME_SIZE, accept_ranges=True)
    ),
)
def test_head(case: HeadCase, client: gcs.Client) -> None:
    # pylint: disable=protected-access
    client._connection.api_request = mock.create_autospec(client._connection.api_request, return_value=case.response)
    adapter = gc.GSAdapter(client=client)
    head = adapter.head(case.url)
    assert head == case.want
    client._connection.api_request.assert_called_once()


SOME_DATA = b"some-data,"
SOME_HEADERS = {
    "Content-Type": "application/octet-stream",
    "X-Goog-Hash": "crc32c=83jA8A==,md5=L1ESqWBTuflUGNe89oDGIQ==",
}


class DummyResponse:
    def __init__(self, body: bytes, ranges: storage.RangeSet = storage.NO_RANGE, headers: dict[str, Any] | None = None) -> None:
        self.headers = dict(SOME_HEADERS)
        if headers is not None:
            self.headers.update(headers)
        self.ranges = ranges
        if ranges:
            self.headers["Content-Range"] = str(ranges)
            self.headers["Content-Length"] = ranges.size
            self.status_code = 206
            self.body = body[ranges.start : ranges.end]
        else:
            self.headers["Content-Length"] = len(body)
            self.status_code = 200
            self.body = body

    @property
    def raw(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass

    def iter_content(self, chunk_size: int, decode_unicode: bytes = False) -> Generator[bytes]:
        while self.body:
            yield self.body[:chunk_size]
            self.body = self.body[chunk_size:]


@dataclasses.dataclass
class GetCase:
    response: dict[str, Any]
    data: bytes
    want_head: storage.Head
    content_length: int | None = None
    ranges: str = ""
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_blob: str = SOME_BLOB
    want_range: str = ""
    want_data: list[bytes] = dataclasses.field(default_factory=list)


@cases(
    no_ranges=GetCase(
        response(size=len(SOME_DATA) * 10),
        data=SOME_DATA * 10,
        want_head=storage.Head(SOME_URL, content_length=len(SOME_DATA) * 10, crc32c=SOME_CRC32C),
        want_data=[SOME_DATA * 10],
    ),
    ranges=GetCase(
        response(size=len(SOME_DATA) * 10),
        data=SOME_DATA * 10,
        ranges="10-20",
        want_head=storage.Head(
            SOME_URL, document_length=len(SOME_DATA) * 10, ranges=storage.rangeset("10-20"), content_length=11, crc32c=SOME_CRC32C
        ),
        want_data=[b"some-data,s"],
    ),
)
def test_get(case: GetCase, client: gcs.Client) -> None:
    # pylint: disable=protected-access
    client._connection.api_request = mock.create_autospec(client._connection.api_request, return_value=case.response)
    client._http_internal = mock.create_autospec(requests.Session)
    client._http_internal.request.return_value = DummyResponse(body=case.data, ranges=storage.rangeset(case.ranges))
    adapter = gc.GSAdapter(client=client)
    head, chunks = adapter.get(case.url, check_head=storage.Head(content_length=case.content_length, ranges=storage.rangeset(case.ranges)))
    assert head == case.want_head
    assert case.want_data == list(chunks)

    kwargs = {}
    if case.want_range:
        kwargs["Range"] = f"bytes={case.ranges}"
    client._connection.api_request.assert_has_calls([])
