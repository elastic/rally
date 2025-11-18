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
from unittest import mock

import pytest
from google.cloud import storage

from esrally.storage import Head, gc, rangeset
from esrally.utils.cases import cases

SOME_PROJECT = "elastic-performance-testing"
SOME_BUCKET = "rally-tracks"
SOME_BLOB = "apm/documents-1k.ndjson.bz2"
SOME_URL = f"gs://{SOME_BUCKET}/{SOME_BLOB}"
SOME_DATA = b"some-data,"
SOME_SIZE = len(SOME_DATA) * 10
SOME_CRC32C = "83jA8A=="
SOME_HEAD = Head(
    url=SOME_URL,
    content_length=SOME_SIZE,
    accept_ranges=True,
    crc32c=SOME_CRC32C,
)


@pytest.fixture
def dummy_blob() -> storage.Blob:
    return DummyBlob()


@pytest.fixture
def adapter(dummy_blob: storage.Blob) -> gc.GSAdapter:
    client = mock.create_autospec(storage.Client)
    adapter = gc.GSAdapter(client=client)
    return adapter


class DummyBlob(storage.Blob):

    def __init__(
        self,
        content: bytes = SOME_DATA * 10,
        crc32s: str = SOME_CRC32C,
        name: str = SOME_BLOB,
        bucket_name: str = SOME_BUCKET,
    ) -> None:
        super().__init__(name=name, bucket=storage.Bucket(client=None, name=bucket_name))
        self.content = content
        self._set_properties({"crc32c": crc32s, "size": len(content)})

    def open(
        self,
        mode="r",
        chunk_size=None,
        ignore_flush=None,
        encoding=None,
        errors=None,
        newline=None,
        **kwargs,
    ) -> io.BytesIO:
        assert mode == "rb"
        return io.BytesIO(self.content)


@dataclasses.dataclass
class HeadCase:
    want_head: Head
    url: str = SOME_URL
    blob: storage.Blob = dataclasses.field(default_factory=DummyBlob)
    want_bucket: str = SOME_BUCKET
    want_blob: str = SOME_BLOB


# @pytest.fixture(autouse=True)
# def default_credentials(monkeypatch: pytest.MonkeyPatch):
#     credentials = mock.create_autospec(google.auth.credentials.Credentials, instance=True, universe_domain="googleapis.com")
#     monkeypatch.setattr(google.auth, "default", mock.create_autospec(google.auth.default, return_value=[credentials, None]))


@cases(
    default=HeadCase(want_head=SOME_HEAD),
    content_length=HeadCase(
        blob=DummyBlob(content=SOME_DATA[:5]), want_head=Head(url=SOME_URL, content_length=5, accept_ranges=True, crc32c=SOME_CRC32C)
    ),
    crc32c=HeadCase(
        blob=DummyBlob(crc32s="some-crc32c"),
        want_head=Head(url=SOME_URL, crc32c="some-crc32c", content_length=SOME_SIZE, accept_ranges=True),
    ),
)
def test_head(case: HeadCase, adapter: gc.GSAdapter) -> None:
    adapter.blob = mock.create_autospec(adapter.blob, return_value=case.blob)
    head = adapter.head(case.url)
    assert head == case.want_head


@dataclasses.dataclass
class GetCase:
    want_head: Head
    blob: DummyBlob = dataclasses.field(default_factory=DummyBlob)
    content_length: int = len(SOME_DATA) * 10
    ranges: str = ""
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_blob: str = SOME_BLOB
    want_range: str = ""
    want_data: list[bytes] = dataclasses.field(default_factory=list)


@cases(
    no_ranges=GetCase(
        want_head=Head(SOME_URL, content_length=len(SOME_DATA) * 10, crc32c=SOME_CRC32C),
        want_data=[SOME_DATA * 10],
    ),
    ranges=GetCase(
        ranges="10-20",
        content_length=11,
        want_head=Head(SOME_URL, document_length=len(SOME_DATA) * 10, ranges=rangeset("10-20"), content_length=11, crc32c=SOME_CRC32C),
        want_data=[b"some-data,s"],
    ),
)
def test_get(case: GetCase, adapter: gc.GSAdapter) -> None:
    adapter.blob = mock.create_autospec(adapter.blob, return_value=case.blob)
    head, chunks = adapter.get(case.url, check_head=Head(content_length=case.content_length, ranges=rangeset(case.ranges)))
    assert head == case.want_head
    assert case.want_data == list(chunks)
