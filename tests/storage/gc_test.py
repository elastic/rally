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
import urllib.parse
from dataclasses import dataclass
from typing import Any
from unittest import mock
from unittest.mock import create_autospec

import pytest
import requests.exceptions
from google.auth.transport.requests import AuthorizedSession
from requests import Response, Session
from requests.structures import CaseInsensitiveDict

from esrally.storage import Head, StorageConfig, gc, rangeset
from esrally.utils.cases import cases

BUCKET_NAME = "rally-tracks"
BLOB_NAME = "big5/logs-1.ndjson.bz2"

GS_URL = f"gs://{BUCKET_NAME}/{BLOB_NAME}"
HTTPS_URL = f"https://storage.cloud.google.com/{BUCKET_NAME}/{BLOB_NAME}"
API_URL = (
    f"https://storage.googleapis.com/storage/v1/b/{urllib.parse.quote(BUCKET_NAME, safe='')}/o/"
    f"{urllib.parse.quote(BLOB_NAME, safe='')}?alt=media"
)

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
    read_error: Exception | None = None,
):
    res = Response()
    res.raw = io.BytesIO(data)
    res.status_code = status_code
    res.headers = CaseInsensitiveDict()
    if headers is not None:
        res.headers.update(headers)
    if read_error is not None:
        res.iter_content = mock.create_autospec(res.iter_content, side_effect=read_error)
    return res


def storage_config(**kwargs: Any) -> StorageConfig:
    cfg = StorageConfig()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


def head(url: str = API_URL, accept_ranges: bool = True, **kwargs) -> Head:
    return Head(url=url, accept_ranges=accept_ranges, **kwargs)


@dataclasses.dataclass()
class HeadCase:
    response: Response = dataclasses.field(default_factory=response)
    want_head: Head = dataclasses.field(default_factory=head)
    url: str = API_URL


@cases(
    api_url=HeadCase(),
    gs_url=HeadCase(url=GS_URL),
    https_url=HeadCase(url=HTTPS_URL),
    content_length=HeadCase(response(CONTENT_LENGTH_HEADER), head(content_length=512)),
    content_range=HeadCase(
        response(CONTENT_RANGE_HEADER),
        head(content_length=18, ranges=rangeset("3-20"), document_length=128),
    ),
    x_goog_hash=HeadCase(response(X_GOOG_HASH_CRC32C_HEADER), head(crc32c="some-checksum")),
    x_amz_checksum=HeadCase(response(X_AMZ_CHECKSUM_CRC32C_HEADER), head(crc32c="some-checksum")),
)
def test_head(case: HeadCase, session: Session) -> None:
    adapter = gc.GSAdapter(session=session)
    session.head.return_value = case.response
    got_head = adapter.head(case.url)
    assert got_head == case.want_head


@dataclasses.dataclass
class GetCase:
    response: Response = dataclasses.field(default_factory=response)
    want_head: Head = dataclasses.field(default_factory=head)
    url: str = API_URL
    ranges: str = ""
    cfg: StorageConfig = dataclasses.field(default_factory=storage_config)
    want_data: list[bytes] = dataclasses.field(default_factory=list)
    want_request_range: str = ""
    want_timeout: tuple[float, float] = (StorageConfig.DEFAULT_CONNECT_TIMEOUT, StorageConfig.DEFAULT_READ_TIMEOUT)
    want_read_error: type[Exception] | None = None


@cases(
    api_url=GetCase(),
    gs_url=GetCase(url=GS_URL),
    https_url=GetCase(url=HTTPS_URL),
    content_length=GetCase(response(CONTENT_LENGTH_HEADER), head(content_length=512)),
    read_data=GetCase(response(data=b"some_data"), head(), want_data=[b"some_data"]),
    ranges=GetCase(
        response(CONTENT_RANGE_HEADER),
        head(content_length=18, ranges=rangeset("3-20"), document_length=128),
        ranges="3-20",
        want_request_range="bytes=3-20",
    ),
    x_goog_hash=GetCase(response(X_GOOG_HASH_CRC32C_HEADER), head(crc32c="some-checksum")),
    x_amz_checksum=GetCase(response(X_AMZ_CHECKSUM_CRC32C_HEADER), head(crc32c="some-checksum")),
    connect_timeout=GetCase(cfg=storage_config(connect_timeout=13.0), want_timeout=(13.0, StorageConfig.DEFAULT_READ_TIMEOUT)),
    read_timeout=GetCase(cfg=storage_config(read_timeout=11.0), want_timeout=(StorageConfig.DEFAULT_CONNECT_TIMEOUT, 11.0)),
    raise_timeout=GetCase(response(read_error=requests.exceptions.Timeout()), want_read_error=TimeoutError),
    raise_connection_error=GetCase(response(read_error=requests.exceptions.ConnectionError()), want_read_error=TimeoutError),
)
def test_get(case: GetCase, session: Session) -> None:
    adapter = gc.GSAdapter(
        session=session, chunk_size=case.cfg.chunk_size, connect_timeout=case.cfg.connect_timeout, read_timeout=case.cfg.read_timeout
    )
    session.get.return_value = case.response
    head, data = adapter.get(case.url, check_head=Head(ranges=rangeset(case.ranges)))
    assert head == case.want_head
    if case.want_read_error is None:
        assert list(data) == case.want_data
    else:
        with pytest.raises(case.want_read_error):
            list(data)
    want_request_headers = {}
    if case.want_request_range:
        want_request_headers["range"] = case.want_request_range
    session.get.assert_called_once_with(
        case.want_head.url, stream=True, allow_redirects=True, headers=want_request_headers, timeout=case.want_timeout
    )


@dataclass()
class FromConfigCase:
    cfg: StorageConfig
    want_chunk_size: int = StorageConfig.DEFAULT_CHUNK_SIZE
    want_max_retries: int = StorageConfig.DEFAULT_MAX_RETRIES
    want_backoff_factor: int = 0
    want_connect_timeout: float = StorageConfig.DEFAULT_CONNECT_TIMEOUT
    want_read_timeout: float = StorageConfig.DEFAULT_READ_TIMEOUT
    want_google_auth_token: str | None = StorageConfig.DEFAULT_GOOGLE_AUTH_TOKEN


@cases(
    default=FromConfigCase(storage_config()),
    chunk_size=FromConfigCase(storage_config(chunk_size=10), want_chunk_size=10),
    max_retries=FromConfigCase(storage_config(max_retries="3"), want_max_retries=3),
    max_retries_yml=FromConfigCase(
        storage_config(max_retries='{"total": 5, "backoff_factor": 5}'), want_max_retries=5, want_backoff_factor=5
    ),
    connect_timeout=FromConfigCase(storage_config(connect_timeout=5.0), want_connect_timeout=5.0),
    read_timeout=FromConfigCase(storage_config(read_timeout=7.0), want_read_timeout=7.0),
    google_auth_token=FromConfigCase(storage_config(google_auth_token="S0m3~T0k3n"), want_google_auth_token="S0m3~T0k3n"),
)
def test_from_config(case: FromConfigCase) -> None:
    adapter = gc.GSAdapter.from_config(case.cfg)
    assert isinstance(adapter, gc.GSAdapter)
    assert adapter.chunk_size == case.want_chunk_size
    retry = adapter.session.adapters["https://"].max_retries
    assert retry.total == case.want_max_retries
    assert retry.backoff_factor == case.want_backoff_factor
    assert adapter.connect_timeout == case.want_connect_timeout
    assert adapter.read_timeout == case.want_read_timeout
    assert isinstance(adapter.session, AuthorizedSession)
    assert adapter.session.credentials.token == case.want_google_auth_token
