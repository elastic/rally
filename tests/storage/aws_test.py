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
# pylint: disable=protected-access

import dataclasses
import threading
from collections.abc import Iterable
from typing import Any
from unittest import mock

import boto3
import botocore.exceptions
import pytest
from typing_extensions import Self

from esrally.storage import Head, StorageConfig, rangeset
from esrally.storage._adapter import ClientUnavailableError, ServiceUnavailableError
from esrally.storage.aws import S3Adapter, S3Client, head_from_response
from esrally.utils.cases import cases

SOME_BUCKET = "some-example"
SOME_KEY = "some/key"
SOME_URL = f"s3://{SOME_BUCKET}/{SOME_KEY}"

ACCEPT_RANGES_HEADERS = {"AcceptRanges": "bytes"}
CONTENT_LENGTH_HEADERS = {"ContentLength": 512}
CONTENT_RANGE_HEADERS = {"ContentRange": "bytes 3-20/128", "ContentLength": 18}


@dataclasses.dataclass
class HeadCase:
    response: dict[str, Any]
    want: Head
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_key: str = SOME_KEY


@pytest.fixture
def s3_client() -> S3Client:
    cls = type(boto3.Session().client("s3"))
    return mock.create_autospec(cls, instance=True)


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
        self.closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def iter_chunks(self, chunk_size: int) -> Iterable[bytes]:
        while self.body:
            yield self.body[:chunk_size]
            self.body = self.body[chunk_size:]

    def close(self) -> None:
        assert not self.closed, "already closed"
        self.closed = True


SOME_DATA = b"some-data"
SOME_DATA_HEADERS = {"ContentLength": len(SOME_DATA), "Body": DummyBody(SOME_DATA)}


@dataclasses.dataclass
class GetCase:
    response: dict[str, Any]
    want_head: Head
    content_length: int | None = None
    ranges: str = ""
    url: str = SOME_URL
    want_bucket: str = SOME_BUCKET
    want_key: str = SOME_KEY
    want_range: str = ""
    want_data: list[bytes] = dataclasses.field(default_factory=list)


@cases(
    empty=GetCase({}, Head(SOME_URL)),
    accept_ranges=GetCase(ACCEPT_RANGES_HEADERS, Head(SOME_URL, accept_ranges=True)),
    content_length=GetCase(CONTENT_LENGTH_HEADERS, Head(SOME_URL, content_length=512)),
    read_data=GetCase(SOME_DATA_HEADERS, Head(SOME_URL, content_length=len(SOME_DATA)), want_data=[SOME_DATA]),
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
    with adapter.get(case.url, check_head=Head(content_length=case.content_length, ranges=rangeset(case.ranges))) as got:
        assert got.head == case.want_head
        assert list(got.chunks) == case.want_data

    kwargs = {}
    if case.want_range:
        kwargs["Range"] = f"bytes={case.ranges}"
    s3_client.get_object.assert_called_once_with(Bucket=case.want_bucket, Key=case.want_key, **kwargs)


@dataclasses.dataclass
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


@dataclasses.dataclass
class FromConfigCase:
    cfg: StorageConfig
    want_aws_profile: str | None = None
    want_chunk_size: int = StorageConfig.DEFAULT_CHUNK_SIZE


def storage_config(**kwargs: Any) -> StorageConfig:
    cfg = StorageConfig()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


@cases(
    default=FromConfigCase(storage_config()),
    chunk_size=FromConfigCase(storage_config(chunk_size=10), want_chunk_size=10),
    aws_profile=FromConfigCase(storage_config(aws_profile="foo"), want_aws_profile="foo"),
)
def test_from_config(case: FromConfigCase) -> None:
    adapter = S3Adapter.from_config(case.cfg)
    assert isinstance(adapter, S3Adapter)
    assert adapter.chunk_size == case.want_chunk_size
    assert adapter.aws_profile == case.want_aws_profile


def _client_error(code: str, status_code: int = 400, operation_name: str = "GetObject") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        error_response={
            "Error": {"Code": code, "Message": "some message"},
            "ResponseMetadata": {
                "RequestId": "some-request-id",
                "HostId": "some-host-id",
                "HTTPStatusCode": status_code,
                "HTTPHeaders": {},
                "RetryAttempts": 0,
            },
        },
        operation_name=operation_name,
    )


@dataclasses.dataclass
class TransientErrorCase:
    error: Exception
    want_error: type[Exception] = ServiceUnavailableError


@cases(
    # Genuine transient failures of the S3 service itself: `Client` fails over to another mirror, and additionally
    # reduces the number of concurrent connections allowed to this server, assuming it is overwhelmed.
    throttling=TransientErrorCase(_client_error("SlowDown"), want_error=ServiceUnavailableError),
    internal_error=TransientErrorCase(_client_error("InternalError"), want_error=ServiceUnavailableError),
    server_error_status=TransientErrorCase(_client_error("SomeOtherCode", status_code=503), want_error=ServiceUnavailableError),
    # Local/client-side connectivity or credentials problems: `Client` still fails over to another mirror, but does
    # not assume the server itself is overwhelmed, so it leaves the connection count for this server untouched.
    no_credentials=TransientErrorCase(botocore.exceptions.NoCredentialsError(), want_error=ClientUnavailableError),
    endpoint_connection=TransientErrorCase(
        botocore.exceptions.EndpointConnectionError(endpoint_url=SOME_URL), want_error=ClientUnavailableError
    ),
)
def test_head_translates_transient_errors(case: TransientErrorCase, s3_client) -> None:
    s3_client.head_object.side_effect = case.error
    adapter = S3Adapter(s3_client=s3_client)
    with pytest.raises(case.want_error):
        adapter.head(SOME_URL)


@cases(
    throttling=TransientErrorCase(_client_error("SlowDown"), want_error=ServiceUnavailableError),
    internal_error=TransientErrorCase(_client_error("InternalError"), want_error=ServiceUnavailableError),
    server_error_status=TransientErrorCase(_client_error("SomeOtherCode", status_code=503), want_error=ServiceUnavailableError),
    no_credentials=TransientErrorCase(botocore.exceptions.NoCredentialsError(), want_error=ClientUnavailableError),
    endpoint_connection=TransientErrorCase(
        botocore.exceptions.EndpointConnectionError(endpoint_url=SOME_URL), want_error=ClientUnavailableError
    ),
)
def test_get_translates_transient_errors(case: TransientErrorCase, s3_client) -> None:
    s3_client.get_object.side_effect = case.error
    adapter = S3Adapter(s3_client=s3_client)
    with pytest.raises(case.want_error):
        adapter.get(SOME_URL)


def test_get_does_not_translate_non_transient_client_errors(s3_client) -> None:
    # An "AccessDenied" (403) response is not a transient failure: it should keep being raised as-is instead of
    # being turned into a `ServiceUnavailableError` (which would make `Client` keep retrying other mirrors forever
    # for what is actually a configuration/permission problem).
    error = _client_error("AccessDenied", status_code=403)
    s3_client.get_object.side_effect = error
    adapter = S3Adapter(s3_client=s3_client)
    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        adapter.get(SOME_URL)
    assert excinfo.value is error


def test_head_does_not_translate_non_transient_client_errors(s3_client) -> None:
    error = _client_error("AccessDenied", status_code=403, operation_name="HeadObject")
    s3_client.head_object.side_effect = error
    adapter = S3Adapter(s3_client=s3_client)
    with pytest.raises(botocore.exceptions.ClientError) as excinfo:
        adapter.head(SOME_URL)
    assert excinfo.value is error


def test_s3_client_creation_is_thread_safe(monkeypatch) -> None:
    # It simulates several threads (as used by the multipart transfer manager) accessing the lazily created S3
    # client of the very same `S3Adapter` instance for the first time, concurrently. It asserts that only one
    # underlying `boto3.Session` (and client) gets created, and that every thread ends up using that same client.
    created_sessions: list[Any] = []
    session_creation_started = threading.Event()
    release_session_creation = threading.Event()

    class FakeSession:
        def __init__(self, profile_name=None):
            created_sessions.append(self)
            session_creation_started.set()
            # It gives every other thread a chance to reach the lock before this one proceeds, to maximize
            # the chances of exposing a race condition if the lazy initialization wasn't thread-safe.
            release_session_creation.wait(timeout=5)
            self.profile_name = profile_name

        def get_credentials(self):
            return None

        def client(self, name):
            assert name == "s3"
            return mock.Mock(name=f"s3-client-{len(created_sessions)}")

    monkeypatch.setattr(boto3, "Session", FakeSession)

    adapter = S3Adapter()
    results: list[S3Client] = []
    results_lock = threading.Lock()

    def worker():
        client = adapter._s3
        with results_lock:
            results.append(client)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    session_creation_started.wait(timeout=5)
    release_session_creation.set()
    for t in threads:
        t.join(timeout=5)

    assert len(created_sessions) == 1
    assert len(results) == 8
    assert len({id(r) for r in results}) == 1


def test_s3_client_warms_up_credentials_once(monkeypatch) -> None:
    credentials = mock.Mock()
    session = mock.Mock()
    session.get_credentials.return_value = credentials
    monkeypatch.setattr(boto3, "Session", mock.Mock(return_value=session))

    adapter = S3Adapter()
    _ = adapter._s3
    _ = adapter._s3

    session.get_credentials.assert_called_once()
    credentials.get_frozen_credentials.assert_called_once()
    session.client.assert_called_once_with("s3")


def test_s3_client_creation_survives_credentials_warm_up_failure(monkeypatch) -> None:
    session = mock.Mock()
    session.get_credentials.side_effect = botocore.exceptions.NoCredentialsError()
    monkeypatch.setattr(boto3, "Session", mock.Mock(return_value=session))

    adapter = S3Adapter()
    # Warming up credentials must never prevent the client from being created: any persistent credentials problem
    # will simply surface (and be translated to `ClientUnavailableError`) on the first real request instead.
    client = adapter._s3
    assert client is session.client.return_value
