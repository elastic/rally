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

import logging
import os
import threading
import urllib.parse
from collections.abc import Mapping
from typing import Any, NamedTuple, Protocol, runtime_checkable

import boto3
import botocore.exceptions
from botocore.response import StreamingBody
from typing_extensions import Self

from esrally import types
from esrally.storage import Adapter, GetResponse, Head, StorageConfig
from esrally.storage._adapter import ServiceUnavailableError
from esrally.storage.http import (
    head_to_headers,
    parse_accept_ranges,
    parse_content_range,
    parse_hashes_from_headers,
)

LOG = logging.getLogger(__name__)

# botocore error codes that represent a transient/retryable failure of the S3 service (as opposed to e.g. an
# invalid request or a missing object). They are translated to `ServiceUnavailableError` so that `Client` can fail
# over to another mirror (or the unauthenticated source URL) instead of aborting the whole transfer.
_RETRYABLE_CLIENT_ERROR_CODES = frozenset(
    {
        "InternalError",
        "RequestTimeout",
        "RequestTimeoutException",
        "PriorRequestNotComplete",
        "ServiceUnavailable",
        "SlowDown",
        "Throttling",
        "ThrottlingException",
        "ThrottledException",
        "RequestLimitExceeded",
        "TooManyRequestsException",
    }
)


class S3Adapter(Adapter):
    """Adapter class for s3:// scheme protocol"""

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("s3://")

    @classmethod
    def from_config(cls, cfg: types.Config | None = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        return cls(aws_profile=cfg.aws_profile, chunk_size=cfg.chunk_size)

    def __init__(
        self,
        aws_profile: str | None = StorageConfig.DEFAULT_AWS_PROFILE,
        chunk_size: int = StorageConfig.DEFAULT_CHUNK_SIZE,
        s3_client: S3Client | None = None,
    ) -> None:
        if chunk_size < 0:
            raise ValueError("Chunk size must be positive")
        self.chunk_size = chunk_size
        self.aws_profile = aws_profile
        self._s3_client = s3_client
        # It protects lazy S3 client creation (and the initial credentials resolution) from being executed
        # concurrently by more than one thread at once. The multipart transfer manager downloads a single file
        # from several threads at the same time, all of them sharing this very same `S3Adapter` instance.
        self._lock = threading.Lock()

    def head(self, url: str) -> Head:
        address = S3Address.from_url(url)
        try:
            res = self._s3.head_object(Bucket=address.bucket, Key=address.key)
        except botocore.exceptions.ClientError as ex:
            raise _translate_client_error(ex) from ex
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.EndpointConnectionError) as ex:
            raise ServiceUnavailableError(str(ex)) from ex
        return head_from_response(url, res)

    def get(self, url: str, *, check_head: Head | None = None) -> GetResponse:
        headers: dict[str, Any] = {}
        head_to_headers(check_head, headers)

        address = S3Address.from_url(url)
        try:
            response = self._s3.get_object(Bucket=address.bucket, Key=address.key, **headers)
        except botocore.exceptions.ClientError as ex:
            raise _translate_client_error(ex) from ex
        except (botocore.exceptions.NoCredentialsError, botocore.exceptions.EndpointConnectionError) as ex:
            # Getting credentials can intermittently fail when many threads are requesting them at the same time
            # (for example while downloading many parts of the same file concurrently via the multipart transfer
            # manager). It is treated as a transient failure so that `Client` can fail over to another mirror (or
            # to the unauthenticated source URL) instead of aborting the whole transfer.
            raise ServiceUnavailableError(str(ex)) from ex

        body: StreamingBody | None = response.get("Body")
        if body is None:
            raise RuntimeError("S3 client returned no body.")

        try:
            head = head_from_response(url, response)
            if check_head is not None:
                check_head.check(head)
        except Exception:
            body.close()
            raise

        def iter_chunks():
            try:
                with body:
                    yield from body.iter_chunks(self.chunk_size)
            except (botocore.exceptions.NoCredentialsError, botocore.exceptions.EndpointConnectionError) as ex:
                raise ServiceUnavailableError(str(ex)) from ex
            except botocore.exceptions.ClientError as ex:
                raise _translate_client_error(ex) from ex

        return GetResponse(head, iter_chunks())

    @property
    def _s3(self) -> S3Client:
        if self._s3_client is None:
            with self._lock:
                # It re-checks the condition once the lock has been acquired as another thread could have created
                # the client while this thread was waiting to acquire the lock.
                if self._s3_client is None:
                    session = boto3.Session(profile_name=self.aws_profile)
                    # It resolves (and caches) credentials once, synchronously, before any request is sent. Several
                    # worker threads from the multipart transfer manager might otherwise all try to resolve
                    # credentials for the very first time concurrently (for example fetching them from the EC2
                    # instance metadata service), which has been observed to intermittently raise
                    # `NoCredentialsError`. Resolving them here, while still holding `self._lock`, ensures it only
                    # happens once from a single thread.
                    self._warm_credentials(session)
                    self._s3_client = session.client("s3")
        return self._s3_client

    @staticmethod
    def _warm_credentials(session: boto3.Session) -> None:
        try:
            credentials = session.get_credentials()
            if credentials is not None:
                credentials.get_frozen_credentials()
        except Exception as ex:
            # It intentionally swallows any error here: the failure (if it persists) will be raised again, and
            # handled, on the first actual S3 request performed through this client.
            LOG.debug("failed warming up AWS credentials: %s", ex)


def _translate_client_error(ex: botocore.exceptions.ClientError) -> Exception:
    """It translates a botocore `ClientError` into a `ServiceUnavailableError` when it represents a transient
    failure of the S3 service, so that the caller can fail over to another mirror. Any other error (for example a
    permission or a not-found error) is returned unmodified so that it keeps being reported as-is."""
    error = ex.response.get("Error", {})
    code = error.get("Code", "")
    status_code = ex.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if code in _RETRYABLE_CLIENT_ERROR_CODES or (isinstance(status_code, int) and status_code >= 500):
        return ServiceUnavailableError(str(ex))
    return ex


class S3Address(NamedTuple):

    bucket: str
    key: str
    region: str = ""

    @classmethod
    def from_url(cls, url: str, region: str = "") -> S3Address:
        url = url.strip()
        if not url:
            raise ValueError("unspecified remote file url")
        u = urllib.parse.urlparse(url, scheme="s3")
        if u.scheme not in ("s3", "https"):
            raise ValueError(f"invalid URL scheme '{url}'")

        if u.scheme == "s3":
            bucket = u.netloc
        elif u.scheme == "https":
            bucket, right = u.netloc.split(".s3.", 1)
            if not right.endswith("amazonaws.com"):
                raise ValueError(f"https URL doesn't ends with 'amazonaws.com': '{url}'")
            region = right[: -len("amazonaws.com")].rstrip(".")
        else:
            raise ValueError(f"invalid URL scheme '{url}'")

        key = os.path.normpath(u.path).strip("/")
        if not key:
            raise ValueError(f"unspecified object key in url: {url}")
        return S3Address(bucket=bucket, key=key, region=region)

    def host(self, scheme: str = "s3") -> str:
        if scheme == "s3":
            return self.bucket
        elif scheme == "https":
            if self.region:
                return f"{self.bucket}.s3.{self.region}.amazonaws.com"
            else:
                return f"{self.bucket}.s3.amazonaws.com"
        else:
            raise ValueError(f"unsupported scheme '{scheme}'")

    def url(self, scheme: str = "s3") -> str:
        netloc = self.host(scheme=scheme)
        return urllib.parse.urlunparse((scheme, netloc, self.key, "", "", ""))


_ACCEPT_RANGES_HEADER = "AcceptRanges"
_CONTENT_LENGTH_HEADER = "ContentLength"
_CONTENT_RANGE_HEADER = "ContentRange"
_CRC32C_HEADER = "Crc32c"
_RANGE_HEADER = "Range"


def head_from_response(url: str, response: Mapping[str, Any]) -> Head:
    accept_ranges = parse_accept_ranges(response.get(_ACCEPT_RANGES_HEADER, ""))
    content_length = response.get(_CONTENT_LENGTH_HEADER)
    ranges, document_length = parse_content_range(response.get(_CONTENT_RANGE_HEADER, ""))
    crc32 = parse_hashes_from_headers(response).get(_CRC32C_HEADER)
    return Head(
        url=url,
        accept_ranges=accept_ranges,
        content_length=content_length,
        ranges=ranges,
        document_length=document_length,
        crc32c=crc32,
    )


@runtime_checkable
class S3Client(Protocol):

    def head_object(self, Bucket: str, Key: str) -> Mapping[str, Any]: ...

    def get_object(self, Bucket: str, Key: str, **kwargs: Any) -> Mapping[str, Any]: ...
