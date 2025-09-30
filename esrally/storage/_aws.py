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
import logging
import os
import urllib.parse
from collections.abc import Mapping
from typing import Any, NamedTuple, Optional, Protocol, runtime_checkable

import boto3
from botocore.response import StreamingBody
from typing_extensions import Self

from esrally.storage._adapter import Adapter, Head, Writable
from esrally.storage._http import (
    CHUNK_SIZE,
    head_to_headers,
    parse_accept_ranges,
    parse_content_range,
    parse_hashes_from_headers,
)
from esrally.types import Config

LOG = logging.getLogger(__name__)

AWS_PROFILE: str | None = None


class S3Adapter(Adapter):
    """Adapter class for s3:// scheme protocol"""

    @classmethod
    def from_config(cls, cfg: Config, chunk_size: int | None = None, aws_profile: str | None = None, **kwargs: Any) -> Self:
        if aws_profile is None:
            aws_profile = cfg.opts("storage", "storage.aws.profile", default_value=AWS_PROFILE, mandatory=False)
        if chunk_size is None:
            chunk_size = int(cfg.opts("storage", "storage.http.chunk_size", CHUNK_SIZE, False))
        return super().from_config(cfg, aws_profile=aws_profile, chunk_size=chunk_size, **kwargs)

    def __init__(
        self,
        aws_profile: str | None = AWS_PROFILE,
        chunk_size: int = CHUNK_SIZE,
        s3_client: Optional["S3Client"] = None,
    ) -> None:
        self.chunk_size = chunk_size
        self.aws_profile = aws_profile
        self._s3_client = s3_client

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("s3://")

    def head(self, url: str) -> Head:
        address = S3Address.from_url(url)
        res = self._s3.head_object(Bucket=address.bucket, Key=address.key)
        return head_from_response(url, res)

    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        headers: dict[str, Any] = {}
        head_to_headers(head, headers)

        address = S3Address.from_url(url)
        res = self._s3.get_object(Bucket=address.bucket, Key=address.key, **headers)
        ret = head_from_response(url, res)
        if head is not None:
            head.check(ret)
        body: StreamingBody | None = res.get("Body")
        if body is None:
            raise RuntimeError("S3 client returned no body.")
        for chunk in body.iter_chunks(self.chunk_size):
            if chunk:
                stream.write(chunk)
        return ret

    _s3_client = None

    @property
    def _s3(self) -> "S3Client":
        if self._s3_client is None:
            self._s3_client = boto3.Session(profile_name=self.aws_profile).client("s3")
        return self._s3_client


class S3Address(NamedTuple):

    bucket: str
    key: str
    region: str = ""

    @classmethod
    def from_url(cls, url: str, region: str = "") -> "S3Address":
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
