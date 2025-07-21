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
import urllib.parse
from datetime import datetime
from typing import Any, NamedTuple, TypeVar

import boto3
from requests.structures import CaseInsensitiveDict

from esrally.storage._adapter import Head, Readable, Writable
from esrally.storage._http import CHUNK_SIZE, HTTPAdapter, Session
from esrally.types import Config

LOG = logging.getLogger(__name__)

AWS_PROFILE: str | None = None

A = TypeVar("A", "S3Adapter", "S3Adapter")


class S3Adapter(HTTPAdapter):
    """Adapter class for s3 scheme protocol"""

    @classmethod
    def from_config(cls: type[A], cfg: Config, **kwargs: dict[str, Any]) -> A:
        assert issubclass(cls, S3Adapter)
        aws_profile = cfg.opts("storage", "storage.aws.profile", default_value=AWS_PROFILE, mandatory=False)
        return super().from_config(cfg, aws_profile=aws_profile, **kwargs)

    def __init__(self, session: Session | None = None, chunk_size: int = CHUNK_SIZE, aws_profile: str | None = AWS_PROFILE) -> None:
        super().__init__(session=session, chunk_size=chunk_size)
        self._aws_profile = aws_profile

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("s3://")

    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        ranges = ""
        if head:
            ranges = str(head.ranges)
        address = S3Address.from_url(url)
        headers = self._s3.get_object(Bucket=address.bucket, Key=address.key, Range=ranges)
        ret = self._make_head(url, CaseInsensitiveDict(headers))
        if head is not None:
            head.check(ret)
        return ret

    def head(self, url: str) -> Head:
        address = S3Address.from_url(url)
        headers = self._s3.head_object(Bucket=address.bucket, Key=address.key)
        return self._make_head(url, CaseInsensitiveDict(headers))

    def put(self, stream: Readable, url: str, head: Head | None = None) -> Head:
        if head is not None and head.ranges:
            raise NotImplementedError("Range headers is not supported.")

        address = S3Address.from_url(url)
        LOG.info("Uploading file to '%s'...", url)
        headers = self._s3.upload_fileobj(stream, address.bucket, address.key)
        LOG.info("File uploaded: '%s'.", url)

        ret = self._make_head(url, CaseInsensitiveDict(headers))
        if head is not None:
            head.check(ret)
        return ret

    _s3_client = None

    @property
    def _s3(self):
        if self._s3_client is None:
            self._s3_client = boto3.Session(profile_name=self._aws_profile).client("s3")
        return self._s3_client

    @classmethod
    def _date_to_headers(cls, date: datetime | None, headers: CaseInsensitiveDict) -> None:
        if date is not None:
            headers["x-amz-date"] = date.strftime("%Y-%m-%dT%H:%M:%SZ")


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
