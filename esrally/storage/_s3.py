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

import fnmatch
import logging
import os
import re
import typing
import urllib.parse
from collections.abc import Mapping
from datetime import datetime

import boto3
import botocore.exceptions
from requests.structures import CaseInsensitiveDict

from esrally.storage._adapter import Head, Readable
from esrally.storage._http import HTTPAdapter

_HTTPS_URL_RE = re.compile(fnmatch.translate("https://*.amazonaws.com/"))

LOG = logging.getLogger(__name__)


class S3Adapter(HTTPAdapter):
    """Adapter class for s3 scheme protocol"""

    _s3_client = None

    @property
    def _s3(self):
        if self._s3_client is None:
            self._s3_client = boto3.Session(profile_name="okta-elastic-dev").client("s3")
        return self._s3_client

    @classmethod
    def match_url(cls, url: str) -> str:
        if url.startswith("s3://") or _HTTPS_URL_RE.match(url):
            return S3Address.from_url(url).url(scheme="https")
        raise NotImplementedError

    @classmethod
    def _date_to_headers(cls, date: datetime | None, headers: CaseInsensitiveDict) -> None:
        if date is not None:
            headers["x-amz-date"] = date.strftime("%Y-%m-%dT%H:%M:%SZ")

    def put(self, stream: Readable, url: str, head: Head | None = None, headers: Mapping[str, str] | None = None) -> Head:
        if headers:
            raise NotImplementedError("passing headers is not supported")

        address = S3Address.from_url(url)
        if head is not None:
            if head.ranges:
                raise NotImplementedError("ranges are not supported")

        try:
            self._s3.upload_fileobj(stream, address.bucket, address.key)
        except botocore.exceptions.ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchBucket":
                LOG.exception("Error uploading file to S3 service: %s", ex)
                self._s3.create_bucket(
                    Bucket=address.bucket,
                    ACL="public-read-write",
                    CreateBucketConfiguration={
                        "LocationConstraint": "eu-central-1",
                    },
                )
            raise

        ret = self.head(url)
        if head is not None:
            head.check(ret)
        return ret


class S3Address(typing.NamedTuple):

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
