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

import os
import threading
import typing
import urllib.parse
from typing import Any

import boto3
import boto3.s3.transfer

from esrally.storage._adapter import Adapter, Head, Readable, Writable
from esrally.storage._range import NO_RANGE, RangeSet


class S3Address(typing.NamedTuple):
    bucket: str
    key: str


class S3Adapter(Adapter):
    """Adapter class for s3 scheme protocol"""

    __adapter_URL_prefixes__ = "s3://"

    _local = threading.local()

    def __init__(self):
        self._config = boto3.s3.transfer.TransferConfig(use_threads=True)

    @classmethod
    def _s3(cls):
        try:
            return cls._local.s3
        except AttributeError:
            s3 = boto3.session.Session().resource("s3")
            cls._local.s3 = s3
            return s3

    def head(self, url: str) -> Head:
        addr = s3_address(url)
        return s3_head(url, self._s3().Object(addr.bucket, addr.key))

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        if ranges:
            raise NotImplementedError("this s3 implementation doesn't accepts ranges")
        addr = s3_address(url)
        head = s3_head(url, self._s3().Object(addr.bucket, addr.key))
        self._s3().download_fileobj(addr.bucket, addr.key, stream, Config=self._config)
        return head

    def put(self, url: str, stream: Readable, ranges: RangeSet = NO_RANGE) -> Head:
        if ranges:
            raise NotImplementedError("this s3 implementation doesn't accepts ranges")
        addr = s3_address(url)
        head = s3_head(url, self._s3().Object(addr.bucket, addr.key))
        self._s3().upload_fileobj(stream, addr.bucket, addr.key, Config=self._config)
        return head


def s3_url(addr: S3Address) -> str:
    return f"s3://{addr.bucket}/{addr.key}"


def s3_address(url: str) -> S3Address:
    url = url.strip()
    if not url:
        raise ValueError("unspecified remote file url")
    u = urllib.parse.urlparse(url, scheme="s3")
    if u.scheme != "s3":
        raise ValueError(f"unsupported scheme in url: {url}")

    bucket = u.netloc
    if not bucket:
        raise ValueError(f"unspecified bucket name in url: {url}")
    key = os.path.normpath(u.path).strip("/")
    if not key:
        raise ValueError(f"unspecified object key in url: {url}")
    return S3Address(bucket, key)


def s3_head(url: str, obj: Any) -> Head:
    return Head.create(url=url, content_length=obj.content_length)
