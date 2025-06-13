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
from urllib.error import HTTPError

import requests
import requests.adapters
import urllib3
import yaml
from requests.structures import CaseInsensitiveDict

from esrally.config import Config
from esrally.storage._adapter import Adapter, Head, ServiceUnavailableError, Writable
from esrally.storage._range import NO_RANGE, RangeSet, rangeset

LOG = logging.getLogger(__name__)


# Size of the buffers used for file transfer content.
CHUNK_SIZE = 1 * 1024 * 1024

# It limits the maximum number of connection retries.
MAX_RETRIES = 10


class Session(requests.Session):

    @classmethod
    def from_config(cls, cfg: Config) -> Session:
        max_retries: urllib3.Retry | int | None = 0
        max_retries_text = cfg.opts("storage", "storage.http.max_retries", MAX_RETRIES, mandatory=False)
        if max_retries_text:
            try:
                max_retries = int(max_retries_text)
            except ValueError:
                max_retries = urllib3.Retry(**yaml.safe_load(max_retries_text))
        return cls(max_retries=max_retries)

    def __init__(self, max_retries: urllib3.Retry | int | None = 0) -> None:
        super().__init__()
        if max_retries:
            adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
            self.mount("http://", adapter)
            self.mount("https://", adapter)


class HTTPAdapter(Adapter):
    """It implements the adapter interface for http(s) protocols using the requests library."""

    __adapter_prefixes__ = ("http://", "https://")

    @classmethod
    def from_config(cls, cfg: Config) -> Adapter:
        chunk_size = int(cfg.opts("storage", "storage.http.chunk_size", CHUNK_SIZE, False))
        return HTTPAdapter(session=Session.from_config(cfg), chunk_size=chunk_size)

    def __init__(self, session: requests.Session | None = None, chunk_size: int = CHUNK_SIZE):
        if session is None:
            session = requests.session()
        self.chunk_size = chunk_size
        self.session = session

    def head(self, url: str) -> Head:
        with self.session.head(url, allow_redirects=True) as res:
            try:
                res.raise_for_status()
            except HTTPError as ex:
                raise FileNotFoundError(f"can't get file head: {url}") from ex
            return head_from_headers(url, res.headers)

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        headers = ranges_to_headers(ranges)
        with self.session.get(url, stream=True, allow_redirects=True, headers=headers) as res:
            if res.status_code == 503:
                raise ServiceUnavailableError()
            res.raise_for_status()

            head = head_from_headers(url, res.headers)
            if head.ranges != ranges:
                raise ValueError(f"unexpected content range in server response: got {head.ranges}, want: {ranges}")

            for chunk in res.iter_content(self.chunk_size):
                if chunk:
                    stream.write(chunk)
        return head


def ranges_to_headers(ranges: RangeSet, headers: CaseInsensitiveDict | None = None) -> CaseInsensitiveDict:
    if headers is None:
        headers = CaseInsensitiveDict()
    if not ranges:
        return headers
    if len(ranges) > 1:
        raise NotImplementedError(f"unsupported multi range requests: ranges are {ranges}")
    headers["range"] = f"bytes={ranges[0]}"
    return headers


def content_length_from_headers(headers: CaseInsensitiveDict) -> int | None:
    value = headers.get("content-length", "*").strip()
    if value == "*":
        return None
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"invalid content-length value: {value}") from None


def accept_ranges_from_headers(headers: CaseInsensitiveDict) -> bool | None:
    got = headers.get("accept-ranges", "").strip()
    if not got:
        return None
    return got == "bytes"


def content_range_from_headers(headers: CaseInsensitiveDict) -> tuple[RangeSet, int | None]:
    content_range_text = headers.get("content-range", "").strip()
    if not content_range_text:
        return NO_RANGE, None

    if not content_range_text.startswith("bytes "):
        raise ValueError(f"invalid content range: {content_range_text}")

    if "," in content_range_text:
        raise ValueError("multi range value is not unsupported")

    try:
        value, content_length_text = content_range_text[len("bytes ") :].strip().replace(" ", "").split("/", 1)
        content_length: int | None = None
        if content_length_text != "*":
            content_length = int(content_length_text)
        return rangeset(value), content_length
    except ValueError:
        raise ValueError(f"invalid content range in '{content_range_text}'")


def head_from_headers(url: str, headers: CaseInsensitiveDict) -> Head:
    ranges, _ = content_range_from_headers(headers)
    content_length = content_length_from_headers(headers)
    accept_ranges = accept_ranges_from_headers(headers)
    return Head.create(url=url, content_length=content_length, accept_ranges=accept_ranges, ranges=ranges)
