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
from collections.abc import Mapping
from typing import Final
from urllib.error import HTTPError

import requests
import requests.adapters
import urllib3

from esrally.storage._adapter import (
    Adapter,
    Head,
    Readable,
    ServiceUnavailableError,
    Writable,
)
from esrally.storage._range import MAX_LENGTH, NO_RANGE, Range, RangeSet

LOG = logging.getLogger(__name__)


# Size of the buffers used for file transfer content.
CHUNK_SIZE = 1 * 1024 * 1024


class Retry(urllib3.Retry):
    DEFAULT_BACKOFF_MAX = 20


# It limits the maximum number of connection retries.
MAX_RETRIES_NUMBER = 10
MAX_RETRIES_BACKOFF_FACTOR = 1
MAX_RETRIES: Final[urllib3.Retry] = Retry(MAX_RETRIES_NUMBER, backoff_factor=MAX_RETRIES_BACKOFF_FACTOR)


class HTTPAdapter(Adapter):
    """It implements the adapter interface for http(s) protocols using the requests library."""

    __adapter_prefixes__ = ("http:", "https:")

    def __init__(self, session: requests.Session | None = None, max_retries: urllib3.Retry = MAX_RETRIES):
        if session is None:
            session = requests.session()
        self._session = session
        self._session.mount("http://", requests.adapters.HTTPAdapter(max_retries=max_retries))
        self._session.mount("https://", requests.adapters.HTTPAdapter(max_retries=max_retries))

    def head(self, url: str) -> Head:
        with self._session.head(url, allow_redirects=True) as r:
            try:
                r.raise_for_status()
            except HTTPError:
                raise FileNotFoundError(f"can't get file head: {url}")
            return head_from_headers(url, r.headers)

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE) -> Head:
        headers: dict[str, str] = {}
        ranges_to_headers(headers, ranges)
        with self._session.get(url, stream=True, allow_redirects=True, headers=headers) as r:
            if r.status_code == 503:
                raise ServiceUnavailableError()
            r.raise_for_status()

            head = head_from_headers(url, r.headers)
            if head.ranges != ranges:
                raise ValueError(f"unexpected content range in server response: got {head.ranges}, want: {ranges}")

            for chunk in r.iter_content(CHUNK_SIZE):
                if chunk:
                    stream.write(chunk)
        return head

    def put(self, url: str, stream: Readable, ranges: RangeSet = NO_RANGE) -> Head:
        headers: dict[str, str] = {}
        ranges_to_headers(headers, ranges)
        with self._session.put(url, allow_redirects=True, headers=headers, data=stream) as r:
            if r.status_code == 503:
                raise ServiceUnavailableError()
            r.raise_for_status()
            head = head_from_headers(url, r.headers)
            if head.ranges != ranges:
                raise ValueError(f"unexpected content range in server response: got {head.ranges}, want: {ranges}")

        return head


def ranges_to_headers(headers: dict[str, str], ranges: RangeSet = NO_RANGE) -> None:
    if len(ranges) > 1:
        raise NotImplementedError(f"unsupported multi range requests: range is {ranges}")
    for r in ranges:
        headers["range"] = f"bytes={r.start}-"
        if r.end != MAX_LENGTH:
            headers["range"] += f"{r.end - 1}"


def content_length_from_headers(headers: Mapping[str, str]) -> int | None:
    try:
        return int(headers.get("content-length", "*").strip())
    except ValueError:
        return None


def accept_ranges_from_headers(headers: Mapping[str, str]) -> bool:
    return headers.get("accept-ranges", "").strip() == "bytes"


def range_from_headers(headers: Mapping[str, str]) -> tuple[RangeSet, int | None]:
    content_range = headers.get("content-range", "").strip()
    if not content_range:
        return NO_RANGE, None

    if not content_range.startswith("bytes "):
        raise ValueError(f"unsupported content range: {content_range}")

    if "," in content_range:
        raise ValueError("multi range value is not unsupported")

    try:
        value = content_range[len("bytes ") :].strip().replace(" ", "")
        value, content_length_text = value.split("/", 1)
        value = value.strip()
        try:
            content_length = int(content_length_text)
        except ValueError:
            if content_length_text == "*":
                raise
            content_length = None

        start_text, end_text = value.split("-", 1)
        try:
            start = int(start_text)
        except ValueError:
            if start_text == "":
                raise
            start = 0
        try:
            end = int(end_text) + 1
        except ValueError:
            if end_text != "*":
                raise
            end = MAX_LENGTH
        return Range(start, end), content_length
    except ValueError as ex:
        raise ValueError(f"invalid content range in {content_range}: {ex}")


def head_from_headers(url: str, headers: Mapping[str, str]) -> Head:
    ranges, content_length = range_from_headers(headers)
    content_length = content_length_from_headers(headers) or content_length
    accept_ranges = accept_ranges_from_headers(headers)
    return Head.create(url=url, content_length=content_length, accept_ranges=accept_ranges, ranges=ranges)
