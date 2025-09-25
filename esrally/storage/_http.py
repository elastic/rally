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

import json
import logging
from collections.abc import Mapping, MutableMapping
from datetime import datetime
from typing import Any

import requests
import requests.adapters
import urllib3
from requests.structures import CaseInsensitiveDict
from typing_extensions import Self

from esrally import types
from esrally.storage._adapter import Adapter, Head, ServiceUnavailableError, Writable
from esrally.storage._config import DEFAULT_STORAGE_CONFIG, StorageConfig
from esrally.storage._range import NO_RANGE, RangeSet, rangeset

LOG = logging.getLogger(__name__)


class Session(requests.Session):

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        return cls(max_retries=parse_max_retries(cfg.max_retries))

    def __init__(self, max_retries: urllib3.Retry | int | None = None) -> None:
        super().__init__()
        if max_retries:
            adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
            self.mount("http://", adapter)
            self.mount("https://", adapter)


class HTTPAdapter(Adapter):
    """It implements the `Adapter` interface for http(s) protocols using the requests library."""

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("http://") or url.startswith("https://")

    @classmethod
    def from_config(cls, cfg: types.AnyConfig = None) -> Self:
        cfg = StorageConfig.from_config(cfg)
        session = Session.from_config(cfg)
        return cls(session=session, chunk_size=cfg.chunk_size)

    def __init__(self, session: requests.Session | None = None, chunk_size: int = DEFAULT_STORAGE_CONFIG.chunk_size):
        if session is None:
            session = requests.Session()
        self.chunk_size = chunk_size
        self.session = session

    def head(self, url: str) -> Head:
        with self.session.head(url, allow_redirects=True) as res:
            if res.status_code == 404:
                raise FileNotFoundError(f"Can't get file head: {url}")
            res.raise_for_status()
        return head_from_headers(url, res.headers)

    def get(self, url: str, stream: Writable, want: Head | None = None) -> Head:
        headers: MutableMapping[str, str] = CaseInsensitiveDict()
        head_to_headers(want, headers)
        with self.session.get(url, stream=True, allow_redirects=True, headers=headers) as res:
            if res.status_code == 503:
                raise ServiceUnavailableError()
            res.raise_for_status()

            got = head_from_headers(url, res.headers)
            if want is not None:
                want.check(got)

            for chunk in res.iter_content(self.chunk_size):
                if chunk:
                    stream.write(chunk)
        return got


_ACCEPT_RANGES_HEADER = "Accept-Ranges"
_CONTENT_RANGE_HEADER = "Content-Range"
_CONTENT_LENGTH_HEADER = "Content-Length"


def head_from_headers(url: str, headers: Mapping[str, Any]) -> Head:
    accept_ranges = parse_accept_ranges(headers.get(_ACCEPT_RANGES_HEADER, ""))
    content_length = parse_content_length(headers.get(_CONTENT_LENGTH_HEADER, ""))
    ranges, document_length = parse_content_range(headers.get(_CONTENT_RANGE_HEADER, ""))
    crc32c = parse_hashes_from_headers(headers).get("crc32c")
    return Head(
        url=url,
        content_length=content_length,
        accept_ranges=accept_ranges,
        ranges=ranges,
        document_length=document_length,
        crc32c=crc32c,
    )


def head_to_headers(head: Head | None, headers: MutableMapping[str, str]) -> None:
    if head is not None:
        date_to_headers(head.date, headers)
        ranges_to_headers(head.ranges, headers)


def date_to_headers(date: datetime | None, headers: MutableMapping[str, str]) -> None:
    if date is not None:
        raise NotImplementedError("date is not implemented yet")


_RANGE_HEADER = "Range"


def ranges_to_headers(ranges: RangeSet, headers: MutableMapping[str, str]) -> None:
    if not ranges:
        return
    if len(ranges) > 1:
        # This will never be supported as notable services like S3 don't support it.
        raise NotImplementedError(f"unsupported multi range requests: ranges are {ranges}")
    headers[_RANGE_HEADER] = f"bytes={ranges[0]}"


def parse_accept_ranges(text: str) -> bool | None:
    got = text.strip()
    if not got:
        return None
    return got == "bytes"


def parse_content_length(text: str) -> int | None:
    text = text.strip()
    if not text:
        return None
    try:
        return int(text)
    except (ValueError, TypeError):
        raise ValueError(f"invalid content length value: {text}") from None


_CONTENT_RANGE_PREFIX = "bytes "


def parse_content_range(text: str) -> tuple[RangeSet, int | None]:
    text = text.strip()
    if not text:
        return NO_RANGE, None

    if not text.startswith(_CONTENT_RANGE_PREFIX):
        raise NotImplementedError(f"Unsupported prefix for 'content-range' header: {text}")

    if "," in text:
        raise NotImplementedError("Multi range value for 'content-range' header is not supported.")

    try:
        range_text, document_length_text = text[len(_CONTENT_RANGE_PREFIX) :].replace(" ", "").split("/", 1)
    except ValueError:
        raise ValueError(f"Invalid value for content-range header: '{text}'") from None

    try:
        document_length = int(document_length_text)
    except ValueError as ex:
        if document_length_text != "*":
            raise ValueError(f"Invalid document length for content range: '{text}', {ex}") from None
        document_length = None
    else:
        assert isinstance(document_length, int)
        if document_length < 0:
            raise ValueError(f"Invalid document length for content range: '{document_length}' < 0")

    try:
        ranges = rangeset(range_text)
    except ValueError as ex:
        raise ValueError(f"Invalid range for content range: '{range_text}', {ex}") from None

    return ranges, document_length


_AMZ_CHECKSUM_PREFIX = "x-amz-checksum-"
_GOOG_HASH_KEY = "X-Goog-Hash"


def parse_hashes_from_headers(headers: Mapping[str, Any]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    hashes_text = headers.get(_GOOG_HASH_KEY, "").replace(" ", "")
    if hashes_text:
        for entry in hashes_text.split(","):
            if "=" not in entry:
                raise ValueError(f"invalid X-Goog-Hash value: {hashes_text}") from None
            name, value = entry.split("=", 1)
            hashes[name] = value

    for k, value in headers.items():
        if k.startswith(_AMZ_CHECKSUM_PREFIX):
            name = k[len(_AMZ_CHECKSUM_PREFIX) :]
            if name == "type":
                continue
            hashes[name] = value
    return hashes


def parse_max_retries(max_retries: str | int | urllib3.Retry) -> urllib3.Retry | int:
    if isinstance(max_retries, str):
        max_retries = max_retries.strip()
        if not max_retries:
            max_retries = DEFAULT_STORAGE_CONFIG.max_retries
        try:
            max_retries = int(max_retries)
        except ValueError:
            assert isinstance(max_retries, str)
            max_retries = urllib3.Retry(**json.loads(max_retries))
    if isinstance(max_retries, int):
        if max_retries <= 0:
            raise ValueError("max_retries must be a positive integer")
    return max_retries
