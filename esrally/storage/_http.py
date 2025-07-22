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
from typing import Any, TypeVar

import requests
import requests.adapters
import urllib3
from requests.structures import CaseInsensitiveDict

from esrally.storage._adapter import Adapter, Head, ServiceUnavailableError, Writable
from esrally.storage._range import NO_RANGE, RangeSet, rangeset
from esrally.types import Config

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
                max_retries = urllib3.Retry(**json.loads(max_retries_text))
        return cls(max_retries=max_retries)

    def __init__(self, max_retries: urllib3.Retry | int | None = 0) -> None:
        super().__init__()
        if max_retries:
            adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
            self.mount("http://", adapter)
            self.mount("https://", adapter)


_CONTENT_RANGE_PREFIX = "bytes "
_AMZ_CHECKSUM_PREFIX = "x-amz-checksum-"
_GOOG_HASH_KEY = "X-Goog-Hash"

A = TypeVar("A", "HTTPAdapter", "HTTPAdapter")


class HTTPAdapter(Adapter):
    """It implements the `Adapter` interface for http(s) protocols using the requests library."""

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("http://") or url.startswith("https://")

    @classmethod
    def from_config(cls: type[A], cfg: Config, **kwargs: dict[str, Any]) -> A:
        assert issubclass(cls, HTTPAdapter)
        chunk_size = int(cfg.opts("storage", "storage.http.chunk_size", CHUNK_SIZE, False))
        kwargs.setdefault("chunk_size", chunk_size)  # type: ignore
        session = Session.from_config(cfg)
        kwargs.setdefault("session", session)  # type: ignore
        return super().from_config(cfg, **kwargs)

    def __init__(self, session: requests.Session | None = None, chunk_size: int = CHUNK_SIZE):
        if session is None:
            session = requests.session()
        self.chunk_size = chunk_size
        self.session = session

    def head(self, url: str) -> Head:
        with self.session.head(url, allow_redirects=True) as res:
            if res.status_code == 404:
                raise FileNotFoundError(f"Can't get file head: {url}")
            res.raise_for_status()
        return self._head_from_headers(url, res.headers)

    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        headers: MutableMapping[str, str] = CaseInsensitiveDict()
        self._head_to_headers(head, headers)
        with self.session.get(url, stream=True, allow_redirects=True, headers=headers) as res:
            if res.status_code == 503:
                raise ServiceUnavailableError()
            res.raise_for_status()

            got = self._head_from_headers(url, res.headers)
            if head is not None:
                head.check(got)

            for chunk in res.iter_content(self.chunk_size):
                if chunk:
                    stream.write(chunk)
        return got

    @classmethod
    def _head_to_headers(cls, head: Head | None, headers: MutableMapping[str, str]) -> None:
        if head is not None:
            cls._date_to_headers(head.date, headers)
            cls._ranges_to_headers(head.ranges, headers)

    @classmethod
    def _date_to_headers(cls, date: datetime | None, headers: MutableMapping[str, str]) -> None:
        if date is not None:
            raise NotImplementedError("date is not implemented yet")

    _RANGE_HEADER = "Range"

    @classmethod
    def _ranges_to_headers(cls, ranges: RangeSet, headers: MutableMapping[str, str]) -> None:
        if not ranges:
            return
        if len(ranges) > 1:
            raise NotImplementedError(f"unsupported multi range requests: ranges are {ranges}")
        headers[cls._RANGE_HEADER] = f"bytes={ranges[0]}"

    @classmethod
    def _head_from_headers(cls, url: str, headers: Mapping[str, str]) -> Head:
        accept_ranges = cls._accept_ranges_from_headers(headers)
        content_length = cls._content_length_from_headers(headers)
        ranges, document_length = cls._content_range_from_headers(headers)
        crc32c = cls._hashes_from_headers(headers).get("crc32c")
        return Head(
            url=url,
            content_length=content_length,
            accept_ranges=accept_ranges,
            ranges=ranges,
            document_length=document_length,
            crc32c=crc32c,
        )

    _CONTENT_LENGTH_HEADER = "Content-Length"

    @classmethod
    def _content_length_from_headers(cls, headers: Mapping[str, str]) -> int | None:
        value = headers.get(cls._CONTENT_LENGTH_HEADER, None)
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
        try:
            return int(value)
        except (ValueError, TypeError):
            raise ValueError(f"invalid content length value: {value}") from None

    _ACCEPT_RANGES_HEADER = "Accept-Ranges"

    @classmethod
    def _accept_ranges_from_headers(cls, headers: Mapping[str, str]) -> bool | None:
        got = headers.get(cls._ACCEPT_RANGES_HEADER, "").strip()
        if not got:
            return None
        return got == "bytes"

    _CONTENT_RANGE_HEADER = "Content-Range"

    @classmethod
    def _content_range_from_headers(cls, headers: Mapping[str, str]) -> tuple[RangeSet, int | None]:
        content_range_text = headers.get(cls._CONTENT_RANGE_HEADER, "").strip()
        if not content_range_text:
            return NO_RANGE, None

        if not content_range_text.startswith(_CONTENT_RANGE_PREFIX):
            raise NotImplementedError(f"Unsupported prefix for 'content-range' header: {content_range_text}")

        if "," in content_range_text:
            raise NotImplementedError("Multi range value for 'content-range' header is not supported.")

        try:
            range_text, document_length_text = content_range_text[len(_CONTENT_RANGE_PREFIX) :].replace(" ", "").split("/", 1)
        except ValueError:
            raise ValueError(f"Invalid value for 'content-range' header: '{content_range_text}'") from None

        try:
            document_length = int(document_length_text)
        except ValueError:
            document_length = None
            if document_length_text.strip() != "*":
                raise ValueError(f"Invalid value for 'content-length' header: '{document_length_text}'") from None
        else:
            if document_length is not None and document_length < 0:
                raise ValueError(f"Value for 'content-length' header: '{document_length}' < 0")
        return rangeset(range_text), document_length

    @classmethod
    def _hashes_from_headers(cls, headers: Mapping[str, str]) -> dict[str, str]:
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
