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

import dataclasses
import logging
import os
import threading
import types
from typing import BinaryIO

from typing_extensions import Self

from esrally.storage._range import NO_RANGE, RangeError, RangeSet

LOG = logging.getLogger(__name__)


class StreamClosedError(Exception):
    pass


@dataclasses.dataclass
class FileDescriptor:

    path: str
    ranges: RangeSet = NO_RANGE
    fd: BinaryIO | None = None
    lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)
    position: int = 0

    @classmethod
    def create(cls, fd: BinaryIO, path: str, ranges: RangeSet = NO_RANGE) -> Self:
        if fd is None:
            raise ValueError("fd can't be None.")
        if len(ranges) > 1:
            raise ValueError("ranges must be continuous.")
        if ranges and ranges.start > 0:
            fd.seek(ranges.start)
        return cls(path=path, fd=fd, ranges=ranges, position=fd.tell())

    @property
    def transferred(self) -> int:
        with self.lock:
            start_position = self.ranges and self.ranges.start or 0
            return self.position - start_position

    def close(self):
        with self.lock:
            fd, self.fd = self.fd, None
        if fd:
            try:
                fd.close()
            except Exception as ex:
                LOG.error("Failed to close file descriptor: %s", ex)

    def writable(self) -> bool:
        return self.fd is not None and self.fd.writable()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: type[Exception], exc_val: BaseException, exc_tb: types.TracebackType) -> None:
        self.close()


@dataclasses.dataclass
class FileWriter(FileDescriptor):
    """OutputStream provides an output stream wrapper able to prevent exceeding writing given range."""

    @classmethod
    def open(cls, path: str, ranges: RangeSet = NO_RANGE) -> Self:
        """It opens a local binary file for writing yielding a file object eventually constrained by a continuous range set.

        Opened file object is thread-safe.
        """
        if os.path.isfile(path):
            fd = open(path, "r+b")
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd = open(path, "w+b")  # pylint: disable=consider-using-with
        assert fd.writable(), "fd must be writable"
        return cls.create(path=path, ranges=ranges, fd=fd)

    def write(self, data: bytes) -> int:
        """It writes data bytes to target file.

        :param data:
        :return: None

        """
        written: int = 0
        chunk = data
        with self.lock:
            if self.fd is None:
                raise StreamClosedError("Stream has been closed.")
            if self.ranges:
                # It prevents exceeding file data range.
                chunk = chunk[: self.ranges.end - self.position]
            if chunk:
                written = self.fd.write(chunk)
                if written is None:
                    # Some implementations could return `None` instead of the count of written bytes.
                    written = len(chunk)
                self.position += written
                if len(chunk) != written:
                    raise RuntimeError(f"Failed to write the whole chunk (written {written} of {len(chunk)} bytes).")
            if len(data) > len(chunk):
                raise RangeError(
                    f"Error writing to file '{self.path}': data size exceeds file range {self.ranges} ({len(data)} > {len(chunk)})",
                    self.ranges,
                )
        return written

    def flush(self):
        with self.lock:
            if self.fd is not None:
                self.fd.flush()
