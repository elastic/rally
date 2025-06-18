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

import contextlib
import enum
import logging
import os
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from typing import Any, BinaryIO, Protocol

import yaml

from esrally.storage._adapter import Head, ServiceUnavailableError, Writable
from esrally.storage._range import (
    MAX_LENGTH,
    NO_RANGE,
    Range,
    RangeError,
    RangeSet,
    rangeset,
)
from esrally.utils import threads

LOG = logging.getLogger(__name__)


class TransferCancelled(Exception):
    """It is raised to communicate that the transfer has been canceled."""


class RetryTransfer(Exception):
    """It is raised to communicate the task should be retried."""


class TransferDirection(enum.Enum):
    DOWN = 1
    UP = 2


class TransferStatus(enum.Enum):
    INITIALIZED = 0
    QUEUED = 1
    DOING = 2
    DONE = 3
    CANCELLED = 4
    FAILED = 5


MULTIPART_SIZE = 8 * 1024 * 1024
MAX_CONNECTIONS = 16


class Executor(Protocol):
    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        raise NotImplementedError()


GetMethod = Callable[[str, Writable, RangeSet], Head]


class Transfer:

    def __init__(
        self,
        head: Head,
        path: str,
        executor: Executor,
        get: GetMethod,
        todo: RangeSet = NO_RANGE,
        multipart_size: int = MULTIPART_SIZE,
        max_connections: int = MAX_CONNECTIONS,
        resume: bool = True,
    ):
        max_connections = max(1, max_connections)
        if max_connections == 1 or not head.accept_ranges:
            multipart_size = MAX_LENGTH
        if not todo:
            todo = Range(0, MAX_LENGTH)
        if head.content_length is not None:
            todo &= Range(0, head.content_length)

        self._max_connections = max_connections
        self._path = path
        self._started = threads.TimedEvent()
        self._workers = threads.WaitGroup(max_count=max_connections)
        self._finished = threads.TimedEvent()
        self._multipart_size = multipart_size
        self._get = get
        self._head = head
        self._todo = todo
        self._fds: list[FileDescriptor] = []
        self._executor = executor
        self._errors = list[Exception]()
        self._lock = threading.Lock()
        self._resumed_size = 0
        if resume:
            self._resume_status()

    @property
    def todo(self) -> RangeSet:
        return self._todo

    @property
    def path(self) -> str:
        return self._path

    @property
    def url(self) -> str:
        return self._head.url

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @max_connections.setter
    def max_connections(self, value: int) -> None:
        value = max(1, value)
        delta = value - self._max_connections
        if delta:
            with self._lock:
                self._max_connections = value
                if delta > 0:
                    self.start()

    def start(self) -> bool:
        with self._lock:
            if not self._todo or self._finished:
                return False
            try:
                self._workers.add(1)
            except threads.WaitGroupLimitError:
                return False

            self._executor.submit(self._run)
            return True

    def _resume_status(self):
        if not os.path.isfile(self._path) or not os.path.isfile(self._path + ".status"):
            return

        with open(self.path + ".status") as fd:
            document = yaml.safe_load(fd)
            if not isinstance(document, Mapping):
                LOG.error("mismatching status file format: got %s, want dict", type(document))
                return

        url = str(document.get("url"))
        if url != self.url:
            LOG.warning("mismatching status file url: %s != %s", url, self._head.url)
            return

        content_length = document.get("content_length", MAX_LENGTH)
        if self.content_length != content_length:
            LOG.warning("mismatching content length: %s != %s", content_length, self._head.content_length)
            return

        transferred = rangeset(document.get("transferred", ""))
        if transferred:
            self._todo -= transferred
            self._resumed_size = transferred.size
            if not self._todo:
                self._finished.set()
            LOG.info("resumed from existing status: %s", self.info())

    def save_status(self):
        document = {
            "url": self.url,
            "content_length": self.content_length,
            "transferred": str(self.transferred),
        }
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        with open(self._path + ".status", "w") as fd:
            yaml.safe_dump(document, fd)

    def _run(self):
        if self._finished:
            return
        respawn = False
        try:
            if self._started.set():
                LOG.info("transfer started: %s, %s", self.method, self.url)
            with self._open() as fd:
                if fd is None:
                    LOG.debug("transfer done: %s, %s", self.method, self.url)
                    return
                if self._todo:
                    # It eventually spawns another thread for multipart transfer
                    self.start()
                assert isinstance(fd, FileWriter)
                self._get(self._head.url, fd, fd.ranges)
        except StreamClosedError as ex:
            LOG.info("task cancelled: %s, %s: %s", self.method, self.url, ex)
        except ServiceUnavailableError as ex:
            LOG.info("service limit reached: %s, %s, workers=%d: %s", self.method, self.url, self._workers.count, ex)
        except Exception as ex:
            LOG.exception("task failed: %s, %s", self.method, self.url)
            with self._lock:
                self._errors.append(ex)
            self.close()
        else:
            with self._lock:
                respawn = not self._errors and bool(self._todo) and self._workers.count <= self._max_connections
            if respawn:
                # It eventually re-spawn himself for multipart transfer
                self._executor.submit(self._run)
        finally:
            if not respawn and self._workers.done():
                self.save_status()
                self._finished.set()
                # Only the last worker finishing will handle errors.
                if self._errors:
                    LOG.error("transfer failed: %s, %s:\n - %s", self.method, self.url, "\n - ".join(str(e) for e in self._errors))
                if self._todo:
                    LOG.info("transfer interrupted: %s, %s: todo: %s", self.method, self.url, self._todo)
                else:
                    if os.path.isfile(self.path + ".part"):
                        os.unlink(self.path + ".part")
                    LOG.info("transfer completed: %s, %s", self.method, self.url)

    def close(self):
        """It cancels all transfer tasks and closes all open streams."""
        self._finished.set()
        self._workers.max_count = 1
        with self._lock:
            fds, self._fds = self._fds, []
        for fd in fds:
            try:
                fd.close()
            except Exception as ex:
                LOG.warning("error closing file descriptor %r: %s", fd.path, ex)

    @contextlib.contextmanager
    def _open(self) -> Iterator[FileDescriptor | None]:
        todo: RangeSet

        with self._lock:
            todo, self._todo = self._todo.split(self._multipart_size)
        if not todo:
            yield None
            return

        try:
            fd: FileDescriptor
            if self.direction == TransferDirection.DOWN:
                fd = FileWriter(self._path, todo)
            elif self.direction == TransferDirection.UP:
                fd = FileReader(self._path, todo)
            else:
                raise ValueError(f"invalid transfer direction: {self.direction}")
            with fd:
                with self._lock:
                    self._fds.append(fd)
                try:
                    yield fd
                    return
                finally:
                    _, todo = todo.split(fd.position)
        finally:
            self._todo |= todo

    @property
    def content_length(self) -> int:
        ret = self._head.content_length
        if ret is None:
            ret = MAX_LENGTH
        return ret

    @property
    def transferred(self) -> RangeSet:
        with self._lock:
            todo = self._todo
            for fd in self._fds:
                if fd.position < fd.ranges.end:
                    todo |= fd.ranges
            return Range(0, self.content_length) - todo

    @property
    def progress(self) -> float:
        if self.content_length != MAX_LENGTH:
            return 100.0 * self.transferred.size / self.content_length
        return 0.0

    @property
    def duration(self) -> float:
        """Obtain the transfer duration.
        :return: the transfer duration in seconds.
        """
        started = self._started.time
        if started is None:
            # hasn't started yet.
            return 0.0
        finished = self._finished.time
        if finished is None:
            # hasn't finished yet.
            finished = time.monotonic()
        return finished - started

    @property
    def average_speed(self) -> float:
        """Obtain the average transfer speed .
        :return: speed in bytes per seconds.
        """
        try:
            return (self.transferred.size - self._resumed_size) / self.duration
        except ZeroDivisionError:
            return 0.0

    def info(self) -> str:
        direction = {TransferDirection.DOWN: "<", TransferDirection.UP: ">"}.get(self.direction, "=")
        return (
            f"{direction} {self.url} {self.progress:.0f}% "
            f"{pretty_size(self.transferred.size)}/{pretty_size(self.content_length)} {pretty_time(self.duration)} "
            f"{pretty_size(self.average_speed)}/s {self.status} {self._workers.count} workers"
        )

    def wait(self, timeout: float | None = None) -> bool:
        if not self._finished.wait(timeout):
            return False
        for ex in self._errors:
            raise ex
        return True

    @property
    def status(self) -> TransferStatus:
        if self._finished:
            if self._errors:
                return TransferStatus.FAILED
            if self._todo:
                return TransferStatus.CANCELLED
            return TransferStatus.DONE
        if self._started:
            return TransferStatus.DOING
        if self._workers.count > 0:
            return TransferStatus.QUEUED
        return TransferStatus.INITIALIZED

    @property
    def errors(self) -> list[Exception]:
        return list(self._errors)

    @property
    def method(self) -> str:
        return pretty_func(self._get)

    @property
    def direction(self) -> TransferDirection:
        return TransferDirection.DOWN

    @property
    def done(self) -> bool:
        return bool(self._finished)


def pretty_size(value: int | float | None) -> str:
    if value is None:
        return "?"
    value = float(value)
    if value < 1024.0:
        return f"{value:.0f}B"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.0f}KB"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.2f}MB"
    value /= 1024.0
    if value < 1024.0:
        return f"{value:.2f}GB"
    value /= 1024.0
    return f"{value:.2f}TB"


def pretty_time(value: int | float) -> str:
    value = float(value)
    if value < 60.0:
        return f"{value:.0f}s"
    value /= 60.0
    if value < 60.0:
        return f"{value:.2f}m"
    value /= 60.0
    return f"{value:.2f}h"


def pretty_func(fn: Any) -> str:
    return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or str(fn)


class StreamClosedError(Exception):
    pass


class FileDescriptor:

    def __init__(self, path: str, fd: BinaryIO, ranges: RangeSet = NO_RANGE):
        if len(ranges) > 1:
            raise NotImplementedError(f"multiple ranges is not supported: {ranges}")
        self.position = fd.tell()
        self.ranges = ranges
        self.path = path
        self._lock = threading.Lock()
        self._fd: BinaryIO | None = fd

    @property
    def transferred(self) -> int:
        return self.position - self.ranges.start

    def close(self):
        with self._lock:
            if self._fd is not None:
                self._fd.close()
            self._fd = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class FileWriter(FileDescriptor):
    """OutputStream provides an output stream wrapper able to prevent exceeding writing given range."""

    def __init__(self, path: str, ranges: RangeSet = NO_RANGE):
        if os.path.isfile(path):
            fd = open(path, "r+b")  # pylint: disable=consider-using-with
        else:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd = open(path, "w+b")  # pylint: disable=consider-using-with
        if ranges and ranges.start > 0:
            fd.seek(ranges.start)
        super().__init__(path, fd, ranges)

    def write(self, data):
        with self._lock:
            if self._fd is None:
                raise StreamClosedError("stream has been closed")
            size = len(data)
            if self.ranges:
                # prevent exceeding file data range
                size = min(size, self.ranges.end - self.position)
            chunk = data[:size]
            if chunk:
                self._fd.write(data)
                self.position += size
            if len(data) > size:
                raise RangeError(f"data size exceeds file range: {len(data)} > {size}", self.ranges)


class FileReader(FileDescriptor):
    """InputStream provides an input stream wrapper able to prevent exceeding reading given range."""

    def __init__(self, path: str, ranges: RangeSet = NO_RANGE):
        fd = open(path, "rb")  # pylint: disable=consider-using-with
        if ranges:
            fd.seek(ranges.start)
        else:
            ranges = Range(0, os.path.getsize(path))
        super().__init__(path, fd, ranges)

    def read(self, size: int = -1) -> bytes:
        with self._lock:
            if self._fd is None:
                raise StreamClosedError("stream has been closed")

            if self.ranges:
                # prevent exceeding file data range
                if size < 0:
                    size = self.ranges.end - self.position
                else:
                    size = min(size, self.ranges.end - self.position)
            if size == 0:
                return b""
            data = self._fd.read(size)
            assert isinstance(data, bytes)
            size = len(data)
            self.position += size
            return data
