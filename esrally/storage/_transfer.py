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

import enum
import json
import logging
import os
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import BinaryIO, Protocol

from esrally.storage._adapter import ServiceUnavailableError
from esrally.storage._client import MAX_CONNECTIONS, Client
from esrally.storage._range import (
    MAX_LENGTH,
    NO_RANGE,
    Range,
    RangeError,
    RangeSet,
    rangeset,
)
from esrally.utils import pretty, threads

LOG = logging.getLogger(__name__)


class TransferStatus(enum.Enum):
    INITIALIZED = 0
    QUEUED = 1
    DOING = 2
    DONE = 3
    CANCELLED = 4
    FAILED = 5


MULTIPART_SIZE = 8 * 1024 * 1024


class Executor(Protocol):
    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        raise NotImplementedError()


class Transfer:

    def __init__(
        self,
        client: Client,
        url: str,
        path: str,
        executor: Executor,
        document_length: int | None,
        todo: RangeSet = NO_RANGE,
        done: RangeSet = NO_RANGE,
        multipart_size: int = MULTIPART_SIZE,
        max_connections: int = MAX_CONNECTIONS,
        resume: bool = True,
    ):
        if max_connections <= 0:
            raise ValueError("max_connections must be greater than 0")
        if max_connections == 1:
            multipart_size = MAX_LENGTH
        if not todo:
            todo = Range(0, MAX_LENGTH)
        if document_length is not None:
            todo &= Range(0, document_length)

        self.client = client
        self.url = url
        self.path = path
        self._document_length = document_length
        self._max_connections = max_connections
        self._started = threads.TimedEvent()
        self._workers = threads.WaitGroup(max_count=max_connections)
        self._finished = threads.TimedEvent()
        self._multipart_size = multipart_size
        self._todo = todo - done
        self._done = done
        self._fds = list[FileDescriptor]()
        self._executor = executor
        self._errors = list[Exception]()
        self._lock = threading.Lock()
        self._resumed_size = 0
        if resume and os.path.isfile(self.path + ".status"):
            try:
                self._resume_status()
            except Exception as ex:
                LOG.error("Failed to resume transfer: %s", ex)
            else:
                LOG.info("resumed from existing status: %s", self.info())

    @property
    def document_length(self) -> int | None:
        return self._document_length

    @document_length.setter
    def document_length(self, value: int) -> None:
        if self._document_length == value:
            return
        if self._document_length is not None:
            raise RuntimeError("mismatching document length")
        with self._lock:
            self._todo = self._todo & Range(0, value)
            self._document_length = value

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
            if self._workers.count >= self._max_connections:
                return False
            try:
                self._workers.add(1)
            except threads.WaitGroupLimitError:
                return False
            self._executor.submit(self._run)
            return True

    def _resume_status(self):
        if not os.path.isfile(self.path):
            raise FileNotFoundError(f"target file not found: {self.path}")

        status_filename = self.path + ".status"
        if not os.path.isfile(status_filename):
            raise FileNotFoundError(f"status file not found: {status_filename}")

        with open(self.path + ".status") as fd:
            document = json.load(fd)
            if not isinstance(document, Mapping):
                raise ValueError(f"mismatching status file format: got {type(document)}, want dict")

        url = document.get("url")
        if url is None:
            raise ValueError(f"url field not found in status file: {status_filename}")
        if url != self.url:
            raise ValueError(f"mismatching url in status file: '{status_filename}', got '{url}', want '{self.url}'")

        document_length = document.get("document_length")
        if document_length is None:
            raise ValueError(f"document_length field not found in status file: {status_filename}")

        if self._document_length is not None and self._document_length != document_length:
            raise ValueError(f"mismatching document length: got: {document_length}, want {self._document_length}")

        done_text = document.get("done")
        if done_text is None:
            raise ValueError(f"done field not found in status file: {status_filename}")

        done = rangeset(done_text)
        if done:
            self._done = self.done | done
            self._todo = self._todo - done
        self._resumed_size = done.size
        if not self._todo:
            self._finished.set()

    def save_status(self):
        document = {
            "url": self.url,
            "document_length": self.document_length,
            "done": str(self.done),
        }
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path + ".status", "w") as fd:
            json.dump(document, fd)

    def _run(self):
        if self._finished:
            return
        cancelled = False
        try:
            if self._started.set():
                LOG.info("transfer started: %s", self.url)
            with self._open() as fd:
                if fd is None:
                    LOG.debug("transfer done: %s", self.url)
                    return
                if self._todo:
                    # It eventually spawns another thread for multipart transfer
                    self.start()
                assert isinstance(fd, FileWriter)
                head = self.client.get(self.url, fd, fd.ranges, document_length=self.document_length)
                if head.document_length is not None:
                    self.document_length = head.document_length
        except StreamClosedError as ex:
            LOG.info("task cancelled: %s: %s", self.url, ex)
            cancelled = True
        except ServiceUnavailableError as ex:
            with self._lock:
                count = self._workers.count
                max_count = max(1, count - 1)
                self._workers.max_count = max_count
            cancelled = count > 1
            LOG.info("service unavailable: %s, workers=%d/%d: %s", self.url, count, max_count, ex)
        except Exception as ex:
            LOG.exception("task failed: %s", self.url)
            with self._lock:
                self._errors.append(ex)
            self.close()
        finally:
            self._workers.done()
            self.save_status()
            if self._errors:
                if self._finished.set():
                    LOG.error("transfer failed: %s:\n - %s", self.url, "\n - ".join(str(e) for e in self._errors))
            elif cancelled:
                LOG.debug("task cancelled: %s", self.url)
            elif not self.todo:
                if self._finished.set():
                    LOG.info("transfer completed: %s", self.url)
            else:
                self.start()

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

    @contextmanager
    def _open(self) -> Iterator[FileDescriptor | None]:
        todo: RangeSet

        with self._lock:
            todo, self._todo = self._todo.split(self._multipart_size)
        if not todo:
            yield None
            return

        LOG.debug("transfer started: %s, %s", self.url, todo)
        done: RangeSet = NO_RANGE
        try:
            fd = FileWriter(self.path, todo)
            with fd:
                with self._lock:
                    self._fds.append(fd)
                try:
                    yield fd
                finally:
                    with self._lock:
                        try:
                            self._fds.remove(fd)
                        except ValueError:
                            pass
                        done, todo = todo.split(fd.position)
                        self._todo |= todo
                        self._done |= done
        finally:
            if todo:
                LOG.debug("transfer interrupted: %s (done: %s, todo: %d).", self.url, done, todo)
            else:
                LOG.debug("transfer done: %s (done: %s).", self.url, done)

    @property
    def done(self) -> RangeSet:
        with self._lock:
            done = self._done
            for fd in self._fds:
                if fd.position > 0:
                    done |= Range(0, fd.position)
        return done

    @property
    def todo(self) -> RangeSet:
        with self._lock:
            todo = self._todo
            for fd in self._fds:
                if fd.position < fd.ranges.end:
                    todo |= Range(fd.position, fd.ranges.end)
        return todo

    @property
    def progress(self) -> float:
        document_length = self.document_length
        if document_length is None:
            return 0.0
        done = self.done
        if not done:
            return 0.0
        return 100.0 * done.size / document_length

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
        done = self.done
        if not done:
            return 0.0
        duration = self.duration
        if duration == 0.0:
            return 0.0
        return (done.size - self._resumed_size) / self.duration

    def info(self) -> str:
        return (
            f"- {self.url} {self.progress:.0f}% "
            f"{pretty.size(self.done.size)}/{pretty.size(self.document_length)} {pretty.seconds(self.duration)} "
            f"{pretty.size(self.average_speed)}/s {self.status} {self._workers.count} workers"
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
    def finished(self) -> bool:
        return bool(self._finished)


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
            fd = open(path, "r+b")
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
