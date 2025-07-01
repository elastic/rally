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
from esrally.utils import convert, pretty, threads

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
    """Executor protocol is used by Transfer class to submit tasks execution.

    Notable implementation of this protocol is concurrent.futures.ThreadPoolExecutor[1] class.

    [1] https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor
    """

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        raise NotImplementedError()


class Transfer:
    """Transfers class implements multipart file transfers by submitting tasks to an Executor.

    By using a thread pool executor like concurrent.futures.ThreadPoolExecutor to delegate its tasks, it implements
    parallel file transfers.

    It splits the file in parts of up to multipart_size bytes, and allocate a task for downloading each part.
    Each task is being executed by a thread provided by the Executor. Each thread after executing the task will
    eventually execute the next task. Every task open local file for writing. Then it seeks to the beginning of the
    file part before writing transferred data from there. On the network side it ask the server to send only the
    part of the remote file that has to be written to the local file.

    By reducing the size of the multipart_size it reduces the scope of each task, increasing the number of tasks and
    parts of file to transfer. This produces some additional work because in increases the number of IO operations and
    of request to the remote service. The advantage of that is making the task allocation between multiple threads more
    dynamic and allows the client to have more granularity for load balance transfers between multiple connections and
    servers.
    """

    def __init__(
        self,
        client: Client,
        url: str,
        path: str,
        executor: Executor,
        document_length: int | None,
        todo: RangeSet = NO_RANGE,
        done: RangeSet = NO_RANGE,
        multipart_size: int | None = None,
        max_connections: int = MAX_CONNECTIONS,
        resume: bool = True,
        crc32c: str | None = None,
    ):
        """
        :param client: The client to use to download file parts from a remote service.
        :param url: The URL of the remote file.
        :param path: The path of the local file to write to.
        :param executor: The executor to use to submit tasks to be executed.
        :param document_length: The number of bytes to read from the remote file and write to the local file.
        :param todo: The set of file ranges to be transferred. In the case of no sets, it will download all the file.
        :param done: The set of file ranges already transferred and to be skip.
        :param multipart_size: The size of the parts the file has to be split for multipart transfer.
        :param max_connections: The maximum number of connections to the server to allow.
        :param resume: Enable/disable resuming transfer from an interrupted status.
        """
        if max_connections <= 0:
            raise ValueError("max_connections must be greater than 0")
        if multipart_size is None:
            if max_connections == 1:
                # It disables multipart transfers.
                multipart_size = MAX_LENGTH
            else:
                # By default, it will enable multipart with a hardcoded size.
                multipart_size = MULTIPART_SIZE
        if not todo:
            # By default, it will transfer the whole file.
            todo = Range(0, MAX_LENGTH)
        if document_length is not None:
            # It limits the parts of the file to transfer to the document length.
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
        self._fds: list[FileDescriptor] = []
        self._executor = executor
        self._errors: list[Exception] = []
        self._lock = threading.Lock()
        self._resumed_size = 0
        self._crc32c = crc32c
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
        """It sets the document length.

        It allows to set the documents length only when it is None.
        """
        if self._document_length == value:
            return
        if self._document_length is not None:
            raise RuntimeError(f"mismatching document length: got {value}, want {self._document_length}")
        with self._lock:
            self._document_length = value
            # It ensures to finish requesting file parts as soon it reaches the expected document length.
            self._todo = self._todo & Range(0, value)
            self._done = self._done & Range(0, value)

    @property
    def max_connections(self) -> int:
        return self._max_connections

    @max_connections.setter
    def max_connections(self, value: int) -> None:
        # It ensures to have at least one connection to use for transferring the file parts.
        value = max(1, value)
        delta = value - self._max_connections
        if delta == 0:
            return
        with self._lock:
            self._max_connections = value
            if delta > 0:
                # After increasing the number of connections it eventually schedules a new task to start a new
                # connection in another thread.
                self.start()

    def start(self) -> bool:
        with self._lock:
            if not self._todo or self._finished:
                # There are no more tasks to do.
                return False
            if self._workers.count >= self._max_connections:
                # There are already enough connections.
                return False
            try:
                # It increases the number of submitted tasks.
                self._workers.add(1)
            except threads.WaitGroupLimitError:
                # There are already enough submitted tasks.
                return False
            # It submits a new task.
            self._executor.submit(self._run)
            return True

    def _resume_status(self):
        if not os.path.isfile(self.path):
            # There is no file to recover interrupted transfer to.
            raise FileNotFoundError(f"target file not found: {self.path}")

        status_filename = self.path + ".status"
        if not os.path.isfile(status_filename):
            # There is no file to read status data from.
            raise FileNotFoundError(f"status file not found: {status_filename}")

        with open(self.path + ".status") as fd:
            document = json.load(fd)
            if not isinstance(document, Mapping):
                # Invalid file format.
                raise ValueError(f"mismatching status file format: got {type(document)}, want dict")

        # It checks the remote URL to ensure the file was downloaded from the same location.
        url = document.get("url")
        if url is None:
            raise ValueError(f"url field not found in status file: {status_filename}")
        if url != self.url:
            raise ValueError(f"mismatching url in status file: '{status_filename}', got '{url}', want '{self.url}'")

        # It checks the document length to ensure the file was the same version.
        document_length = document.get("document_length")
        if document_length is None:
            raise ValueError(f"document_length field not found in status file: {status_filename}")
        self.document_length = document_length

        # It checks the crc32c checksum to ensure the file was the same version.
        crc32c = document.get("crc32c", None)
        if crc32c is not None:
            self.crc32c = crc32c

        # It skips the parts that has been already downloaded.
        done_text = document.get("done")
        if done_text is None:
            raise ValueError(f"done field not found in status file: {status_filename}")
        done = rangeset(done_text)
        if done:
            self._done = self.done | done
            self._todo = self._todo - done
        # Update the resumed size so that it will compute download speed only on the new parts.
        self._resumed_size = done.size
        if not self._todo:
            # There is nothing more to do.
            self._finished.set()

    def save_status(self):
        """It updates the status file."""
        document = {
            "url": self.url,
            "document_length": self.document_length,
            "done": str(self.done),
            "crc32c": self.crc32c,
        }
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path + ".status", "w") as fd:
            json.dump(document, fd)

    def _run(self):
        """It downloads part of the file."""
        if self._finished:
            # Anything else to do.
            return

        cancelled = False  # It indicates the task has been interrupted.
        try:
            if self._started.set():
                # The first executed task will enter here.
                LOG.info("transfer started: %s", self.url)
            # It opens a portion of the file to write to.
            with self._open() as fd:
                if fd is None:
                    # This is an escape trick used to exit when there is nothing more to do.
                    LOG.debug("transfer done: %s", self.url)
                    return
                if self._todo:
                    # It eventually spawns another thread for multipart transfer.
                    self.start()
                assert isinstance(fd, FileWriter)
                # It downloads the part of the file from a remote location.
                head = self.client.get(self.url, fd, fd.ranges, document_length=self._document_length, crc32c=self._crc32c)
                if head.document_length is not None:
                    # It checks the size of the file it downloaded the data from.
                    self.document_length = head.document_length
                if head.crc32c is not None:
                    # It checks the crc32c check sum of the file it downloaded the data from.
                    self.crc32c = head.crc32c
        except StreamClosedError as ex:
            LOG.info("transfer cancelled: %s: %s", self.url, ex)
            cancelled = True
        except ServiceUnavailableError as ex:
            # The client raised this error because the maximum number of client connections for each remote server
            # has been already established.
            with self._lock:
                count = self._workers.count
                max_count = max(1, count - 1)
                self._workers.max_count = max_count
            # In case of the count of concurrent tasks is greater than 1 then if forbid to reschedule it.
            cancelled = count > 1
            LOG.info("service unavailable: %s, workers=%d/%d: %s", self.url, count, max_count, ex)
        except Exception as ex:
            # This error will brutally interrupt the transfer.
            LOG.exception("task failed: %s", self.url)
            with self._lock:
                self._errors.append(ex)
            # It will interrupt all other tasks execution by closing the streams causing an error in the fd.write()
            # method.
            self.close()
        finally:
            # It decreases the number of scheduled tasks, allowing a new tasks to be submit.
            self._workers.done()
            self.save_status()
            if self._errors:
                if self._finished.set():
                    # The first task entered here will write the error(s) to the log fine.
                    LOG.error("transfer failed: %s:\n - %s", self.url, "\n - ".join(str(e) for e in self._errors))
            elif cancelled:
                # It will submit any task for execution.
                LOG.debug("task cancelled: %s", self.url)
            elif not self.todo:
                # There is nothing more to do: the transfer has been completed with success.
                if self._finished.set():
                    # Only the first task that enters here will write this message.
                    LOG.info("transfer completed: %s", self.url)
            else:
                # It eventually re-schedule another task for execution.
                self.start()

    def close(self):
        """It cancels all transfer tasks and closes all open streams."""
        self._finished.set()
        self._workers.max_count = 1
        with self._lock:
            # It de-reference all the file descriptors.
            fds, self._fds = self._fds, []
        # It closes all the file descriptors.
        for fd in fds:
            try:
                # This will cause the fd to raise an error when calling the write method causing its task to be
                # cancelled.
                fd.close()
            except Exception as ex:
                LOG.warning("error closing file descriptor %r: %s", fd.path, ex)

    @contextmanager
    def _open(self) -> Iterator[FileDescriptor | None]:
        todo: RangeSet
        with self._lock:
            # It gets a part of the file to transfer from the remaining range.
            todo, self._todo = self._todo.split(self._multipart_size)
        if not todo:
            # There is nothing more to do.
            yield None
            return

        LOG.debug("transfer started: %s, %s", self.url, todo)
        done: RangeSet = NO_RANGE
        try:
            with self._lock:
                # It opens a file descriptor bound to the part of the file assigned to this task.
                fd = FileWriter(self.path, todo)
                # It registers the file descriptor so that it can be closed from another thread.
                self._fds.append(fd)
            with fd:
                try:
                    yield fd
                finally:
                    with self._lock:
                        try:
                            # It de-registers the file descriptor so that we can't be closed from another thread.
                            fd.flush()
                            self._fds.remove(fd)
                        except ValueError:
                            pass
                        # After receiving data _document_length could have been changed reducing the range to download.
                        todo &= Range(0, self._document_length or MAX_LENGTH)
                        # It updates the status of the works with the completed part.
                        done, todo = todo.split(fd.position)
                        self._todo |= todo
                        self._done |= done
        finally:
            if todo:
                LOG.info("transfer interrupted: %s (done: %s, todo: %s).", self.url, done, todo)
            else:
                LOG.debug("transfer done: %s (done: %s).", self.url, done)

    @property
    def done(self) -> RangeSet:
        """It retrieves the parts of the file that have already been transferred."""
        with self._lock:
            done = self._done
            for fd in self._fds:
                if fd.position > 0:
                    # It sums up all the file parts that have been writen by currently running tasks.
                    done |= Range(0, fd.position)
        return done

    @property
    def todo(self) -> RangeSet:
        """It retrieves the parts of the file that have yet to be transferred."""
        with self._lock:
            todo = self._todo
            for fd in self._fds:
                if fd.position < fd.ranges.end:
                    # It sums up all the file parts that haven't been writen yet by currently running tasks.
                    todo |= Range(fd.position, fd.ranges.end)
        return todo

    @property
    def progress(self) -> float:
        """It obtains the percentage of the file that have been transferred."""
        document_length = self.document_length
        if document_length is None:
            return 0.0
        done = self.done
        if not done:
            return 0.0
        return 100.0 * done.size / document_length

    @property
    def duration(self) -> convert.Duration:
        """It obtains the transfer duration (up to now).
        :return: the transfer duration in seconds.
        """
        started = self._started.time
        if started is None:
            # It hasn't started yet.
            return convert.Duration(0)
        finished = self._finished.time
        if finished is None:
            # It hasn't finished yet.
            finished = time.monotonic_ns()
        return convert.duration(finished - started, convert.Duration.Unit.NS)

    @property
    def average_speed(self) -> float:
        """It obtains the average transfer speed .
        :return: speed in bytes per seconds.
        """
        done = self.done
        if not done:
            # It hasn't started yet.
            return 0.0
        duration = self.duration
        if duration == 0.0:
            # It hasn't started yet.
            return 0.0
        return (done.size - self._resumed_size) / self.duration.s()

    def info(self) -> str:
        return (
            f"- {self.url} {self.progress:.0f}% "
            f"{pretty.size(self.done.size)}/{pretty.size(self.document_length)} {self.duration} "
            f"{pretty.size(self.average_speed)}/s {self.status} {self._workers.count} workers"
        )

    def wait(self, timeout: float | None = None) -> bool:
        """It waits for transfer termination."""
        if not self._finished.wait(timeout):
            return False
        for ex in self._errors:
            raise ex
        return True

    @property
    def status(self) -> TransferStatus:
        """It returns the status of the transfer."""
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

    @property
    def crc32c(self) -> str | None:
        return self._crc32c

    @crc32c.setter
    def crc32c(self, value: str) -> None:
        if not value:
            raise ValueError("crc32c value can't be empty")
        with self._lock:
            if value == self._crc32c:
                return
            if self._crc32c is not None:
                raise ValueError(f"mismatching crc32c checksum: got: {value!r}, want: {self._crc32c}")
            self._crc32c = value


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

    def flush(self):
        with self._lock:
            if self._fd is not None:
                self._fd.flush()
