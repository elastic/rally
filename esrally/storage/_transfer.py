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

import collections
import copy
import dataclasses
import json
import logging
import os
import threading
import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any

from esrally import types
from esrally.storage._adapter import Head, ServiceUnavailableError
from esrally.storage._client import Client
from esrally.storage._config import StorageConfig
from esrally.storage._executor import Executor
from esrally.storage._io import FileWriter, StreamClosedError
from esrally.storage._range import MAX_LENGTH, NO_RANGE, Range, RangeSet, rangeset
from esrally.utils import convert
from esrally.utils import crc32c as _crc32c
from esrally.utils import pretty, threads

LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class TransferStats:
    # The number of requests.
    request_count: int = 0

    # The amount of bytes transferred.
    transferred_bytes: int = 0

    # The amount of time spent waiting for a response.
    response_time: float = 0.0

    # The amount of time spent receiving data.
    read_time: float = 0.0

    # The amount of time spent writing data.
    write_time: float = 0.0

    @property
    def total_time(self) -> float:
        """total_time represents the total mount of time spend performing this transfer."""
        return self.response_time + self.read_time + self.write_time

    @property
    def average_latency(self) -> float:
        """average_latency is computed as the average response time of each GET request."""
        if self.request_count == 0:
            return 0.0
        return self.response_time / self.request_count

    @property
    def requests_per_second(self) -> float:
        """requests_per_second is computed as the average number of GET requests per second."""
        duration = self.total_time
        if not duration:
            return 0.0
        return self.request_count / duration

    @property
    def throughput(self) -> float:
        """throughput is computed as the average throughput of each GET request."""
        if not self.total_time:
            return 0.0
        return self.transferred_bytes / self.total_time

    @property
    def read_throughput(self) -> float:
        """throughput is computed as the average throughput of reading data of each GET request."""
        if not self.read_time:
            return 0.0
        return self.transferred_bytes / self.read_time

    @property
    def write_throughput(self) -> float:
        """throughput is computed as the average throughput of writing data of each GET request."""
        if not self.write_time:
            return 0.0
        return self.transferred_bytes / self.write_time

    def add(
        self,
        *,
        request_count: int = 0,
        transferred_bytes: int = 0,
        response_time: float = 0.0,
        read_time: float = 0.0,
        write_time: float = 0.0,
    ) -> None:
        self.request_count += request_count
        self.transferred_bytes += transferred_bytes
        self.response_time += response_time
        self.read_time += read_time
        self.write_time += write_time

    def pretty(self) -> dict[str, Any]:
        details: dict[str, Any] = {
            "bytes": pretty.size(self.transferred_bytes),
            "requests": self.request_count,
            "latency": pretty.duration(self.average_latency),
            "duration": {
                "total": pretty.duration(self.total_time),
                "response": pretty.duration(self.response_time),
                "read": pretty.duration(self.read_time),
                "write": pretty.duration(self.write_time),
            },
            "throughput": {
                "total": pretty.throughput(self.throughput),
                "read": pretty.throughput(self.read_throughput),
                "write": pretty.throughput(self.write_throughput),
            },
        }
        return {k: v for k, v in details.items() if v}


class Transfer:
    """Transfers class implements multipart file transfers by submitting tasks to an Executor.

    By using a thread pool executor like concurrent.futures.ThreadPoolExecutor to delegate its tasks, it implements
    parallel file transfers.

    It splits the file in parts of up to multipart_size bytes, and allocate a task for downloading each part.
    Each task is being executed by a thread provided by the Executor. Each thread after executing the task will
    eventually execute the next task. Every task opens local file for writing. Then it seeks to the beginning of the
    file part before writing transferred data from there. On the network side it asks the server to send only the
    part of the remote file that has to be written to the local file.

    By reducing the size of the multipart_size it reduces the scope of each task, increasing the number of tasks and
    parts of file to transfer. This produces some additional work because it increases the number of IO operations and
    of requests to the remote service. The advantage of that is making the task allocation between multiple threads more
    dynamic and allows the client to have more granularity for load balance transfers between multiple connections and
    servers.
    """

    def __init__(
        self,
        client: Client,
        url: str,
        executor: Executor,
        document_length: int | None,
        path: str | None = None,
        todo: RangeSet | None = None,
        done: RangeSet = NO_RANGE,
        multipart_size: int | None = None,
        max_connections: int = StorageConfig.DEFAULT_MAX_CONNECTIONS,
        resume: bool = True,
        crc32c: str | None = None,
        cfg: types.Config | None = None,
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
                multipart_size = StorageConfig.DEFAULT_MULTIPART_SIZE

        # This also ensures the path is a string
        if path is not None:
            path = os.path.normpath(os.path.expanduser(path))

        if todo is None:
            todo = Range()

        if document_length is not None:
            todo &= Range(0, document_length)

        self.client = client
        self.url = url
        self.cfg = StorageConfig.from_config(cfg)
        self._path = path
        self._document_length = document_length
        self._max_connections = max_connections
        self._started = threads.TimedEvent()
        self._workers = threads.WaitGroup(max_count=max_connections)
        self._finished = threads.TimedEvent()
        self._multipart_size = multipart_size
        self._todo = todo - done
        self._done = done
        self._fds: list[FileWriter] = []
        self._executor = executor
        self._errors: list[Exception] = []
        self._lock = threading.Lock()
        self._resumed_size = 0
        self._crc32c = crc32c
        self._mirror_failures: dict[str, str] = {}
        self._stats: dict[str, TransferStats] = collections.defaultdict(TransferStats)
        if resume and self.status_file_path and os.path.isfile(self.status_file_path):
            try:
                self._resume_status()
            except Exception as ex:
                LOG.error("Failed to resume transfer: %s", ex)
            else:
                LOG.debug("Transfer resumed from existing status:\n%s", self.info())

    @property
    def stats(self) -> dict[str, TransferStats]:
        return copy.deepcopy(self._stats)

    @property
    def status_file_path(self) -> str:
        return self.cfg.transfer_status_path(self.url)

    @property
    def path(self) -> str:
        if self._path is None:
            self._path = self.cfg.transfer_file_path(self.url)
        return self._path

    @path.setter
    def path(self, value: str) -> None:
        path = os.path.normpath(os.path.expanduser(value))
        if self._path == path:
            return
        if self._path is not None:
            raise ValueError(f"mismatching file path: got '{value}', expected '{self._path}'")
        self._path = path

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
            raise ValueError(f"mismatching document length: got '{value}', expected '{self._document_length}'")
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

    def start(self, todo: RangeSet | None = None) -> bool:
        with self._lock:
            if todo is not None:
                todo |= self._todo
                todo -= self._done
                if self._document_length is not None:
                    todo &= Range(0, self._document_length)
                self._todo = todo
                if self._todo:
                    self._errors = []
                    self._finished.clear()
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

    @property
    def mirror_failures(self) -> dict[str, str]:
        return dict(self._mirror_failures)

    def _resume_status(self):
        status_filename = self.status_file_path
        if not os.path.isfile(status_filename):
            # There is no file to read status data from.
            raise FileNotFoundError(f"status file not found: {status_filename}")

        with open(status_filename) as fd:
            document = json.load(fd)
            if not isinstance(document, Mapping):
                # Invalid file format.
                raise ValueError(f"mismatching status file format: got {type(document)}, expected dict")

        # It checks the remote URL to ensure the file was downloaded from the same location.
        url = document.get("url")
        if url is None:
            raise ValueError(f"url field not found in status file: '{status_filename}'")
        if url != self.url:
            raise ValueError(f"mismatching url in status file: '{status_filename}', got '{url}', expected '{self.url}'")

        path = document.get("path")
        if path is not None:
            self.path = path

        if not os.path.isfile(self.path):
            # There is no file to recover interrupted transfer to.
            raise FileNotFoundError(f"target file not found: {self.path}")

        # It checks the document length to ensure the file was the same version.
        document_length = document.get("document_length")
        if document_length is None:
            raise ValueError(f"document_length field not found in status file: '{status_filename}'")
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
            file_size = os.path.getsize(self.path)
            if done.end > file_size:
                raise ValueError(f"corrupted file size is smaller than completed part: {done.end} > {file_size} (path='{self.path}')")

            self._done = self.done | done
            self._todo = self._todo - done

        # It updates the resumed size so that it will compute download speed only on the new parts.
        self._resumed_size = done.size
        if not self._todo:
            # There is nothing more to do.
            self._finished.set()

        # It restores mirror failures.
        self._mirror_failures = document.get("mirror_failures", {})

        # It restores transfer statistics.
        for stats in document.get("stats", []):
            if not isinstance(stats, dict):
                raise ValueError(f"mismatching stats file format: got {type(stats)}, expected dict")
            url = stats.pop("url", None)
            if url is None:
                raise ValueError(f"url field not found in stats '{stats}' in status file: '{status_filename}'")
            self._stats[url].add(**stats)

    def save_status(self):
        """It updates the status file."""
        self._mirror_failures = {f.mirror_url: f.error for f in self.client.mirror_failures(self.url)}
        with self._lock:
            stats = [{"url": u, **dataclasses.asdict(s)} for u, s in self._stats.items()]

        document = {
            "url": self.url,
            "path": self.path,
            "document_length": self.document_length,
            "done": str(self.done),
            "crc32c": self.crc32c,
            # Mirror failures is intended to be consumed by a tool in charge to upload downloaded files that was missing
            # in any mirror server to keep it in sync with source file repository.
            "mirror_failures": self._mirror_failures,
            "stats": stats,
        }
        status_filename = self.status_file_path
        os.makedirs(os.path.dirname(status_filename), exist_ok=True)
        with open(status_filename, "w") as fd:
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
                if fd.ranges:
                    check_head = Head(
                        ranges=fd.ranges, content_length=fd.ranges.size, document_length=self._document_length, crc32c=self._crc32c
                    )
                else:
                    check_head = Head(content_length=self._document_length, crc32c=self._crc32c)

                start_time = time.monotonic()
                with self.client.get(self.url, check_head=check_head) as got:
                    with self._lock:
                        # Response time includes only:
                        # - resolving the mirrored file URL(s);
                        # - receiving the file headers.
                        self._stats[got.head.url].add(request_count=1, response_time=time.monotonic() - start_time)

                    # When ranges are not given, then content_length replaces document_length.
                    document_length = got.head.document_length or got.head.content_length
                    if document_length:
                        # It checks the size of the file it downloaded the data from.
                        self.document_length = document_length
                    if got.head.crc32c is not None:
                        # It checks the crc32c check sum of the file it downloaded the data from.
                        self.crc32c = got.head.crc32c

                    LOG.debug("Downloading file chunks from '%s' to '%s' (range=%s)...", got.head.url, self.path, got.head.ranges)
                    while True:
                        start_time = time.monotonic()
                        try:
                            chunk = next(got.chunks)
                        except StopIteration:
                            LOG.debug(
                                "Stopped downloading file chunks from '%s' to '%s' (range=%s)", got.head.url, self.path, got.head.ranges
                            )
                            break
                        read_time = time.monotonic()
                        if chunk:
                            fd.write(chunk)
                        write_time = time.monotonic()
                        with self._lock:
                            # Read time includes the time spent receiving all chunks of data.
                            # Write time includes the time spent writing all chunks of data.
                            self._stats[got.head.url].add(
                                transferred_bytes=len(chunk), read_time=read_time - start_time, write_time=write_time - read_time
                            )

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
            # In case of the count of concurrent tasks is greater than 1 then it forbids to reschedule it.
            cancelled = count > 1
            LOG.info("service unavailable: %s, workers=%d/%d: %s", self.url, count, max_count, ex)
        except TimeoutError as ex:
            # The client raised this error because it timed out waiting for answer from server.
            LOG.info("transfer timed out (url=%s): %s", self.url, ex)
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
            elif not self.todo and not self._workers.count:
                # There is nothing more to do: the transfer has been completed with success.
                LOG.info("Checking transferred document: %s", self.url)
                try:
                    self._check_finished_document()
                except Exception as ex:
                    self._errors.append(ex)
                    raise

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
                LOG.error("error closing file descriptor %r: %s", fd.path, ex)

    @contextmanager
    def _open(self) -> Generator[FileWriter | None]:
        todo: RangeSet
        with self._lock:
            # It gets a part of the file to transfer from the remaining range.
            todo, self._todo = self._todo.split(self._multipart_size)
        if not todo:
            # There is nothing more to do.
            yield None
            return

        # It opens a file descriptor bound to the part of the file assigned to this task.
        with FileWriter.open(self.path, todo) as fd:
            with self._lock:
                # It registers the file descriptor so that it can be closed from another thread.
                self._fds.append(fd)
            LOG.debug("transfer started: %s, %s", self.url, todo)
            try:
                yield fd
            finally:
                # It waits for data to be actually written before updating the status.
                fd.flush()
                # After receiving data _document_length could have been changed reducing the range to download.
                todo &= Range(0, self._document_length or MAX_LENGTH)
                done, todo = todo.split(fd.transferred)
                if todo:
                    LOG.info("transfer interrupted: %s (done: %s, todo: %s).", self.url, done, todo)
                else:
                    LOG.debug("transfer done: %s (done: %s).", self.url, done)

                with self._lock:
                    try:
                        # It de-registers the file descriptor so that it can't be closed from another thread.
                        self._fds.remove(fd)
                    except ValueError:
                        pass
                    # It updates the status of the works with the completed parts.
                    self._todo |= todo
                    self._done |= done

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

    def pretty(self, *, stats: bool = False, mirror_failures: bool = False) -> dict[str, Any]:
        details: dict[str, Any] = {
            "url": self.url,
            "path": self.path,
            "progress": f"{self.progress:.0f}%",
            "done": pretty.size(self.done.size),
            "size": pretty.size(self.document_length),
            "workers": self._workers.count,
            "duration": self.duration and pretty.duration(self.duration),
            "throughput": self.average_speed and pretty.throughput(self.average_speed),
            "stats": stats and {u: s.pretty() for u, s in self._stats.items()},
            "mirror_failures": mirror_failures and self._mirror_failures,
        }
        return {k: v for k, v in details.items() if v}

    def info(self, *, stats: bool = False, mirror_failures: bool = False) -> str:
        return json.dumps(self.pretty(stats=stats, mirror_failures=mirror_failures), indent=2)

    def wait(self, timeout: float | None = None) -> bool:
        """It waits for transfer termination."""
        if not self._finished.wait(timeout):
            return False
        for ex in self._errors:
            raise ex
        return True

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

    def _check_finished_document(self) -> None:
        with self._lock:
            if self._todo:
                raise ValueError("Transfer has not been finished.")
            if self._fds:
                raise ValueError("Some file writers is still there.")
            if not os.path.isfile(self.path):
                raise FileNotFoundError(self.path)
            want_size = self._document_length
            if want_size is not None:
                size = os.path.getsize(self.path)
                if size != want_size:
                    raise ValueError(f"Unexpected file size: {size}, want {want_size}")
                want_done = Range(0, want_size)
                if self._done != want_done:
                    raise ValueError(f"Unexpected done range: {self._done}, want {want_done}.")
            want_checksum = _crc32c.Checksum.from_base64(self._crc32c) if self._crc32c else None
            if want_checksum is not None:
                checksum = _crc32c.Checksum.from_filename(self.path)
                if checksum != want_checksum:
                    raise ValueError(f"Unexpected checksum: {checksum}, want {want_checksum}")
