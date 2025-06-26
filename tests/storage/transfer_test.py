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
import os
from collections.abc import Iterator
from dataclasses import dataclass

import pytest

from esrally.config import Config
from esrally.storage._adapter import Head, Writable
from esrally.storage._client import Client
from esrally.storage._range import NO_RANGE, Range, RangeSet, rangeset
from esrally.storage._transfer import MAX_CONNECTIONS, MULTIPART_SIZE, Transfer
from esrally.utils.cases import cases

URL = "https://rally-tracks.elastic.co/apm/span.json.bz2"
MISMATCH_URL = "https://rally-tracks.elastic.co/apm/span.json.gz"
DATA = b"\xff" * 1024


class DummyExecutor:

    def __init__(self):
        self.tasks: list[tuple] | None = []

    def submit(self, fn, /, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as fn(*args, **kwargs).
        """
        tasks = self.tasks
        if tasks is None:
            raise RuntimeError("Executor already closed")
        self.tasks.append((fn, args, kwargs))

    def execute_tasks(self):
        tasks = self.tasks
        if tasks is None:
            raise RuntimeError("Executor already closed")
        self.tasks = []
        for fn, args, kwargs in tasks:
            fn(*args, **kwargs)

    def shutdown(self):
        self.tasks = None


class DummyClient(Client):

    def head(self, url: str, ttl: float | None = None) -> Head:
        return Head.create(url, content_length=len(DATA), accept_ranges=True)

    def get(self, url: str, stream: Writable, ranges: RangeSet = NO_RANGE, document_length: int | None = None) -> Head:
        data = DATA
        if ranges:
            data = data[ranges.start : ranges.end]
        if data:
            stream.write(data)
        return Head.create(url, ranges=ranges, content_length=len(data), document_length=len(DATA))


@pytest.fixture
def executor() -> Iterator[DummyExecutor]:
    executor = DummyExecutor()
    try:
        yield executor
    finally:
        executor.shutdown()


@dataclass()
class TransferCase:
    url: str = URL
    todo: str = ""
    document_length: int | None = len(DATA)
    multipart_size: int = MULTIPART_SIZE
    max_connections: int = MAX_CONNECTIONS
    want_done: str = ""
    want_todo: str = ""
    want_written: str = ""
    want_document_length: int = len(DATA)
    want_error: type[Exception] | None = None
    resume: bool = True
    resume_status: dict[str, str] | None = None


@cases(
    # It tests default behavior for small transfers (content_length < multipart_size).
    default=TransferCase(want_done="0-1023", want_written="0-1023"),
    # It tests limiting transfers scope to some ranges.
    todo=TransferCase(todo="10-20, 30-40", want_done="10-20", want_todo="30-40", want_written="10-20"),
    # It tests multipart working when multipart_size < content_length.
    multipart_size=TransferCase(multipart_size=128, want_done="0-127", want_todo="128-1023", want_written="0-127"),
    # # It tests multipart working when multipart_size < content_length.
    # mismatching_document_length = TransferCase(want_done="0-49", document_length=50),
    # # It tests multipart working when multipart_size < content_length.
    # no_document_length = TransferCase(want_done="0-1023", document_length=None),
    # It tests multipart is disabled when one max_connections is 1.
    one_connection=TransferCase(multipart_size=128, max_connections=1, want_done="0-1023"),
    # It tests when max_connections < 0.
    invalid_max_connections=TransferCase(multipart_size=128, max_connections=0, want_error=ValueError),
    # It tests resuming from an existing status.
    resume_status=TransferCase(
        resume_status={"done": "128-255", "url": URL, "document_length": len(DATA)}, want_done="0-255", want_todo="256-1023"
    ),
    # It tests disabling resuming from an existing status.
    no_resume=TransferCase(resume=False, resume_status={"done": "128-255", "url": URL, "content_length": 1024}, want_done="0-1023"),
    # It tests mismatching URL in the status file produces re-starting transfer from the beginning
    mismach_status_url=TransferCase(resume_status={"done": "128-255", "url": MISMATCH_URL, "content_length": 1024}, want_done="0-1023"),
    # It tests mismatching content_length in the status file produces re-starting transfer from the beginning
    mismach_status_content_length=TransferCase(
        resume_status={"done": "128-255", "url": MISMATCH_URL, "content_length": 212}, want_done="0-1023"
    ),
)
def test_transfer(case: TransferCase, executor: DummyExecutor, tmpdir: os.PathLike) -> None:
    client = DummyClient.from_config(Config())
    path = os.path.join(str(tmpdir), os.path.basename(case.url))
    status_path = path + ".status"
    if case.resume_status is not None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as fd:
            fd.write(b"\0" * 1024)
        with open(status_path, "w") as fd:
            json.dump(case.resume_status, fd)
    try:
        transfer = Transfer(
            client=client,
            url=case.url,
            document_length=case.document_length,
            path=path,
            executor=executor,
            todo=rangeset(case.todo),
            multipart_size=case.multipart_size,
            max_connections=case.max_connections,
            resume=case.resume,
        )
    except ValueError as exc:
        assert case.want_error is not None
        assert isinstance(exc, case.want_error)
        return

    # It verifies the initial state after creation
    want_todo = rangeset(case.todo) or Range(0, len(DATA))
    want_done = NO_RANGE
    if case.resume and case.resume_status is not None and case.resume_status["url"] == case.url:
        want_done = rangeset(case.resume_status["done"])
        want_todo = want_todo - want_done

    assert transfer.path == path
    assert transfer.url == transfer.url
    assert transfer.todo == want_todo
    assert transfer.done == want_done
    assert transfer.document_length == case.want_document_length

    assert case.want_error is None
    transfer.start()
    executor.execute_tasks()

    # It verifies the status after the first task execution
    want_done = rangeset(case.want_done)
    want_todo = rangeset(case.want_todo)
    want_written = rangeset(case.want_written)
    assert transfer.done == want_done
    assert transfer.todo == want_todo
    assert transfer.document_length == case.want_document_length

    # It verifies the file has been written
    assert os.path.isfile(path)
    with open(path, "rb") as fd:
        data = fd.read()

    for r in want_written:
        assert data[r.start : r.end] == DATA[r.start : r.end]

    # It verifies the status file has been written
    assert os.path.isfile(status_path)
    with open(status_path) as fd:
        status = json.load(fd)
    assert status["url"] == case.url
    assert status["document_length"] == case.want_document_length
    assert status["done"] == case.want_done

    # It verifies the transfer can be resumed from the file status
    transfer2 = Transfer(
        client=client, url=case.url, path=path, document_length=case.document_length, todo=rangeset(case.todo), executor=executor
    )
    assert transfer2.path == path
    assert transfer2.url == transfer.url
    assert transfer2.done == transfer.done
    assert transfer2.todo == transfer.todo
    assert transfer2.document_length == transfer.document_length
