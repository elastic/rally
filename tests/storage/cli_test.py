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
import subprocess
from typing import Any, cast

import pytest

from esrally import storage
from esrally.storage import _cli, dummy
from esrally.utils import cases

URL = "https://rally-tracks.elastic.co/apm/span.json.bz2"
PATH = "/tmp/span.json.bz2"
SIZE = 1024
CRC32C = "some-checksum"


@dataclasses.dataclass
class FakeTransfer:
    url: str = URL
    path: str = PATH
    done: storage.RangeSet = storage.Range(0, SIZE)
    document_length: int | None = SIZE
    crc32c: str | None = None
    verified: bool = False


@dataclasses.dataclass
class PutCase:
    transfer: FakeTransfer
    want_uploaded: bool


@cases.cases(
    complete_without_crc32c=PutCase(FakeTransfer(), want_uploaded=True),
    complete_verified=PutCase(FakeTransfer(crc32c=CRC32C, verified=True), want_uploaded=True),
    complete_not_verified=PutCase(FakeTransfer(crc32c=CRC32C), want_uploaded=False),
    partial=PutCase(FakeTransfer(done=storage.Range(0, SIZE // 2)), want_uploaded=False),
    empty=PutCase(FakeTransfer(done=storage.NO_RANGE), want_uploaded=False),
    unknown_document_length=PutCase(FakeTransfer(document_length=None), want_uploaded=False),
)
def test_put(case: PutCase, monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def run(command: list[str], **kwargs: Any) -> None:
        commands.append(command)

    monkeypatch.setattr(subprocess, "run", run)
    _cli.put([cast(storage.Transfer, case.transfer)], "remote:bucket")

    if case.want_uploaded:
        assert commands == [["rclone", "copy", PATH, "remote:bucket/apm"]]
    else:
        assert commands == []


def test_transfer_to_dict_includes_verified(tmpdir) -> None:
    cfg = storage.StorageConfig()
    transfer = storage.Transfer(
        client=storage.Client.from_config(cfg),
        url=URL,
        path=str(tmpdir.join("span.json.bz2")),
        executor=dummy.DummyExecutor(),
        document_length=SIZE,
        resume=False,
        cfg=cfg,
    )
    assert _cli.transfer_to_dict(transfer)["verified"] is False
