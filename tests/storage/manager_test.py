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
import os
from collections.abc import Generator
from typing import Any

import pytest

from esrally import types
from esrally.storage import (
    Head,
    StorageConfig,
    TransferManager,
    _manager,
    dummy,
    get_transfer_manager,
    init_transfer_manager,
    shutdown_transfer_manager,
)
from esrally.utils.cases import cases


@pytest.fixture(scope="function")
def cfg(request, tmpdir: os.PathLike) -> types.Config:
    cfg = StorageConfig()
    cfg.adapters = (f"{__name__}:StorageAdapter",)
    cfg.local_dir = str(tmpdir)
    return cfg


@pytest.fixture(scope="function")
def dummy_executor(monkeypatch: pytest.MonkeyPatch) -> Generator[dummy.DummyExecutor]:
    executor = dummy.DummyExecutor()
    monkeypatch.setattr(_manager, "executor_from_config", lambda cfg: executor)
    try:
        yield executor
    finally:
        executor.shutdown()


@pytest.fixture
def manager(cfg: types.Config, dummy_executor: dummy.DummyExecutor) -> Generator[TransferManager]:
    manager = init_transfer_manager(cfg=cfg)
    try:
        yield manager
    finally:
        shutdown_transfer_manager()


SIMPLE_URL = "http://example.com"
SIMPLE_DATA = b"example document"
SIMPLE_HEAD = Head(url=SIMPLE_URL, content_length=len(SIMPLE_DATA))


@dataclasses.dataclass
class StorageAdapter(dummy.DummyAdapter):

    heads: dict[str, Head] = dataclasses.field(default_factory=lambda: {SIMPLE_URL: SIMPLE_HEAD})
    data: dict[str, bytes] = dataclasses.field(default_factory=lambda: {SIMPLE_URL: SIMPLE_DATA})


@dataclasses.dataclass
class GetCase:
    url: str
    path: os.PathLike | str | None = None
    document_length: int | None = None
    want_data: bytes | None = None
    want_error: tuple[type[Exception], ...] = tuple()


@cases(
    simple=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA),
    path=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA, path="some/path"),
    document_length=GetCase(url=SIMPLE_URL, want_data=SIMPLE_DATA, document_length=len(SIMPLE_DATA)),
    mismach_document_length=GetCase(url=SIMPLE_URL, want_error=(ValueError,), document_length=len(SIMPLE_DATA) - 1),
)
def test_get(case: GetCase, manager: TransferManager, dummy_executor: dummy.DummyExecutor, tmpdir: os.PathLike) -> None:
    kwargs: dict[str, Any] = {}
    if case.path is not None:
        kwargs["path"] = os.path.join(tmpdir, case.path)
    if case.document_length is not None:
        kwargs["document_length"] = case.document_length

    try:
        tr = manager.get(url=case.url, **kwargs)
    except case.want_error:
        return

    assert not case.want_error

    got = tr.wait(timeout=0.0)
    assert not got

    if case.path is not None:
        assert os.path.join(tmpdir, case.path) == tr.path

    dummy_executor.execute_tasks()
    got = tr.wait(timeout=0.0)
    assert got

    if case.want_data is not None:
        assert os.path.exists(tr.path)
        if case.want_data is not None:
            with open(tr.path, "rb") as f:
                assert f.read() == SIMPLE_DATA
    if case.document_length is not None:
        assert os.path.getsize(tr.path) == case.document_length


@pytest.fixture(scope="function", autouse=True)
def cleanup_transfer_manager():
    shutdown_transfer_manager()
    yield
    shutdown_transfer_manager()


def test_transfer_manager(tmpdir: os.PathLike, cfg: StorageConfig) -> None:
    manager = init_transfer_manager(cfg=cfg)
    assert isinstance(manager, TransferManager)

    tr = manager.get(url=SIMPLE_URL, document_length=len(SIMPLE_DATA))
    assert tr.wait(timeout=10.0)
    assert os.path.exists(tr.path)
    assert os.path.getsize(tr.path) == len(SIMPLE_DATA)

    assert manager is get_transfer_manager()

    shutdown_transfer_manager()

    with pytest.raises(RuntimeError):
        assert get_transfer_manager()
