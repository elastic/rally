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

import base64
import dataclasses
import os.path
from collections.abc import Generator

import pytest

from esrally import storage
from esrally.utils import cases, crc32c

BASE_URLS = {
    # "default": "https://rally-tracks.elastic.co",
    "gs": "gs://rally-tracks"
}


@pytest.fixture(params=BASE_URLS.keys())
def base_url(request) -> str:
    return BASE_URLS[request.param]


@pytest.fixture
def storage_config(tmpdir) -> storage.StorageConfig:
    cfg = storage.StorageConfig()
    cfg.local_dir = str(tmpdir)
    cfg.mirror_files = []
    return cfg


@pytest.fixture
def transfer_manager(storage_config: storage.StorageConfig) -> Generator[storage.TransferManager]:
    manager = storage.init_transfer_manager(cfg=storage_config)
    try:
        yield manager
    finally:
        storage.shutdown_transfer_manager()


READ_CHUNK_SIZE = 1024 * 1024
SMALL_FILE = "big5/logs-1-1k.ndjson.bz2"
BIG_FILE = "big5/logs-1.ndjson.bz2"


@dataclasses.dataclass
class GetCase:
    path: str
    want_size: int


@pytest.mark.slow
@cases.cases(
    small_file=GetCase(SMALL_FILE, want_size=55896),
    big_file=GetCase(BIG_FILE, want_size=7103110671),
)
def test_get(case: GetCase, base_url: str, transfer_manager: storage.TransferManager):
    tr = transfer_manager.get(f"{base_url}/{case.path}")
    tr.wait(timeout=300.0)
    assert tr.done
    assert os.path.isfile(tr.path)
    assert os.path.getsize(tr.path) == case.want_size
    assert tr.crc32c is not None
    assert crc32c_from_file(tr.path) == crc32c_from_string(tr.crc32c)


def crc32c_from_string(value: str) -> int:
    return int.from_bytes(base64.b64decode(value), "big")


def crc32c_from_file(path: str) -> int:
    checksum = crc32c.Checksum()
    with open(path, "rb") as fd:
        while chunk := fd.read(READ_CHUNK_SIZE):
            checksum.update(chunk)
    return checksum.value
