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
import dataclasses
import os.path

import pytest

from esrally.storage import StorageConfig, _mirror
from esrally.utils import cases

BASE_URL = "https://rally-tracks.elastic.co"
SOME_PATH = "some/file.json.bz2"
URL = f"{BASE_URL}/{SOME_PATH}"
MIRROR1_URL = "https://rally-tracks-eu-central-1.s3.eu-central-1.amazonaws.com"
MIRROR2_URL = "https://rally-tracks-us-west-1.s3.us-west-1.amazonaws.com"
MIRRORS = {
    BASE_URL: {MIRROR1_URL, f"{MIRROR2_URL}/"},
}
MIRROR_FILES = os.path.join(os.path.dirname(__file__), "mirrors.json")


def storage_config(**kwargs) -> StorageConfig:
    cfg = StorageConfig()
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


@dataclasses.dataclass()
class FromConfigCase:
    cfg: StorageConfig
    want_error: type[Exception] | None = None
    want_mirror_files: set[str] = dataclasses.field(default_factory=lambda: set(StorageConfig.DEFAULT_MIRROR_FILES))
    want_urls: dict[str, set[str]] | None = None


@pytest.fixture(autouse=True)
def patch_default_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(StorageConfig, "DEFAULT_MIRROR_FILES", tuple())


@cases.cases(
    default=FromConfigCase(storage_config(), want_urls={}),
    mirror_files=FromConfigCase(
        storage_config(mirror_files=MIRROR_FILES), want_urls={f"{BASE_URL}/": {f"{MIRROR1_URL}/", f"{MIRROR2_URL}/"}}
    ),
    invalid_mirror_files=FromConfigCase(storage_config(mirror_files="<!invalid-file-path!>"), want_urls={}),
)
def test_from_config(case: FromConfigCase, monkeypatch):
    try:
        got_mirrors = _mirror.MirrorList.from_config(case.cfg)
        got_error = None
    except Exception as ex:
        got_mirrors = None
        got_error = ex

    if case.want_urls is not None:
        assert got_mirrors is not None
        assert dict(got_mirrors._urls) == case.want_urls  # pylint: disable=protected-access
    if case.want_error is not None:
        assert got_error is not None
        assert isinstance(got_error, case.want_error)


@dataclasses.dataclass()
class ResolveCase:
    url: str
    want: list[str] | None = None
    want_error: type[Exception] | None = None


@cases.cases(
    empty=ResolveCase("", want_error=Exception),
    simple=ResolveCase(URL, want=[f"{MIRROR1_URL}/{SOME_PATH}", f"{MIRROR2_URL}/{SOME_PATH}"]),
    normalized=ResolveCase("https://rally-tracks.elastic.co/", want=[f"{MIRROR1_URL}/", f"{MIRROR2_URL}/"]),
)
def test_resolve(case: ResolveCase):
    mirrors = _mirror.MirrorList(urls=MIRRORS)
    try:
        got = sorted(mirrors.resolve(case.url))
        got_error = None
    except Exception as ex:
        got = None
        got_error = ex
    if case.want is not None:
        assert got_error is None
        assert got == case.want
    if case.want_error is not None:
        assert got_error is not None
        assert isinstance(got_error, case.want_error)
