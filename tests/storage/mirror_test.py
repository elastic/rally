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

import os.path
from dataclasses import dataclass

import pytest

from esrally.config import Config, Scope
from esrally.storage._mirror import MirrorList
from esrally.utils.cases import cases

BASE_URL = "https://rally-tracks.elastic.co/"
SOME_PATH = "some/file.json.bz2"
URL = f"{BASE_URL}/{SOME_PATH}"
MIRROR1_URL = "https://rally-tracks-eu-central-1.s3.eu-central-1.amazonaws.com"
MIRROR2_URL = "https://rally-tracks-us-west-1.s3.us-west-1.amazonaws.com"
MIRRORS = {
    BASE_URL: {MIRROR1_URL, MIRROR2_URL},
}
MIRROR_FILES = os.path.join(os.path.dirname(__file__), "mirrors.json")


@pytest.fixture
def cfg():
    cfg = Config()
    cfg.add(Scope.application, "storage", "storage.mirrors_files", MIRROR_FILES)
    return cfg


@dataclass()
class FromConfigCase:
    url: str
    want: set[str]


@cases(
    empty=FromConfigCase("", want=set()),
    simple=FromConfigCase(URL, want={f"{MIRROR1_URL}/{SOME_PATH}", f"{MIRROR2_URL}/{SOME_PATH}"}),
    normalized=FromConfigCase("https://rally-tracks.elastic.co/", want={f"{MIRROR1_URL}/", f"{MIRROR2_URL}/"}),
)
def test_from_config(case: FromConfigCase, cfg: Config):
    mirrors = MirrorList.from_config(cfg)
    got = mirrors.resolve(case.url)
    assert got == case.want
