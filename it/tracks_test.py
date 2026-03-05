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
import os
from collections.abc import Generator

import pytest

from esrally import config
from esrally.utils import compose

TRACK_NAMES_FILE = os.path.join(os.path.dirname(__file__), "resources", "track-names.txt")
with open(TRACK_NAMES_FILE) as f:
    TRACK_NAMES = f.read().splitlines()


@pytest.fixture(scope="module", autouse=True)
def compose_config() -> Generator[compose.ComposeConfig]:
    cfg = compose.ComposeConfig()
    config.init_config(cfg=cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="module", autouse=True)
def build_rally(compose_config):
    compose.build_rally()


@pytest.fixture(params=TRACK_NAMES)
def track_name(request) -> Generator[str]:
    yield request.param


def test_tracks(compose_config: compose.ComposeConfig, track_name: str, build_rally):
    if track_name not in TRACK_NAMES:
        pytest.skip(f"Track not selected for testing [{track_name}].")
    compose.race(track_name=track_name, test_mode=True, target_hosts=["es01:9200"])
