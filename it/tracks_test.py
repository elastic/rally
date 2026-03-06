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
import json
import logging
from collections.abc import Generator

import pytest

from esrally import config
from esrally.track import loader
from esrally.utils import compose

LOG = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=True)
def compose_config() -> Generator[compose.ComposeConfig]:
    cfg = compose.ComposeConfig()
    config.init_config(cfg=cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="module", autouse=True)
def build_rally(compose_config):
    compose.build_rally()


@pytest.fixture(scope="module", params=loader.load_tracks_file()["tracks"], ids=lambda param: f"track_{param['name']}")
def track(request) -> Generator[loader.TrackJson]:
    yield request.param


@pytest.fixture(scope="module", params=["8.19.10", "9.3.1"], ids=lambda param: f"es_version_{param}")
def elasticsearch_version(request) -> Generator[str]:
    return request.param


def test_tracks(compose_config: compose.ComposeConfig, track: loader.TrackJson, elasticsearch_version):
    LOG.info("Testing elasticsearch version:\n%s", elasticsearch_version)
    LOG.info("Testing track:\n%s", json.dumps(track, indent=2))
    compose.race(track_name=track["name"], test_mode=True, target_hosts=["es01:9200"], elasticsearch_version=elasticsearch_version)
