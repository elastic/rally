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
import os
import subprocess
import time
from collections.abc import Generator

import pytest

from esrally import config
from esrally.track import loader
from esrally.utils import compose

LOG = logging.getLogger(__name__)

RALLY_TRACKS = loader.load_tracks_file()["tracks"]
ES_VERSIONS = ["8.19.10", "9.3.1"]

# Let have 120 minutes for all track sto complete. In case it times out before
# returning any other error, we will accept the testing outcome as it would require
# too much to complete all the tests in a reasonable time frame. We can always
# increase this timeout if we see that some tracks require much more time to complete.
TEST_TIMEOUT_M = os.environ.get("IT_TRACKS_TIMEOUT_MINUTES", 120)
# The max time for a single track to complete is the total time divided by the number of tracks and ES versions we
# test against.
RACE_TIMEOUT_S = TEST_TIMEOUT_M * 60 / len(RALLY_TRACKS) / len(ES_VERSIONS)


@pytest.fixture(scope="module", autouse=True)
def compose_config() -> Generator[compose.ComposeConfig]:
    cfg = compose.ComposeConfig()
    config.init_config(cfg=cfg)
    yield cfg
    config.clear_config()


@pytest.fixture(scope="module", autouse=True)
def build_rally(compose_config):
    compose.build_rally()


@pytest.fixture(scope="module", params=RALLY_TRACKS, ids=lambda param: f"track_{param['name']}")
def rally_track(request) -> Generator[loader.TrackJson]:
    yield request.param


@pytest.fixture(scope="module", params=ES_VERSIONS, ids=lambda param: f"es_version_{param}")
def elasticsearch_version(request) -> Generator[str]:
    return request.param


def test_tracks(compose_config: compose.ComposeConfig, rally_track: loader.TrackJson, elasticsearch_version):
    LOG.info("Testing elasticsearch version:\n%s", elasticsearch_version)
    LOG.info("Testing track: %s", json.dumps(rally_track))
    LOG.info("Testing timeout: %d seconds", RACE_TIMEOUT_S)
    start_time = time.time()
    try:
        compose.race(
            track_name=rally_track["name"],
            test_mode=True,
            target_hosts=["es01:9200"],
            elasticsearch_version=elasticsearch_version,
            timeout=RACE_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired:
        LOG.warning("Race timeout: no errors until we take it as a success.")
    finally:
        end_time = time.time()
        LOG.info(
            "Race terminated after %s seconds for track [%s] and elasticsearch version [%s]",
            end_time - start_time,
            rally_track["name"],
            elasticsearch_version,
        )
