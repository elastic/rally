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
import subprocess

import pytest

import it
from esrally import version
from esrally.utils import cases


@cases.cases(
    arg_name="command",
    help="--help",
    race_geonames=(
        "race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only " "--target-hosts=es01:9200"
    ),
    list_tracks="list tracks",
)
def test_docker_compose(command: str):
    env = os.environ.copy()
    env["TEST_COMMAND"] = command
    env["RALLY_DOCKER_IMAGE"] = "elastic/rally"
    env["RALLY_VERSION"] = version.__version__
    env["RALLY_VERSION_TAG"] = version.__version__

    try:
        return subprocess.run(
            f"docker compose -f {it.ROOT_DIR}/docker/docker-compose-tests.yml up --abort-on-container-exit",
            env=env,
            capture_output=False,  # We'll define streams manually
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        pytest.fail(f"Docker compose test failed:\n{err.stdout}")
    finally:
        subprocess.run(f"docker compose -f {it.ROOT_DIR}/docker/docker-compose-tests.yml down -v", shell=True, check=False)
