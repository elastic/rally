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
import logging
import os
import subprocess

import pytest

import it
from esrally import version
from esrally.utils import cases

LOG = logging.getLogger(__name__)


@cases.cases(
    arg_name="command",
    help="--help",
    race_geonames=(
        "race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    ),
    override_cmd_race_geonames=(
        "esrally race --pipeline=benchmark-only --test-mode --track=geonames "
        "--challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    ),
    list_tracks="list tracks",
)
def test_docker_compose(command: str):
    run_compose(command)


def run_compose(
    command: str,
    compose_file: str = os.path.join(it.ROOT_DIR, "docker", "docker-compose-tests.yml"),
    logging_level: int = logging.INFO,
    check: bool = True,
    fail: bool = True,
    tear_down: bool = True,
) -> subprocess.CompletedProcess:
    env = {
        "TEST_COMMAND": command,
        "RALLY_VERSION": version.__version__,
        "RALLY_VERSION_TAG": version.__version__,
    }
    LOG.log(logging_level, "Running rally with 'docker compose', command='%s', env=%r", command, env)
    try:
        result = subprocess.run(
            f"docker compose -f '{compose_file}' up --abort-on-container-exit",
            env=env,
            capture_output=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=check,
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        msg = (
            "Docker compose up failed:\n"
            f"  - command: {err.cmd}\n"
            f"  - args: {err.args}\n"
            f"  - return code: {err.returncode}\n"
            f"  - stderr:\n{err.stderr}"
        )
        if fail:
            pytest.fail(msg)
        else:
            LOG.log(logging_level, msg)
        raise
    else:
        LOG.log(logging_level, "Docker compose up succeeded. STDOUT:\n%s", result.stdout)
        return result
    finally:
        if tear_down:
            try:
                subprocess.run(
                    f"docker compose -f '{compose_file}' down -v",
                    env=env,
                    capture_output=False,  # It defines streams manually
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=True,
                    shell=True,
                )
            except subprocess.CalledProcessError as err:
                LOG.log(
                    logging_level,
                    "Docker compose down failed:\n  - command: '%s'\n  - args: %s\n  - return code: %s\n  - stderr:\n%s",
                    err.cmd,
                    err.args,
                    err.returncode,
                    err.stderr,
                )
