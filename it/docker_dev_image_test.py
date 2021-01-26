# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import it

from esrally.utils import process
from esrally import version


def test_docker_geonames():
    test_command = "race --pipeline=benchmark-only --test-mode --track=geonames " \
                   "--challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    run_docker_compose_test(test_command)


def test_docker_list_tracks():
    test_command = "list tracks"
    run_docker_compose_test(test_command)


def test_docker_help():
    test_command = "--help"
    run_docker_compose_test(test_command)


def test_docker_override_cmd():
    test_command = "esrally race --pipeline=benchmark-only --test-mode --track=geonames " \
                   "--challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    run_docker_compose_test(test_command)


def run_docker_compose_test(test_command):
    try:
        if run_docker_compose_up(test_command) != 0:
            raise AssertionError(f"The docker-compose test failed with test command: {test_command}")
    finally:
        # Always ensure proper cleanup regardless of results
        run_docker_compose_down()


def run_docker_compose_up(test_command):
    env_variables = os.environ.copy()
    env_variables["TEST_COMMAND"] = test_command
    env_variables['RALLY_VERSION'] = version.__version__

    return process.run_subprocess_with_logging(f"docker-compose -f {it.ROOT_DIR}/docker/docker-compose-tests.yml up "
                                               f"--abort-on-container-exit", env=env_variables)


def run_docker_compose_down():
    if process.run_subprocess_with_logging(f"docker-compose -f {it.ROOT_DIR}/docker/docker-compose-tests.yml down -v") != 0:
        raise AssertionError("Failed to stop running containers from docker-compose-tests.yml")
