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

from esrally.utils import process
from it import ROOT_DIR

DOCKER_COMPOSE_UP = "docker-compose -f docker/docker-compose-tests.yml up --abort-on-container-exit"


def test_docker_geonames():
    test_command = "--pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    os.environ["TEST_COMMAND"] = test_command

    if process.run_subprocess_in_path(ROOT_DIR, DOCKER_COMPOSE_UP) != 0:
        raise AssertionError("It was not possible to run the test_docker_geoname successfully")
