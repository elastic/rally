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

import copy
import json
import os
import time
from typing import Any

import pytest

from esrally import log


@pytest.fixture
def template() -> dict[str, Any]:
    with open(log.TEMPLATE_PATH) as fd:
        return json.load(fd)


@pytest.fixture
def config(tmpdir, template: dict[str, Any]) -> dict[str, Any]:
    config = copy.deepcopy(template)
    # change existing to differ from source template, showing that we don't overwrite any existing loggers config
    config["loggers"]["rally.profile"]["level"] = "DEBUG"
    # simulate user missing 'elastic_transport' in logging.json
    del config["loggers"]["elastic_transport"]
    return config


@pytest.fixture
def config_path(tmpdir, config: dict[str, Any]) -> str:
    path = os.path.join(tmpdir, "config.json")
    with open(path, "w") as fd:
        json.dump(config, fd)
    return path


def test_add_missing_loggers_to_config_missing(template: dict[str, Any], config: dict[str, Any], config_path: str) -> None:
    log.add_missing_loggers_to_config(config_path=config_path)

    with open(config_path) as fd:
        got = json.load(fd)

    want = copy.deepcopy(config)
    want["loggers"].update((k, v) for k, v in template["loggers"].items() if k not in config["loggers"])

    assert got["loggers"] == want["loggers"]


LOG_FORMAT = "%(asctime)s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def test_configure_formatter_utc():
    formatter = log.configure_utc_formatter(format=LOG_FORMAT, datefmt=DATE_FORMAT)
    assert formatter.converter is time.gmtime


def test_configure_formatter_localtime():
    formatter = log.configure_utc_formatter(format=LOG_FORMAT, datefmt=DATE_FORMAT, timezone="localtime")
    assert formatter.converter is time.localtime
