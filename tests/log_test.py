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
from unittest import mock
from unittest.mock import mock_open, patch

import pytest

from esrally import log


class TestLog:
    def _load_logging_template(self, p):
        with open(p) as f:
            return json.load(f)

    @pytest.fixture(autouse=True)
    def set_configuration(self):
        p = os.path.join(os.path.join(os.path.dirname(__file__), "..", "esrally", "resources", "logging.json"))
        self.configuration = self._load_logging_template(p)

    @mock.patch("json.dump")
    @mock.patch("json.load")
    def test_migrate_logging_configuration(self, mock_json_load, mock_json_dump, caplog):
        source_template = copy.deepcopy(self.configuration)
        existing_configuration = copy.deepcopy(self.configuration)
        # change existing to differ from source template, showing that we don't overwrite any existing loggers config
        existing_configuration["loggers"]["rally.profile"]["level"] = "DEBUG"
        expected_configuration = copy.deepcopy(existing_configuration)

        # simulate user missing 'elastic_transport' in logging.json
        del existing_configuration["loggers"]["elastic_transport"]

        # first loads template, then existing configuration
        mock_json_load.side_effect = [source_template, copy.deepcopy(existing_configuration)]

        with patch("builtins.open", mock_open()) as mock_file:
            log.migrate_logging_configuration()
            mock_json_dump.assert_called_once_with(expected_configuration, mock_file(), indent=2)

    log_format = "%(asctime)s %(message)s"

    def test_configure_time_formatter_utc(self):
        formatter = log.configure_time_formatter(format=self.log_format, datefmt="%Y-%m-%d %H:%M:%S")
        assert formatter.converter is time.gmtime

    def test_configure_time_formatter_localtime(self):
        formatter = log.configure_time_formatter(format=self.log_format, datefmt="%Y-%m-%d %H:%M:%S", timezone="localtime")
        assert formatter.converter is time.localtime
