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

    @mock.patch("json.load")
    def test_add_missing_loggers_to_config_missing(self, mock_json_load, caplog):
        source_template = copy.deepcopy(self.configuration)
        existing_configuration = copy.deepcopy(self.configuration)
        # change existing to differ from source template, showing that we don't overwrite any existing loggers config
        existing_configuration["loggers"]["rally.profile"]["level"] = "DEBUG"
        expected_configuration = json.dumps(copy.deepcopy(existing_configuration), indent=2)
        # simulate user missing 'elastic_transport' in logging.json
        del existing_configuration["loggers"]["elastic_transport"]

        # first loads template, then existing configuration
        mock_json_load.side_effect = [source_template, existing_configuration]

        with patch("builtins.open", mock_open()) as mock_file:
            log.add_missing_loggers_to_config()
            handle = mock_file()
            handle.write.assert_called_once_with(expected_configuration)

        assert (
            "Found loggers [{'elastic_transport': {'handlers': ['rally_log_handler'], 'level': 'WARNING', 'propagate': "
            "False}}] in source template that weren't present in the existing configuration, adding them." in caplog.text
        )
