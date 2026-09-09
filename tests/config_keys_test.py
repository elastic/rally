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

import typing

from esrally import config_keys, types


class TestConfigKeys:
    def test_constants_match_types_key_literal(self):
        """Every config key constant must be a valid member of ``types.Key``.

        This keeps the constants in sync with the static type contract and
        ensures a typo in either place surfaces here as a test failure.
        """
        valid_keys = set(typing.get_args(types.Key))
        for name, value in vars(config_keys).items():
            if name.startswith("_") or name == "SYSTEM_SECTION":
                continue
            if isinstance(value, str) and "." in value:
                assert value in valid_keys, f"{name} = {value!r} is not a valid types.Key"

    def test_constants_are_unique(self):
        values = [
            value
            for name, value in vars(config_keys).items()
            if not name.startswith("_") and isinstance(value, str)
        ]
        assert len(values) == len(set(values)), "config key constants are not unique"

    def test_system_section(self):
        assert config_keys.SYSTEM_SECTION == "system"
        assert "system" in typing.get_args(types.Section)
