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

import pytest

from esrally.utils import convert


class TestToBool:
    def test_convert_to_true(self):
        values = ["True", "true", "Yes", "yes", "t", "y", "1", True]
        for value in values:
            assert convert.to_bool(value) is True

    def test_convert_to_false(self):
        values = ["False", "false", "No", "no", "f", "n", "0", False]
        for value in values:
            assert convert.to_bool(value) is False

    def test_cannot_convert_invalid_value(self):
        values = ["Invalid", None, []]
        for value in values:
            with pytest.raises(ValueError) as exc:
                convert.to_bool(value)
            assert exc.value.args[0] == f"Cannot convert [{value}] to bool."


class TestBytesToHuman:
    def test_none(self):
        assert convert.bytes_to_human_string(None) == "N/A"
        assert convert.bytes_to_human_value(None) == None
        assert convert.bytes_to_human_unit(None) == "N/A"

    def test_positive_bytes(self):
        assert convert.bytes_to_human_string(100) == "100 bytes"
        assert convert.bytes_to_human_value(100) == 100
        assert convert.bytes_to_human_unit(100) == "bytes"

    def test_negative_bytes(self):
        assert convert.bytes_to_human_string(-100) == "-100 bytes"
        assert convert.bytes_to_human_value(-100) == -100
        assert convert.bytes_to_human_unit(-100) == "bytes"

    def test_positive_kb(self):
        assert convert.bytes_to_human_string(8808) == "8.6 kB"
        assert convert.bytes_to_human_value(8808) == 8.6015625
        assert convert.bytes_to_human_unit(8808) == "kB"

    def test_negative_kb(self):
        assert convert.bytes_to_human_string(-88134) == "-86.1 kB"
        assert convert.bytes_to_human_value(-88134) == -86.068359375
        assert convert.bytes_to_human_unit(-88134) == "kB"

    def test_positive_mb(self):
        assert convert.bytes_to_human_string(8808812) == "8.4 MB"
        assert convert.bytes_to_human_value(8808812) == 8.400737762451172
        assert convert.bytes_to_human_unit(8808812) == "MB"

    def test_negative_mb(self):
        assert convert.bytes_to_human_string(-881348666) == "-840.5 MB"
        assert convert.bytes_to_human_value(-881348666) == -840.5195865631104
        assert convert.bytes_to_human_unit(-881348666) == "MB"

    def test_positive_gb(self):
        assert convert.bytes_to_human_string(8808812123) == "8.2 GB"
        assert convert.bytes_to_human_value(8808812123) == 8.2038455856964
        assert convert.bytes_to_human_unit(8808812123) == "GB"

    def test_negative_gb(self):
        assert convert.bytes_to_human_string(-881348666323) == "-820.8 GB"
        assert convert.bytes_to_human_value(-881348666323) == -820.8199090538546
        assert convert.bytes_to_human_unit(-881348666323) == "GB"
