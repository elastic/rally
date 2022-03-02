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
from pytest import approx

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


class TestBytesToUnit:
    def test_to_na(self):
        assert convert.bytes_to_unit("N/A", 100) == 100
        assert convert.bytes_to_unit("N/A", 4500) == 4500
        assert convert.bytes_to_unit("N/A", 8004200) == 8004200
        assert convert.bytes_to_unit("N/A", 12348004200) == 12348004200

    def test_to_bytes(self):
        assert convert.bytes_to_unit("bytes", 100) == 100
        assert convert.bytes_to_unit("bytes", 4500) == 4500
        assert convert.bytes_to_unit("bytes", 8004200) == 8004200
        assert convert.bytes_to_unit("bytes", 12348004200) == 12348004200

    def test_to_kb(self):
        assert convert.bytes_to_unit("kB", 100) == approx(.098, rel=.1)
        assert convert.bytes_to_unit("kB", 4500) == approx(4.4, rel=.1)
        assert convert.bytes_to_unit("kB", 8004200) == approx(7800, rel=.1)
        assert convert.bytes_to_unit("kB", 12348004200) == approx(12000000, rel=.1)

    def test_to_mb(self):
        assert convert.bytes_to_unit("MB", 100) == 9.5367431640625e-05
        assert convert.bytes_to_unit("MB", 4500) == 0.004291534423828125
        assert convert.bytes_to_unit("MB", 8004200) == 7.633399963378906
        assert convert.bytes_to_unit("MB", 12348004200) == 11775.974464416504

    def test_to_gb(self):
        assert convert.bytes_to_unit("GB", 100) == 9.313225746154785e-08
        assert convert.bytes_to_unit("GB", 4500) == 4.190951585769653e-06
        assert convert.bytes_to_unit("GB", 8004200) == 0.007454492151737213
        assert convert.bytes_to_unit("GB", 12348004200) == 11.499975062906742
