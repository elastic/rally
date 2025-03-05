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
from __future__ import annotations

import dataclasses
from typing import Any

import pytest

from esrally.utils import cases, convert


@dataclasses.dataclass
class ToBoolCase:
    value: Any
    want: bool | None = None
    want_error: str | None = None


@cases.cases(
    true=ToBoolCase(value=True, want=True),
    true_str=ToBoolCase(value="true", want=True),
    True_str=ToBoolCase(value="True", want=True),
    yes_str=ToBoolCase(value="yes", want=True),
    Yes_str=ToBoolCase(value="Yes", want=True),
    t_str=ToBoolCase(value="t", want=True),
    y_str=ToBoolCase(value="y", want=True),
    one_str=ToBoolCase(value="1", want=True),
    false=ToBoolCase(value=False, want=False),
    false_str=ToBoolCase(value="false", want=False),
    False_str=ToBoolCase(value="False", want=False),
    no_str=ToBoolCase(value="no", want=False),
    No_str=ToBoolCase(value="No", want=False),
    f_str=ToBoolCase(value="f", want=False),
    n_str=ToBoolCase(value="n", want=False),
    zero_str=ToBoolCase(value="0", want=False),
    none=ToBoolCase(value=None, want_error="Cannot convert [None] to bool."),
    list=ToBoolCase(value=[], want_error="Cannot convert [[]] to bool."),
    invalid_str=ToBoolCase(value="Invalid", want_error="Cannot convert [Invalid] to bool."),
)
def test_to_bool(case: ToBoolCase):
    try:
        got = convert.to_bool(case.value)
    except ValueError as ex:
        assert None is case.want
        assert str(ex) == case.want_error
    else:
        assert got == case.want
        assert None is case.want_error


class TestBytesToHuman:
    def test_none(self):
        assert convert.bytes_to_human_string(None) == "N/A"
        assert convert.bytes_to_human_value(None) is None
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
        assert convert.bytes_to_unit("N/A", None) is None
        assert convert.bytes_to_unit("N/A", 100) == 100
        assert convert.bytes_to_unit("N/A", 4500) == 4500
        assert convert.bytes_to_unit("N/A", 8004200) == 8004200
        assert convert.bytes_to_unit("N/A", 12348004200) == 12348004200

    def test_to_bytes(self):
        assert convert.bytes_to_unit("bytes", None) is None
        assert convert.bytes_to_unit("bytes", 100) == 100
        assert convert.bytes_to_unit("bytes", 4500) == 4500
        assert convert.bytes_to_unit("bytes", 8004200) == 8004200
        assert convert.bytes_to_unit("bytes", 12348004200) == 12348004200

    def test_to_kb(self):
        assert convert.bytes_to_unit("kb", None) is None
        assert convert.bytes_to_unit("kB", 100) == pytest.approx(0.098, rel=0.1)
        assert convert.bytes_to_unit("kB", 4500) == pytest.approx(4.4, rel=0.1)
        assert convert.bytes_to_unit("kB", 8004200) == pytest.approx(7800, rel=0.1)
        assert convert.bytes_to_unit("kB", 12348004200) == pytest.approx(12000000, rel=0.1)

    def test_to_mb(self):
        assert convert.bytes_to_unit("MB", None) is None
        assert convert.bytes_to_unit("MB", 100) == pytest.approx(9.5e-05, rel=0.1)
        assert convert.bytes_to_unit("MB", 4500) == pytest.approx(0.0043, rel=0.1)
        assert convert.bytes_to_unit("MB", 8004200) == pytest.approx(7.6, rel=0.1)
        assert convert.bytes_to_unit("MB", 12348004200) == pytest.approx(12000, rel=0.1)

    def test_to_gb(self):
        assert convert.bytes_to_unit("GB", None) is None
        assert convert.bytes_to_unit("GB", 100) == pytest.approx(9.3e-08, rel=0.1)
        assert convert.bytes_to_unit("GB", 4500) == pytest.approx(4.2e-06, rel=0.1)
        assert convert.bytes_to_unit("GB", 8004200) == pytest.approx(0.0075, rel=0.1)
        assert convert.bytes_to_unit("GB", 12348004200) == pytest.approx(11, rel=0.1)
