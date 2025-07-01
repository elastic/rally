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

from dataclasses import dataclass

import pytest

from esrally.utils import convert
from esrally.utils.cases import cases


class TestToBool:
    def test_convert_to_true(self):
        values: list[str | bool] = ["True", "true", "Yes", "yes", "t", "y", "1", True]
        for value in values:
            assert convert.to_bool(value) is True

    def test_convert_to_false(self):
        values: list[str | bool] = ["False", "false", "No", "no", "f", "n", "0", False]
        for value in values:
            assert convert.to_bool(value) is False

    def test_cannot_convert_invalid_value(self):
        values = ["Invalid", None, []]
        for value in values:
            with pytest.raises(ValueError) as exc:
                convert.to_bool(value)  # type: ignore
            assert exc.value.args[0] == f"Cannot convert [{value}] to bool."


class TestBytesToHuman:
    def test_none(self):
        assert convert.bytes_to_human_value(None) is None
        assert convert.bytes_to_human_unit(None) == "N/A"

    def test_positive_bytes(self):
        assert convert.bytes_to_human_value(100) == 100
        assert convert.bytes_to_human_unit(100) == "B"

    def test_negative_bytes(self):
        assert convert.bytes_to_human_value(-100) == -100
        assert convert.bytes_to_human_unit(-100) == "B"

    def test_positive_kb(self):
        assert convert.bytes_to_human_value(8808) == 8.6015625
        assert convert.bytes_to_human_unit(8808) == "KB"

    def test_negative_kb(self):
        assert convert.bytes_to_human_value(-88134) == -86.068359375
        assert convert.bytes_to_human_unit(-88134) == "KB"

    def test_positive_mb(self):
        assert convert.bytes_to_human_value(8808812) == 8.400737762451172
        assert convert.bytes_to_human_unit(8808812) == "MB"

    def test_negative_mb(self):
        assert convert.bytes_to_human_value(-881348666) == -840.5195865631104
        assert convert.bytes_to_human_unit(-881348666) == "MB"

    def test_positive_gb(self):
        assert convert.bytes_to_human_value(8808812123) == 8.2038455856964
        assert convert.bytes_to_human_unit(8808812123) == "GB"

    def test_negative_gb(self):
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
        assert convert.bytes_to_unit("B", None) is None
        assert convert.bytes_to_unit("B", 100) == 100
        assert convert.bytes_to_unit("B", 4500) == 4500
        assert convert.bytes_to_unit("B", 8004200) == 8004200
        assert convert.bytes_to_unit("B", 12348004200) == 12348004200

    def test_to_kb(self):
        assert convert.bytes_to_unit("KB", None) is None
        assert convert.bytes_to_unit("KB", 100) == pytest.approx(0.098, rel=0.1)
        assert convert.bytes_to_unit("KB", 4500) == pytest.approx(4.4, rel=0.1)
        assert convert.bytes_to_unit("KB", 8004200) == pytest.approx(7800, rel=0.1)
        assert convert.bytes_to_unit("KB", 12348004200) == pytest.approx(12000000, rel=0.1)

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


@dataclass()
class DurationCase:
    value: float | int
    want_str: str
    unit: convert.Duration.Unit = convert.Duration.Unit.S


@cases(
    zero=DurationCase(0, "0s"),
    nanos=DurationCase(1.23456789, "1ns", convert.Duration.Unit.NS),
    micros=DurationCase(1.23456789, "1.23us", convert.Duration.Unit.US),
    millis=DurationCase(1.23456789, "1.23ms", convert.Duration.Unit.MS),
    seconds=DurationCase(1.23456789, "1.23s", convert.Duration.Unit.S),
    minutes=DurationCase(1.23456789, "1m 14s", convert.Duration.Unit.M),
    hours=DurationCase(1.23456789, "1h 14m 4s", convert.Duration.Unit.H),
    days=DurationCase(1.23456789, "1d 5h 37m 46s", convert.Duration.Unit.D),
    integer=DurationCase(42, "42s"),
    float=DurationCase(12.345, "12.35s"),
    minute=DurationCase(60, "1m"),
    hundred=DurationCase(1e2, "1m 40s"),
    thausands=DurationCase(1e3, "16m 40s"),
    hour=DurationCase(3600, "1h"),
    day=DurationCase(86400, "1d"),
    milions=DurationCase(1e6, "11d 13h 46m 40s"),
)
def test_duration(case: DurationCase):
    got = convert.duration(case.value, case.unit)
    assert str(got) == case.want_str


@dataclass()
class SizeCase:
    value: float | int
    want: int
    want_to_unit: float
    want_unit: convert.Size.Unit
    want_str: str
    unit: convert.Size.Unit = convert.Size.Unit.B


@cases(
    zero=SizeCase(0, 0, 0.0, convert.Size.Unit.B, "0B"),
    float=SizeCase(1.234, 1, 1.0, convert.Size.Unit.B, "1B"),
    integer=SizeCase(42, 42, 42.0, convert.Size.Unit.B, "42B"),
    bytes=SizeCase(1.234, 1, 1.0, convert.Size.Unit.B, "1B", unit=convert.Size.Unit.B),
    kylos=SizeCase(1.234, 1263, 1.2333984375, convert.Size.Unit.KB, "1.2KB", unit=convert.Size.Unit.KB),
    megas=SizeCase(1.234, 1293942, 1.233999252319336, convert.Size.Unit.MB, "1.2MB", unit=convert.Size.Unit.MB),
    gigas=SizeCase(1.234, 1324997410, 1.2339999992400408, convert.Size.Unit.GB, "1.2GB", unit=convert.Size.Unit.GB),
    teras=SizeCase(1.234, 1356797348675, 1.2339999999994689, convert.Size.Unit.TB, "1.2TB", unit=convert.Size.Unit.TB),
    hundred=SizeCase(100, 100, 100.0, convert.Size.Unit.B, "100B"),
    thausands=SizeCase(3800, 3800, 3.7109375, convert.Size.Unit.KB, "3.7KB"),
    hundred_kilos=SizeCase(102400, 102400, 100.0, convert.Size.Unit.KB, "100.0KB"),
    milions=SizeCase(5000000, 5000000, 4.76837158203125, convert.Size.Unit.MB, "4.8MB"),
    bilions=SizeCase(4000000000, 4000000000, 3.725290298461914, convert.Size.Unit.GB, "3.7GB"),
    trillions=SizeCase(2000000000000, 2000000000000, 1.8189894035458565, convert.Size.Unit.TB, "1.8TB"),
)
def test_size(case: SizeCase):
    got = convert.size(case.value, unit=case.unit)
    assert got == case.want
    assert got.to_unit(got.unit) == case.want_to_unit
    assert got.unit == case.want_unit
    assert str(got) == case.want_str
