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

from esrally.storage._range import (
    MAX_LENGTH,
    NO_RANGE,
    Range,
    RangeSet,
    RangeTree,
    rangeset,
)
from esrally.utils.cases import cases


@dataclass()
class RangeSetCase:
    given: str
    want: RangeSet | type[BaseException]


@cases(
    no_range=RangeSetCase("", NO_RANGE),
    single=RangeSetCase("1", Range(1, 2)),
    range=RangeSetCase("2-4", Range(2, 5)),
    disjoint=RangeSetCase("2-3, 5-6", RangeTree(Range(2, 4), Range(5, 7))),
    intersection=RangeSetCase("2-5, 3-6", Range(2, 7)),
    unordered=RangeSetCase("4-6, 2-2", RangeTree(Range(2, 3), Range(4, 7))),
    value_error=RangeSetCase("2-1", ValueError),
)
def test_rangeset(case: RangeSetCase):
    try:
        got = rangeset(case.given)
    except ValueError as ex:
        got = ex
    if isinstance(case.want, RangeSet):
        assert got == case.want
    elif issubclass(case.want, BaseException):
        assert isinstance(got, case.want)


@dataclass()
class BinOpCase:
    first: str
    second: str
    want: str


@cases(
    no_range=BinOpCase("", "", ""),
    no_range_range=BinOpCase("", "1-2", "1-2"),
    no_range_rangeset=BinOpCase("", "1,3-5", "1,3-5"),
    disjoint_left=BinOpCase("3-5", "0-1", "0-1,3-5"),
    disjoint_rigth=BinOpCase("1-2", "3-5", "1-2,3-5"),
    same=BinOpCase("3-5", "3-5", "3-5"),
    intersect_left=BinOpCase("3-6", "1-4", "1-6"),
    intersect_rigth=BinOpCase("1-4", "3-6", "1-6"),
    inner=BinOpCase("1-10", "3-5", "1-10"),
    outer=BinOpCase("3-5", "1-10", "1-10"),
    consecutive_left=BinOpCase("7-8", "3-6", "3-8"),
    consecutive_right=BinOpCase("3-6", "7-8", "3-8"),
    rangesets=BinOpCase("1-3,5-7", "3-5,7-9", "1-9"),
)
def test_or(case: BinOpCase):
    got = rangeset(case.first) | rangeset(case.second)
    assert got == rangeset(case.want)


@cases(
    no_range=BinOpCase("", "", ""),
    no_range_range=BinOpCase("", "1-2", ""),
    no_range_rangeset=BinOpCase("", "1,3-5", ""),
    disjoint_left=BinOpCase("3-5", "0-1", ""),
    disjoint_rigth=BinOpCase("1-2", "3-5", ""),
    same=BinOpCase("3-5", "3-5", "3-5"),
    intersect_left=BinOpCase("3-6", "1-4", "3-4"),
    intersect_rigth=BinOpCase("1-4", "3-6", "3-4"),
    inner=BinOpCase("1-10", "3-5", "3-5"),
    outer=BinOpCase("3-5", "1-10", "3-5"),
    consecutive_left=BinOpCase("7-8", "3-6", ""),
    consecutive_right=BinOpCase("3-6", "7-8", ""),
    rangesets=BinOpCase("1-3,5-7", "3-5,7-9", "3,5,7"),
)
def test_and(case: BinOpCase):
    got = rangeset(case.first) & rangeset(case.second)
    assert got == rangeset(case.want)


@cases(
    no_range=BinOpCase("", "", ""),
    no_range_range=BinOpCase("", "1-2", ""),
    no_range_rangeset=BinOpCase("", "1,3-5", ""),
    disjoint_left=BinOpCase("3-5", "0-1", "3-5"),
    disjoint_rigth=BinOpCase("1-2", "3-5", "1-2"),
    same=BinOpCase("3-5", "3-5", ""),
    intersect_left=BinOpCase("3-6", "1-4", "5-6"),
    intersect_rigth=BinOpCase("1-4", "3-6", "1-2"),
    inner=BinOpCase("1-10", "3-5", "1-2, 6-10"),
    outer=BinOpCase("3-5", "1-10", ""),
    consecutive_left=BinOpCase("7-8", "3-6", "7-8"),
    consecutive_right=BinOpCase("3-6", "7-8", "3-6"),
    rangesets=BinOpCase("1-3,5-7", "3-5,7-9", "1-2,6"),
)
def test_sub(case: BinOpCase):
    got = rangeset(case.first) - rangeset(case.second)
    assert got == rangeset(case.want)


@dataclass()
class LenCase:
    given: str
    want: int


@cases(no_range=LenCase("", 0), one=LenCase("1-1", 1), two=LenCase("2-3, 5-6", 2), three=LenCase("4-6, 2-2, 0-0", 3))
def test_len(case: LenCase):
    got = len(rangeset(case.given))
    assert got == case.want


@dataclass()
class BoolCase:
    given: str
    want: bool


@cases(no_range=BoolCase("", False), one=BoolCase("1-1", True), two=BoolCase("2-3, 5-6", True), three=BoolCase("4-6, 2-2, 0-0", True))
def test_bool(case: BoolCase):
    got = bool(rangeset(case.given))
    assert got == case.want


@dataclass()
class PopCase:
    ranges: str
    want_left: str
    want_right: str
    max_size: int = MAX_LENGTH


@cases(
    no_range=PopCase("", "", ""),
    range=PopCase("1-1", "1-1", ""),
    tree=PopCase("0-0, 2-2, 4-6", "0-0", "2-2, 4-6"),
    no_range_max_size=PopCase("", "", "", max_size=10),
    range_max_size=PopCase("1-5", "1-2", "3-5", max_size=2),
    tree_max_size1=PopCase("0-4, 5-6", "0-2", "3-4, 5-6", max_size=3),
    tree_max_size2=PopCase("0-1, 3-6", "0-1", "3-6", max_size=5),
)
def test_split(case: PopCase):
    left, right = rangeset(case.ranges).split(max_size=case.max_size)
    assert left == rangeset(case.want_left)
    assert right == rangeset(case.want_right)
