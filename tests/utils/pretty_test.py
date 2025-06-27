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
from typing import Any

from esrally.utils import cases, pretty


@dataclass
class DumpCase:
    o: Any
    want: str
    flags: pretty.Flag | None = None


@cases.cases(
    # fmt: off
    null=DumpCase(None, "null"),
    string=DumpCase("string", '"string"'),
    integer=DumpCase(1, "1"),
    float=DumpCase(1.0, "1.0"),
    list=DumpCase(
        [1, 2, 3],
        "[\n"
        "  1,\n"
        "  2,\n"
        "  3\n"
        "]"
    ),
    tuple=DumpCase(
        (2, 3),
        "[\n"
        "  2,\n"
        "  3\n"
        "]"
    ),
    object=DumpCase(
        {"a": "a", "b": 2},
        '{\n'
        '  "a": "a",\n'
        '  "b": 2\n'
        '}'
    ),
    flat_dict=DumpCase(
        {"a": {"b": "c"}},
        '{\n'
        '  "a.b": "c"\n'
        '}',
        flags=pretty.Flag.FLAT_DICT
    ),
    # fmt: on
)
def test_dump(case: DumpCase):
    params: dict[str, Any] = {}
    if case.flags is not None:
        params["flags"] = case.flags
    got = pretty.dump(case.o, **params)
    assert got == case.want


@dataclass
class DiffCase:
    old: Any
    new: Any
    want: str
    flags: pretty.Flag | None = None


@cases.cases(
    # fmt: off
    none_and_none=DiffCase(None, None, ""),
    none_and_string=DiffCase(
        None,
        "something",
        '- null\n'
        '+ "something"'
    ),
    strings=DiffCase(
        "cat",
        "cut",
        '- "cat"\n'
        '?   ^\n'
        '\n'
        '+ "cut"\n'
        '?   ^\n'
    ),
    equal_strings=DiffCase("same", "same", ""),
    integers=DiffCase(
        123,
        132,
        "- 123\n"
        "+ 132"
    ),
    equal_integers=DiffCase(42, 42, ""),
    floats=DiffCase(
        1.23,
        13.2,
        "- 1.23\n"
        "?    -\n"
        "\n"
        "+ 13.2\n"
        "?  +\n"
    ),
    equal_floats=DiffCase(3.140, 3.14e0, ""),
    float_and_integer=DiffCase(1.0, 1, ""),
    lists=DiffCase(
        [1, 2, 3],
        [1, 3, 4],
        "  [\n"
        "    1,\n"
        "-   2,\n"
        "-   3\n"
        "+   3,\n"
        "?    +\n"
        "\n"
        "+   4\n"
        "  ]"),
    equal_lists=DiffCase([2, 3], [2, 3], ""),
    tuples=DiffCase(
        (1, 2, 3),
        (1, 3, 4),
        "  [\n"
        "    1,\n"
        "-   2,\n"
        "-   3\n"
        "+   3,\n"
        "?    +\n"
        "\n"
        "+   4\n"
        "  ]"
    ),
    equal_tuples=DiffCase((2, 3), (2, 3), ""),
    list_and_tuples=DiffCase((3, 4), [3, 4], ""),
    objects=DiffCase(
        {"a": 1, "b": 2},
        {"b": 2, "c": 3},
        '  {\n'
        '-   "a": 1,\n'
        '-   "b": 2\n'
        '+   "b": 2,\n'
        '?         +\n'
        '\n'
        '+   "c": 3\n'
        '  }'
    ),
    flat_dict=DiffCase(
        {"a": {"b": "c"}},
        {"a": {"c": "d"}},
        '  {\n'
        '-   "a.b": "c"\n'
        '?      ^    ^\n'
        '\n'
        '+   "a.c": "d"\n'
        '?      ^    ^\n'
        '\n'
        '  }',
        flags=pretty.Flag.FLAT_DICT,
    ),
    dump_equals=DiffCase(
        {"a": 1, "b": 2},
        {"a": 1, "b": 2},
        '  {\n'
        '    "a": 1,\n'
        '    "b": 2\n'
        '  }',
        flags=pretty.Flag.DUMP_EQUALS,
    ),
    # fmt: on
)
def test_diff(case: DiffCase):
    params: dict[str, Any] = {}
    if case.flags is not None:
        params["flags"] = case.flags
    got = pretty.diff(case.old, case.new, **params)
    assert got == case.want


@dataclass()
class SecondsCase:
    value: float | int
    want: str


@cases.cases(
    zero=SecondsCase(0, "0s"),
    milliseconds=SecondsCase(3.1465, "3.146s"),
    integers=SecondsCase(42, "42s"),
    float=SecondsCase(42.0, "42s"),
    minute=SecondsCase(60, "1m0s"),
    hundred=SecondsCase(1e2, "1m40s"),
    thausands=SecondsCase(1e3, "16m40s"),
    hour=SecondsCase(3600, "1h0m0s"),
    day=SecondsCase(86400, "1d0h0m0s"),
    milions=SecondsCase(1e6, "11d13h46m40s"),
)
def test_seconds(case: SecondsCase):
    got = pretty.seconds(case.value)
    assert got == case.want


@dataclass()
class SizeCase:
    value: float | int | None
    want: str


@cases.cases(
    none=SizeCase(None, "?"),
    zero=SizeCase(0, "0B"),
    integers=SizeCase(42, "42B"),
    float=SizeCase(42.0, "42B"),
    hundred=SizeCase(100, "0.10KB"),
    kilos=SizeCase(1024, "1.00KB"),
    hundred_kilos=SizeCase(100 * 1024, "0.10MB"),
    megas=SizeCase(1024 * 1024, "1.00MB"),
    hundred_megas=SizeCase(100 * 1024 * 1024, "0.10GB"),
    gigas=SizeCase(1024 * 1024 * 1024, "1.00GB"),
    hundred_gigas=SizeCase(100 * 1024 * 1024 * 1024, "0.10TB"),
    teras=SizeCase(1024 * 1024 * 1024 * 1024, "1.00TB"),
)
def test_size(case: SizeCase):
    got = pretty.size(case.value)
    assert got == case.want
