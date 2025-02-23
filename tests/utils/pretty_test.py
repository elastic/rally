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

from dataclasses import dataclass

from esrally.utils import cases, pretty


@dataclass
class DumpCase:
    o: pretty.O
    want: str
    flat_dict: bool = False


@cases.cases(
    null=DumpCase(None, "null"),
    string=DumpCase("string", '"string"'),
    integer=DumpCase(1, "1"),
    float=DumpCase(1.0, "1.0"),
    array=DumpCase([1, 2, 3], "[\n  1,\n  2,\n  3\n]"),
    object=DumpCase({"a": "a", "b": 2}, '{\n  "a": "a",\n  "b": 2\n}'),
    flat_dict=DumpCase({"a": {"b": "c"}}, '{\n  "a.b": "c"\n}', flat_dict=True),
)
def test_dump(case: DumpCase):
    got = pretty.dump(case.o, flat_dict=case.flat_dict)
    assert got == case.want


@dataclass
class DiffCase:
    old: pretty.O
    new: pretty.O
    want: str
    flat_dict: bool = False


@cases.cases(
    null=DiffCase(old=None, new="something", want='- null\n+ "something"'),
    string=DiffCase(old="cat", new="cut", want='- "cat"\n?   ^\n\n+ "cut"\n?   ^\n'),
    integer=DiffCase(old=123, new=132, want="- 123\n+ 132"),
    float=DiffCase(old=1.23, new=13.2, want="- 1.23\n?    -\n\n+ 13.2\n?  +\n"),
    array=DiffCase(old=[1, 2, 3], new=[1, 3, 4], want="  [\n    1,\n-   2,\n-   3\n+   3,\n?    +\n\n+   4\n  ]"),
    object=DiffCase(
        old={"a": 1, "b": 2},
        new={"b": 2, "c": 3},
        want='  {\n-   "a": 1,\n-   "b": 2\n+   "b": 2,\n?         +\n\n+   "c": 3\n  }',
    ),
    flat_dict=DiffCase(
        old={"a": {"b": "c"}},
        new={"a": {"c": "d"}},
        want='  {\n-   "a.b": "c"\n?      ^    ^\n\n+   "a.c": "d"\n?      ^    ^\n\n  }',
        flat_dict=True,
    ),
)
def test_pretty(case: DiffCase):
    got = pretty.diff(case.old, case.new, flat_dict=case.flat_dict)
    assert got == case.want
