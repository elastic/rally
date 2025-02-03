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

from typing import Callable, TypeVar

import pytest

C = TypeVar("C")


def cases(arg_name: str = "case", **table: C) -> Callable:
    """cases defines a decorator wrapping `pytest.mark.parametrize` to run a test against multiple cases.

    The purpose of this decorator is to create table-driven unit tests (https://go.dev/wiki/TableDrivenTests).

    Example of use:

        @dataclass
        class SumCase:
            values: list[int]
            want: int


        @cases(
            no_values=SumCase(values=[], want=0),
            two_values=SumCase(values=[1, 2], want=3),
        )
        def test_sum(case: SumCase):
            assert sum(case.values) == case.want

    :param arg_name: the name of the parameter used for the input table (by default is 'case').
    :param table:
        a dictionary of per use case entries that represent the test input table. It typically contains
        either input parameters and configuration for initial test case status (or fixtures) and to specify desired
        outcome (prefixed with `want_`).
    :return: a test method decorator.
    """
    return pytest.mark.parametrize(argnames=arg_name, argvalues=list(table.values()), ids=list(table.keys()))
