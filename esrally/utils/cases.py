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

from typing import Callable, Protocol

import pytest


class Case(Protocol):
    """Case represent the interface expected an instance passed to cases decorator is expected to implement."""

    # case_id specifies the ID of the test case.
    case_id: str


def cases(*cases: Case) -> Callable:
    """cases defines a decorator wrapping pytest.mark.parametrize to run a test with multiple cases.

    Example of use:

        @dataclass
        class SumCase:
            id: str
            values: list[int]
            want: int

        @cases(
            MyCase("no_values", want=0)
            MyCase("2 values", values=[1,2] want=3),
        )
        def test_sum(case: MyCase):
            assert sum(*case.values) == case.want

    :param cases: sequence of cases
    :return: test method decorator
    """
    return pytest.mark.parametrize(argnames="case", argvalues=cases, ids=lambda case: case.case_id)
