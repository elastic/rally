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

import dataclasses
import os
import sys

import pytest

import esrally
from esrally import version
from esrally.utils import cases

MIN_ES_VERSION_PATH = os.path.join(os.path.dirname(version.__file__), "min-es-version.txt")


def test_minimum_es_version():
    assert os.path.exists(MIN_ES_VERSION_PATH)
    got = version.minimum_es_version()
    with open(MIN_ES_VERSION_PATH) as f:
        want = f.read().strip()
    assert want == got


@dataclasses.dataclass
class VersionCase:
    version: esrally.Version
    want_error: Exception | None = None


@cases.cases(
    v309=VersionCase(esrally.Version(3, 9), want_error=RuntimeError(f"Expecting Python version >= {esrally.MIN_PYTHON_VERSION}, got 3.9")),
    v310=VersionCase(esrally.Version(3, 10)),
    v311=VersionCase(esrally.Version(3, 11)),
    v321=VersionCase(esrally.Version(3, 12)),
    v313=VersionCase(esrally.Version(3, 13)),
    v314=VersionCase(esrally.Version(3, 14)),
)
def test_check_version(case: VersionCase, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "version_info", case.version)
    monkeypatch.setattr(sys, "version", str(case.version))
    if case.want_error is None:
        esrally.check_python_version()
        return

    want_error_types = (type(case.want_error),)
    with pytest.raises(want_error_types, match=str(case.want_error)):
        esrally.check_python_version()
