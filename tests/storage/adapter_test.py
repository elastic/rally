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

from abc import ABC
from dataclasses import dataclass
from unittest import mock

import pytest

from esrally.config import Config, Scope
from esrally.storage._adapter import Adapter, AdapterRegistry
from esrally.utils.cases import cases


class MockAdapter(Adapter, ABC):

    @classmethod
    def from_config(cls, cfg: Config) -> Adapter:
        return mock.create_autospec(cls, spec_set=True, instance=True)


class HTTPAdapter(MockAdapter, ABC):

    __adapter_prefixes__ = ("http://",)


class HTTPSAdapter(MockAdapter, ABC):

    __adapter_prefixes__ = ("https://",)


@pytest.fixture()
def cfg() -> Config:
    cfg = Config()
    cfg.add(Scope.application, "storage", "storage.adapters", f"{__name__}:HTTPAdapter,{__name__}:HTTPSAdapter")
    return cfg


# Initialize default registry
@pytest.fixture()
def registry(cfg: Config) -> AdapterRegistry:
    return AdapterRegistry.from_config(cfg)


@dataclass()
class RegistryCase:
    url: str
    want: type[Adapter] | type[Exception]


@cases(
    ftp=RegistryCase("ftp://example.com", ValueError),
    http=RegistryCase("http://example.com", HTTPAdapter),
    https=RegistryCase("https://example.com", HTTPSAdapter),
)
def test_adapter_registry_get(case: RegistryCase, registry: AdapterRegistry) -> None:
    try:
        got = registry.get(case.url)
    except Exception as ex:
        got = ex
    assert isinstance(got, case.want)
    if isinstance(case.want, Adapter):
        assert got is registry.get(case.url)
