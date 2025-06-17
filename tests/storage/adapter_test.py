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

    __adapter_URL_prefixes__ = ("http://",)


class HTTPSAdapter(MockAdapter, ABC):

    __adapter_URL_prefixes__ = ("https://",)


class ExampleAdapter(MockAdapter, ABC):
    __adapter_URL_prefixes__ = ("https://example.com/",)


class ExampleAdapterWithPath(MockAdapter, ABC):

    __adapter_URL_prefixes__ = ("https://example.com/some/path/",)


@pytest.fixture()
def cfg() -> Config:
    cfg = Config()
    cfg.add(
        Scope.application,
        "storage",
        "storage.adapters",
        f"{__name__}:HTTPSAdapter,{__name__}:HTTPAdapter,{__name__}:ExampleAdapter,{__name__}:ExampleAdapterWithPath",
    )
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
    example=RegistryCase("https://example.com/", ExampleAdapter),
    example_with_path=RegistryCase("https://example.com/some/path/", ExampleAdapterWithPath),
)
def test_adapter_registry_get(case: RegistryCase, registry: AdapterRegistry) -> None:
    try:
        got = registry.get(case.url)
    except Exception as ex:
        got = ex
    assert isinstance(got, case.want)
    if isinstance(case.want, Adapter):
        assert got is registry.get(case.url)


def test_adapter_registry_order(registry: AdapterRegistry) -> None:
    registered_prefixes = [c.url_prefix for c in registry._classes]  # pylint: disable=protected-access
    assert registered_prefixes == [
        "https://example.com/some/path/",
        "https://example.com/",
        "https://",
        "http://",
    ], "prefixes are not sorted from the longest to the shortest"
