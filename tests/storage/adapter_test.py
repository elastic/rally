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
from typing import Any
from unittest import mock

import pytest

from esrally import types
from esrally.config import Config, Scope
from esrally.storage._adapter import Adapter, AdapterRegistry
from esrally.utils.cases import cases


class MockAdapter(Adapter, ABC):

    @classmethod
    def from_config(cls: type[Adapter], cfg: types.Config, **kwargs: dict[str, Any]) -> MockAdapter:
        return mock.create_autospec(cls, spec_set=True, instance=True)


class HTTPAdapter(MockAdapter, ABC):

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("http://")


class HTTPSAdapter(MockAdapter, ABC):

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("https://")


class ExampleAdapter(MockAdapter, ABC):

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("https://example.com/")


class ExampleAdapterWithPath(MockAdapter, ABC):

    @classmethod
    def match_url(cls, url: str) -> bool:
        return url.startswith("https://example.com/some/path/")


@pytest.fixture()
def cfg() -> Config:
    cfg = Config()
    cfg.add(
        Scope.application,
        "storage",
        "storage.adapters",
        f"{__name__}:ExampleAdapterWithPath,{__name__}:ExampleAdapter,{__name__}:HTTPSAdapter,{__name__}:HTTPAdapter",
    )
    return cfg


# Initialize default registry
@pytest.fixture()
def registry(cfg: Config) -> AdapterRegistry:
    return AdapterRegistry.from_config(cfg)


@dataclass()
class RegistryCase:
    url: str
    want_type: type[Adapter] | None = None
    want_error: type[Exception] | None = None


@cases(
    ftp=RegistryCase("ftp://example.com", want_error=ValueError),
    http=RegistryCase("http://example.com", want_type=HTTPAdapter),
    https=RegistryCase("https://example.com", want_type=HTTPSAdapter),
    example=RegistryCase("https://example.com/", want_type=ExampleAdapter),
    example_with_path=RegistryCase("https://example.com/some/path/", ExampleAdapterWithPath),
)
def test_adapter_registry_get(case: RegistryCase, registry: AdapterRegistry) -> None:
    try:
        adapter = registry.get(case.url)
        error = None
    except Exception as ex:
        adapter = None
        error = ex

    if case.want_type is not None:
        assert isinstance(adapter, case.want_type)
        adapter2 = registry.get(case.url)
        assert adapter2 is adapter

    if case.want_error is not None:
        assert isinstance(error, case.want_error)
