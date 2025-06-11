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

from collections.abc import Iterable
from dataclasses import dataclass

from esrally import config
from esrally.storage import Adapter, Client, Head
from esrally.utils import cases


def test_init():
    client = Client()
    assert isinstance(client, Client)


def test_from_config():
    client = Client.from_config(config.Config())
    assert isinstance(client, Client)


def test_head():
    client = Client()
    got = client.head("https://example.com", ttl=10)
    assert isinstance(got, Head)
    assert got is client.head("https://example.com", ttl=10)
