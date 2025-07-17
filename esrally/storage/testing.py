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

import copy
from collections.abc import Iterator

from esrally.config import Scope
from esrally.storage._adapter import Adapter, Head, Readable, Writable
from esrally.types import Config


class DummyAdapter(Adapter):

    @classmethod
    def register(cls, cfg: Config) -> None:
        adapters = [n for n0 in cfg.opts("storage", "storage.adapters", mandatory=False, default_value="").split(",") if (n := n0.strip())]
        name = f"{cls.__module__}:{cls.__name__}"
        if name not in adapters:
            adapters.append(name)
            cfg.add(Scope.application, "storage", "storage.adapters", ",".join(adapters))

    DATA: dict[str, bytes] = {}

    def __init__(self, data: dict[str, bytes] | None) -> None:
        if data is None:
            data = copy.deepcopy(self.DATA)
        self.data: dict[str, bytes] = data

    @classmethod
    def match_url(cls, url: str) -> str:
        return url

    def head(self, url: str) -> Head:
        try:
            data = self.data[url]
        except KeyError:
            raise FileNotFoundError from None
        return Head(url, content_length=len(data), accept_ranges=True)

    def list(self, url: str) -> Iterator[Head]:
        raise NotImplementedError()

    def get(self, url: str, stream: Writable, head: Head | None = None) -> Head:
        ret = self.head(url)
        stream.write(self.data[url])
        if head is not None:
            head.check(ret)
        return ret

    def put(self, stream: Readable, url: str, head: Head | None = None) -> Head:
        raise NotImplementedError()
