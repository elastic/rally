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

from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping

import yaml

MAX_CONNECTIONS = 4


class Mirror:

    def __init__(self, urls: Mapping[str, Iterable[str]]):
        self._urls = {_normalize_url(src): [_normalize_url(dst) for dst in v] for src, v in urls.items()}

    @classmethod
    def from_file(cls, filename: str) -> Mirror:
        urls: dict[str, set[str]] = defaultdict(set)
        with open(filename) as file:
            document = yaml.safe_load(file)
        for mirror in document.get("mirrors", []):
            if not isinstance(mirror, Mapping):
                raise ValueError(f"invalid mirrors value: got {document.get('mirrors')}, want list of objects")
            sources = mirror.get("sources", None)
            if not isinstance(sources, list) or not sources:
                raise ValueError(f"invalid source value: got {sources}, want non empty list of urls")
            destinations = mirror.get("destinations", None)
            if not isinstance(destinations, list) or not destinations:
                raise ValueError(f"invalid destinations value: got {destinations}, want non empty list of urls")
            for s in sources:
                urls[s].update(destinations)
        return Mirror(urls=urls)

    def urls(self, url: str) -> Iterator[str]:
        for base_url, mirror_urls in self._urls.items():
            if not url.startswith(base_url):
                continue
            path = url[len(base_url) :]
            if not path:
                continue
            for u in mirror_urls:
                yield u + path


def _normalize_url(url: str) -> str:
    return url.rstrip("/") + "/"
