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

import os
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping

import yaml

from esrally.config import Config

MAX_CONNECTIONS = 4
MIRRORS_FILES = ""


class MirrorList:

    @classmethod
    def from_config(cls, cfg: Config) -> MirrorList:
        mirror_files = []
        for filename in cfg.opts(section="storage", key="storage.mirrors_files", default_value=MIRRORS_FILES, mandatory=False).split(","):
            filename = filename.strip()
            if filename:
                mirror_files.append(filename)
        return cls(mirror_files=mirror_files)

    def __init__(
        self,
        urls: Mapping[str, Iterable[str]] | None = None,
        mirror_files: Iterable[str] = tuple(),
    ):
        self._urls: dict[str, set] = defaultdict(set)
        self._mirror_files = tuple(mirror_files or [])
        for path in self._mirror_files:
            self._update(_load_file(path))
        if urls is not None:
            self._update(urls)

    def _update(self, urls: Mapping[str, Iterable[str]]) -> None:
        for src, dsts in urls.items():
            self._urls[_normalize_url(src)].update(_normalize_url(dst) for dst in dsts)

    def urls(self, url: str) -> Collection[str]:
        ret: set[str] = set()
        for base_url, mirror_urls in self._urls.items():
            if not url.startswith(base_url):
                continue
            path = url[len(base_url) :]
            if not path:
                continue
            for u in mirror_urls:
                ret.add(u + path)
        return ret


def _load_file(path: str) -> Mapping[str, Iterable[str]]:
    ret: dict[str, set[str]] = defaultdict(set)
    with open(os.path.expanduser(path)) as file:
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
            ret[s].update(destinations)
    return ret


def _normalize_url(url: str) -> str:
    return url.rstrip("/") + "/"
