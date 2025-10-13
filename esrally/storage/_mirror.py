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
import json
import logging
import os
from collections import defaultdict
from collections.abc import Iterable, Mapping

from typing_extensions import Self

from esrally import types
from esrally.storage._config import StorageConfig
from esrally.utils import convert

LOG = logging.getLogger(__name__)


class MirrorList:

    @classmethod
    def from_config(cls, cfg: types.Config | StorageConfig | None = None) -> Self:
        return cls(cfg=StorageConfig.from_config(cfg))

    def __init__(
        self,
        cfg: StorageConfig | None = None,
        mirror_files: Iterable[str] | None = None,
        urls: Mapping[str, Iterable[str]] | None = None,
    ):
        self._cfg = cfg
        self._mirror_files: tuple[str, ...] = tuple()
        self._urls: dict[str, set] = defaultdict(set)
        mirror_files = set(convert.to_strings(mirror_files))
        if cfg is not None:
            mirror_files.update(cfg.mirror_files)
        for path in mirror_files:
            try:
                self._update(_load_file(path))
            except FileNotFoundError as ex:
                LOG.warning("failed loading mirror file '%s': %s", path, ex)
        if urls is not None:
            self._update(urls)

    def _update(self, urls: Mapping[str, Iterable[str]]) -> None:
        for src, dsts in urls.items():
            self._urls[_normalize_base_url(src)].update(_normalize_base_url(dst) for dst in dsts)

    def resolve(self, url: str) -> list[str]:
        # There couldn't be URLs duplication in resulting output because there can't be repeated entries.
        for base_url, mirror_urls in self._urls.items():
            if url.startswith(base_url):
                ret = set()
                path = _normalize_path(url[len(base_url) :])
                for u in mirror_urls:
                    ret.add(u + path)
                return list(ret)
        raise ValueError(f"No mirror url found for URL '{url}'")


def _load_file(path: str) -> Mapping[str, Iterable[str]]:
    ret: dict[str, set[str]] = defaultdict(set)
    with open(os.path.expanduser(path)) as file:
        document = json.load(file)
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


def _normalize_base_url(url: str) -> str:
    return url.rstrip("/") + "/"


def _normalize_path(path: str) -> str:
    return path.lstrip("/")
