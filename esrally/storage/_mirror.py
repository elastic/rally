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

import dataclasses
import json
import os
from collections.abc import Iterable, Iterator, Set
from typing import Any

from typing_extensions import Self

from esrally.types import Config


@dataclasses.dataclass
class MirrorEntry:
    source: str
    destination: str
    labels: Set[tuple[str, Any]]


@dataclasses.dataclass
class MirrorFileEntry:
    sources: Set[str]
    destinations: Set[str]
    labels: Set[tuple[str, Any]]


class MirrorList:

    @classmethod
    def from_config(cls, cfg: Config) -> Self:
        files: list[str] = [
            os.path.expanduser(f)
            for f in (
                os.environ.get("ESRALLY_MIRROR_FILES")
                or cfg.opts(section="storage", key="storage.mirror_files", default_value="", mandatory=False)
            ).split(",")
        ]
        labels: set[tuple[str, str]] = _extract_key_value_set(
            json.loads(
                os.environ.get("ESRALLY_MIRROR_LABELS")
                or cfg.opts(section="storage", key="storage.mirror_labels", default_value="[]", mandatory=False)
            )
        )
        return cls(files=files, labels=labels)

    def __init__(
        self,
        mirrors: Iterable[MirrorEntry] | None = None,
        files: Iterable[str] | None = None,
        labels: Iterable[tuple[str, Any]] | None = None,
    ):
        self.labels = frozenset(labels or [])
        self.mirrors: dict[tuple[str, str], MirrorEntry] = {}
        if mirrors is not None:
            self._update(mirrors)
        if files is not None:
            for path in files:
                if os.path.isfile(path):
                    self._update(load_mirror_file(path))

    def _update(self, mirrors: Iterable[MirrorFileEntry | MirrorEntry]) -> None:
        todo = list(mirrors)
        while todo:
            m = todo.pop()
            if isinstance(m, MirrorFileEntry):
                if self.labels and len(self.labels - m.labels):
                    continue
                sources = {_normalize_base_url(s) for s in m.sources}
                destinations = {_normalize_base_url(s) for s in m.destinations}
                for s in sources:
                    for d in destinations:
                        todo.append(MirrorEntry(s, d, m.labels))
                continue

            if isinstance(m, MirrorEntry):
                if self.labels and len(self.labels - m.labels):
                    continue
                source = _normalize_base_url(m.source)
                destination = _normalize_base_url(m.destination)
                key = (source, destination)
                entry = self.mirrors.get(key)
                if entry is None:
                    self.mirrors[key] = MirrorEntry(source, destination, frozenset(m.labels))
                else:
                    entry.labels = frozenset(entry.labels | m.labels)
                continue

            raise TypeError(f"Unsupported entry type {type(m)}")

    def resolve(self, url: str, labels: Iterable[tuple[str, str]] = tuple()) -> Iterator[MirrorEntry]:
        _labels = frozenset(labels or [])
        # There can't be URLs duplications in resulting output because there can't be repeated (source, destination) pairs.
        for m in self.mirrors.values():
            if not url.startswith(m.source):
                continue

            if _labels and _labels - m.labels:
                continue

            path = _normalize_path(url[len(m.source) :])
            yield MirrorEntry(url, m.destination + path, m.labels)


def load_mirror_file(path: str) -> Iterator[MirrorFileEntry]:
    with open(path) as file:
        document = json.load(file)
    try:
        return _extract_mirrors(document)
    except ValueError as ex:
        raise ValueError(f"invalid mirrors file {path}: {ex}")


def _extract_mirrors(document: Any) -> Iterator[MirrorFileEntry]:
    if not isinstance(document, dict):
        raise ValueError(f"expected a dict but got {type(document)}")
    try:
        mirrors = _extract_object_list(document.get("mirrors", []))
    except ValueError as ex:
        raise ValueError(f"invalid 'mirrors' value: {ex}")

    for mirror in mirrors:
        try:
            sources = _extract_string_set(mirror.get("sources", []))
        except ValueError as ex:
            raise ValueError(f"invalid 'sources' value: {ex}")
        if not sources:
            raise ValueError("'sources' list is empty")

        try:
            destinations = _extract_string_set(mirror.get("destinations", []))
        except ValueError as ex:
            raise ValueError(f"invalid 'destinations' value: {ex}")
        if not destinations:
            raise ValueError("'destinations' list is empty")

        try:
            labels = _extract_key_value_set(mirror.get("labels", []))
        except ValueError as ex:
            raise ValueError(f"invalid 'labels' value: {ex}")

        yield MirrorFileEntry(sources, destinations, labels)


def _extract_object_list(objects: Any) -> list[dict[str, Any]]:
    if isinstance(objects, dict):
        objects = [objects]
    if not isinstance(objects, list) or not all(isinstance(o, dict) and all(isinstance(k, str) for k in o) for o in objects):
        raise ValueError(f"expected dict or list of dicts with string keys, got {objects}")
    return objects


def _extract_string_set(values: list[str]) -> set[str]:
    if not isinstance(values, list) or not values or not all(isinstance(s, str) for s in values):
        raise ValueError(f"expected list of strings, got {values}")
    return set(values)


def _extract_key_value_set(objects: Any) -> set[tuple[str, Any]]:
    objects = _extract_object_list(objects)
    ret: set[tuple[str, Any]] = set()
    for o in objects:
        for k, v in o.items():
            ret.add((k, v))
    return ret


def _normalize_base_url(url: str) -> str:
    return url.rstrip("/") + "/"


def _normalize_path(path: str) -> str:
    return path.lstrip("/")
