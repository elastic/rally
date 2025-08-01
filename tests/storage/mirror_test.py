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

import json
import os.path
from collections.abc import Sequence
from dataclasses import dataclass

from esrally.config import Config, Scope
from esrally.storage._mirror import MirrorEntry, MirrorList
from esrally.utils.cases import cases

BASE_URL = "https://rally-tracks.elastic.co"
SOME_PATH = "some/file.json.bz2"
SOME_URL = f"{BASE_URL}/{SOME_PATH}"

EU_CENTRAL_URL = "https://rally-tracks-eu-central-1.s3.eu-central-1.amazonaws.com"
EU_CENTRAL_LABELS = {"region": "eu-central"}

US_WEST_URL = "https://rally-tracks-us-west-1.s3.us-west-1.amazonaws.com"
US_WEST_LABELS = {"region": "us-west"}

MIRRORS = [
    MirrorEntry(BASE_URL, EU_CENTRAL_URL, set(EU_CENTRAL_LABELS.items())),
    MirrorEntry(BASE_URL, US_WEST_URL, set(US_WEST_LABELS.items())),
]

MIRROR_FILES = "mirrors.json"
INVALID_MIRROR_FILES = "invalid-mirrors.json"
MIRROR_FILE_CONTENT = {
    "mirrors": [
        {"sources": [BASE_URL], "destinations": [EU_CENTRAL_URL], "labels": EU_CENTRAL_LABELS},
        {"sources": [BASE_URL], "destinations": [US_WEST_URL], "labels": US_WEST_LABELS},
    ]
}


@dataclass()
class FromConfigCase:
    mirror_files: str | None = None
    mirror_labels: str | None = None
    want_error: type[Exception] | tuple = tuple()
    want_mirrors: Sequence[MirrorEntry] = tuple()
    want_labels: Sequence[tuple[str, str]] = tuple()


@cases(
    default=FromConfigCase(want_mirrors=[]),
    mirror_files=FromConfigCase(
        mirror_files=MIRROR_FILES,
        want_mirrors=[
            MirrorEntry(source=f"{BASE_URL}/", destination=f"{US_WEST_URL}/", labels=set(US_WEST_LABELS.items())),
            MirrorEntry(source=f"{BASE_URL}/", destination=f"{EU_CENTRAL_URL}/", labels=set(EU_CENTRAL_LABELS.items())),
        ],
    ),
    invalid_mirror_files=FromConfigCase(INVALID_MIRROR_FILES, want_error=FileNotFoundError),
    eu_central_labels=FromConfigCase(
        mirror_files=MIRROR_FILES,
        mirror_labels=json.dumps(EU_CENTRAL_LABELS),
        want_mirrors=[
            MirrorEntry(source=f"{BASE_URL}/", destination=f"{EU_CENTRAL_URL}/", labels=set(EU_CENTRAL_LABELS.items())),
        ],
        want_labels=list(EU_CENTRAL_LABELS.items()),
    ),
    us_west_labels=FromConfigCase(
        mirror_files=MIRROR_FILES,
        mirror_labels=json.dumps(US_WEST_LABELS),
        want_mirrors=[
            MirrorEntry(source=f"{BASE_URL}/", destination=f"{US_WEST_URL}/", labels=set(US_WEST_LABELS.items())),
        ],
        want_labels=list(US_WEST_LABELS.items()),
    ),
)
def test_from_config(case: FromConfigCase, tmpdir: os.PathLike) -> None:
    cfg = Config()
    if case.mirror_files is not None:
        with open(os.path.join(tmpdir, MIRROR_FILES), "w") as fd:
            json.dump(MIRROR_FILE_CONTENT, fd)
        cfg.add(Scope.application, "storage", "storage.mirror_files", os.path.join(tmpdir, case.mirror_files))
    if case.mirror_labels is not None:
        cfg.add(Scope.application, "storage", "storage.mirror_labels", case.mirror_labels)
    try:
        mirrors = MirrorList.from_config(cfg)
    except case.want_error:
        return

    assert set(case.want_labels) == mirrors.labels

    got_mirrors = sorted(mirrors.mirrors.values(), key=lambda m: (m.source, m.destination))
    want_mirrors = sorted(case.want_mirrors, key=lambda m: (m.source, m.destination))
    assert got_mirrors == want_mirrors


@dataclass()
class ResolveCase:
    url: str
    want_destinations: list[str] | None = None
    want_error: type[Exception] | tuple = tuple()


@cases(
    empty=ResolveCase("", want_error=Exception),
    simple=ResolveCase(SOME_URL, want_destinations=[f"{EU_CENTRAL_URL}/{SOME_PATH}", f"{US_WEST_URL}/{SOME_PATH}"]),
)
def test_resolve(case: ResolveCase):
    mirrors = MirrorList(mirrors=MIRRORS)
    try:
        got = list(mirrors.resolve(case.url))
    except case.want_error:
        return
    for m in got:
        assert isinstance(m, MirrorEntry)
        assert m.source == case.url
    if case.want_destinations is not None:
        got_destinations = sorted(e.destination for e in got)
        want_destinations = sorted(case.want_destinations)
        assert got_destinations == want_destinations
