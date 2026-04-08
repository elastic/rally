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

"""Pytest configuration for ``it/tracks`` only.

``pytest.ini`` in this directory is what keeps ``it/conftest.py`` from loading:
when collection starts under ``it/tracks/``, pytest picks up that inifile and
sets ``confcutdir`` to this folder (see ``_pytest.config.Config._preparse``), so
parent conftest modules are skipped.

This module adds CLI/env controls for track races (ES versions, timeouts, skips,
track-name filter) and stashes ``race_timeout_s`` after collection: total budget
minutes × 60 / ``N``. Here ``N`` is the count of collected ``test_race_with_track``
nodes after ``-k`` and track-name deselection, **excluding** nodes that would
``pytest.skip`` for ``skip_reason_by_es_version`` unless ``--it-tracks-no-skip`` /
``IT_TRACKS_NO_SKIP`` is set (see ``pytest_collection_finish``).
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path

import pytest

from it.tracks.helpers import (
    it_tracks_no_skip_from_config,
    race_item_counts_toward_timeout_budget,
    resolve_es_versions,
    resolve_track_name_patterns,
    total_timeout_minutes,
)

_TRACKS_DIR = Path(__file__).resolve().parent
os.environ.setdefault("RALLY_COMPOSE_FILE", str(_TRACKS_DIR / "compose.yaml"))

_IT_TRACKS_ES_VERSIONS_KEY = pytest.StashKey[list[str]]()
_IT_TRACKS_RACE_TIMEOUT_S_KEY = pytest.StashKey[float]()


def pytest_addoption(parser: pytest.Parser) -> None:
    """Register ``it/tracks``-specific pytest options (see README)."""
    group = parser.getgroup("it_tracks", "it/tracks Docker track race integration tests")
    group.addoption(
        "--it-tracks-no-skip",
        action="store_true",
        default=False,
        help="Run tests even when TrackCase.skip_reason_by_es_version would skip (also IT_TRACKS_NO_SKIP).",
    )
    group.addoption(
        "--it-tracks-es-versions",
        default=None,
        help="Comma-separated Elasticsearch versions for es01 (overrides IT_TRACKS_ES_VERSIONS).",
    )
    group.addoption(
        "--it-tracks-total-timeout-minutes",
        type=int,
        default=None,
        help="Total wall-clock minutes for all selected race tests before dividing by N (overrides IT_TRACKS_TIMEOUT_MINUTES).",
    )
    group.addoption(
        "--it-tracks-name",
        default=None,
        help="Comma-separated fnmatch patterns against TrackCase.track_name (overrides IT_TRACKS_NAME), e.g. geo*,elastic/*",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Resolve ES version list once and stash for ``pytest_generate_tests``."""
    cli_es = config.getoption("--it-tracks-es-versions")
    config.stash[_IT_TRACKS_ES_VERSIONS_KEY] = resolve_es_versions(cli_es)


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize the ``elasticsearch`` fixture indirectly for ``test_race_with_track`` only."""
    if "elasticsearch" not in metafunc.fixturenames:
        return
    if metafunc.definition.name != "test_race_with_track":
        return
    versions = metafunc.config.stash[_IT_TRACKS_ES_VERSIONS_KEY]
    metafunc.parametrize(
        "elasticsearch",
        versions,
        indirect=True,
        ids=lambda v: f"es_version_{v}",
    )


def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]) -> None:
    """Deselect ``test_race_with_track`` nodes whose ``TrackCase.track_name`` matches no pattern."""
    cli = config.getoption("--it-tracks-name", default=None)
    patterns = resolve_track_name_patterns(cli, os.environ.get("IT_TRACKS_NAME"))
    if not patterns:
        return
    kept: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        if "test_race_with_track" not in item.nodeid:
            kept.append(item)
            continue
        try:
            params = item.callspec.params
            case = params["case"]
            name = case.track_name
        except (AttributeError, KeyError, ValueError):
            kept.append(item)
            continue
        if any(fnmatch.fnmatch(name, pat) for pat in patterns):
            kept.append(item)
        else:
            deselected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = kept


def pytest_collection_finish(session: pytest.Session) -> None:
    """Compute per-race timeout seconds from total minutes and divisor ``N``.

    ``N`` counts collected ``test_race_with_track`` items after other deselection, but
    omits items that would skip for ``TrackCase.skip_reason_by_es_version`` at the
    active ES version when no-skip mode is off (same rule as ``race_test``). If
    ``callspec`` cannot be read, the item still counts (conservative budget).
    """
    race_items = [i for i in session.items if "test_race_with_track" in i.nodeid]
    no_skip = it_tracks_no_skip_from_config(session.config)
    n = 0
    for item in race_items:
        try:
            params = item.callspec.params
            case = params["case"]
            es_version = params["elasticsearch"]
        except (AttributeError, KeyError, ValueError):
            n += 1
            continue
        if race_item_counts_toward_timeout_budget(
            no_skip=no_skip,
            skip_reason_by_es_version=case.skip_reason_by_es_version,
            es_version=es_version,
        ):
            n += 1
    cli_t = session.config.getoption("--it-tracks-total-timeout-minutes")
    tmin = total_timeout_minutes(cli_t)
    session.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY] = (tmin * 60) / max(1, n)


def pytest_report_header() -> str:
    return "it/tracks: isolated from it/conftest.py (via pytest.ini confcutdir)"


@pytest.fixture(scope="session")
def race_timeout_s(request: pytest.FixtureRequest) -> float:
    """Per ``test_race_with_track`` subprocess timeout (seconds), from stash after collection."""
    return request.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY]
