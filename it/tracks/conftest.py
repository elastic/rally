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
minutes × 60 / ``N``, times the pytest-xdist worker count when ``-n`` is used.
Here ``N`` is the count of collected ``test_race_with_track`` nodes after ``-k``
and track-name deselection, **excluding** nodes that would ``pytest.skip`` for
``skip_reason_by_es_version`` unless ``--it-tracks-no-skip`` / ``IT_TRACKS_NO_SKIP``
is set (see ``pytest_collection_finish``).

Under pytest-xdist, each worker sets ``COMPOSE_PROJECT_NAME`` from its worker id
so Docker Compose stacks do not clash, and race tests are marked with
``xdist_group`` per Elasticsearch version. ``pytest.ini`` defaults to ``-n auto`` and ``--dist loadgroup``; the
``pytest_xdist_auto_num_workers`` hook maps ``auto`` to the number of configured
ES versions (see README).
"""

from __future__ import annotations

import fnmatch
import os
from pathlib import Path

import pytest

from esrally.utils import compose
from it.tracks.helpers import (
    it_tracks_es_version_worker_count,
    it_tracks_no_skip_from_config,
    it_tracks_xdist_num_workers,
    race_item_counts_toward_timeout_budget,
    resolve_es_versions,
    resolve_track_name_patterns,
    total_timeout_minutes,
)

_TRACKS_DIR = Path(__file__).resolve().parent
os.environ.setdefault("RALLY_COMPOSE_FILE", str(_TRACKS_DIR / "compose.yaml"))

_IT_TRACKS_ES_VERSIONS_KEY = pytest.StashKey[list[str]]()
_IT_TRACKS_RACE_TIMEOUT_S_KEY = pytest.StashKey[float]()
_IT_TRACKS_RACE_TIMEOUT_BASE_S_KEY = pytest.StashKey[float]()


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


@pytest.hookimpl(tryfirst=True)
def pytest_xdist_auto_num_workers(config: pytest.Config) -> int:
    """Spawn one pytest-xdist worker per configured Elasticsearch version for ``-n auto``."""
    return it_tracks_es_version_worker_count(config)


def _compose_project_suffix_for_worker(worker_id: str) -> str:
    safe = "".join(c if c.isalnum() else "_" for c in worker_id)
    return safe or "worker"


def pytest_configure(config: pytest.Config) -> None:
    """Resolve ES version list once and stash for ``pytest_generate_tests``.

    On pytest-xdist workers, set ``COMPOSE_PROJECT_NAME`` so each worker uses an
    isolated Compose stack (containers, volumes, networks).
    """
    cli_es = config.getoption("--it-tracks-es-versions")
    config.stash[_IT_TRACKS_ES_VERSIONS_KEY] = resolve_es_versions(cli_es)

    workerinput = getattr(config, "workerinput", None)
    if isinstance(workerinput, dict):
        wid = workerinput.get("workerid", "gw0")
        os.environ["COMPOSE_PROJECT_NAME"] = f"rally_it_tracks_{_compose_project_suffix_for_worker(str(wid))}"


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
        ids=lambda v: f"es_{v}",
    )


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(session: pytest.Session, config: pytest.Config, items: list[pytest.Item]) -> None:
    """Deselect ``test_race_with_track`` nodes by track name; add xdist groups per ES version.

    ``tryfirst=True`` ensures ``xdist_group`` marks exist before pytest-xdist's worker
    ``pytest_collection_modifyitems`` (see ``xdist.remote.WorkerInteractor``): that hook
    appends ``@<group>`` to ``item._nodeid`` when ``--dist loadgroup``. If this hook ran
    later, nodeids would stay ungrouped and scheduling would load-balance individual tests,
    mixing Elasticsearch versions across workers.
    """
    cli = config.getoption("--it-tracks-name", default=None)
    patterns = resolve_track_name_patterns(cli, os.environ.get("IT_TRACKS_NAME"))
    if patterns:
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

    xdist_group = getattr(pytest.mark, "xdist_group", None)
    if xdist_group is not None:
        for item in items:
            if "test_race_with_track" not in item.nodeid:
                continue
            try:
                es_version = item.callspec.params["elasticsearch"]
            except (AttributeError, KeyError, ValueError):
                continue
            group_id = "".join(c if c.isalnum() or c in "._-" else "_" for c in str(es_version))
            item.add_marker(xdist_group(name=f"es_{group_id}"))


def pytest_collection_finish(session: pytest.Session) -> None:
    """Compute per-race timeout from global ``N``, then scale by xdist worker count.

    ``N`` counts collected ``test_race_with_track`` items after other deselection, but
    omits items that would skip for ``TrackCase.skip_reason_by_es_version`` at the
    active ES version when no-skip mode is off (same rule as ``race_test``). If
    ``callspec`` cannot be read, the item still counts (conservative budget).

    Stashes ``race_timeout_s_base`` for the controller to send to workers via
    ``pytest_configure_node``. Workers prefer ``workerinput['it_tracks_race_timeout_s']``
    when set so the timeout matches the controller's global ``N``.
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
    race_timeout_s_base = (tmin * 60) / max(1, n)
    session.config.stash[_IT_TRACKS_RACE_TIMEOUT_BASE_S_KEY] = race_timeout_s_base

    workerinput = getattr(session.config, "workerinput", None)
    if isinstance(workerinput, dict) and "it_tracks_race_timeout_s" in workerinput:
        session.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY] = float(workerinput["it_tracks_race_timeout_s"])
    else:
        if isinstance(workerinput, dict) and "workercount" in workerinput:
            nw = max(1, int(workerinput["workercount"]))
        else:
            nw = it_tracks_xdist_num_workers(session.config)
        session.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY] = race_timeout_s_base * nw


def pytest_configure_node(node: object) -> None:
    """pytest-xdist: push scaled race timeout to workers (global ``N`` × worker count)."""
    cfg = getattr(node, "config", None)
    if cfg is None:
        return
    try:
        race_timeout_s_base = float(cfg.stash[_IT_TRACKS_RACE_TIMEOUT_BASE_S_KEY])
    except (KeyError, TypeError, ValueError):
        return
    wi = getattr(node, "workerinput", None)
    if not isinstance(wi, dict):
        return
    nw = max(1, int(wi.get("workercount", it_tracks_xdist_num_workers(cfg))))
    wi["it_tracks_race_timeout_s"] = race_timeout_s_base * nw


def pytest_report_header() -> str:
    return "it/tracks: isolated from it/conftest.py (via pytest.ini confcutdir)"


def pytest_sessionstart(session: pytest.Session) -> None:
    """pytest-xdist controller: build the shared Rally image once before workers start.

    Workers still run the module ``build_rally`` fixture (cheap when the image exists). The
    stable ``image:`` in ``compose.yaml`` ensures all workers use the same tag regardless of
    ``COMPOSE_PROJECT_NAME``.
    """
    wi = getattr(session.config, "workerinput", None)
    opt = getattr(session.config, "option", None)
    num = getattr(opt, "numprocesses", None) if opt else None
    if isinstance(wi, dict):
        return
    if num in (None, 0, False):
        return
    compose.build_image("rally", cfg=compose.ComposeConfig())


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Tear down the Compose project when the session ends (including ``KeyboardInterrupt`` / Ctrl+C).

    Per-test fixture teardown can be skipped if the process exits before unwinding generators; this hook
    runs on normal exit and on the first interrupt that ends the session. Uses a fresh
    :class:`~esrally.utils.compose.ComposeConfig` so it works even if ``init_config`` was never set.

    Under pytest-xdist, only **worker** processes set ``COMPOSE_PROJECT_NAME`` (see ``pytest_configure``);
    the controller must not run ``docker compose down`` or it would target the wrong project name while
    workers still hold containers.
    """
    wi = getattr(session.config, "workerinput", None)
    opt = getattr(session.config, "option", None)
    num = getattr(opt, "numprocesses", None) if opt else None
    if isinstance(wi, dict):
        compose.teardown_project(cfg=compose.ComposeConfig())
        return
    if num not in (None, 0, False):
        return
    compose.teardown_project(cfg=compose.ComposeConfig())


@pytest.fixture(scope="session")
def race_timeout_s(request: pytest.FixtureRequest) -> float:
    """Per ``test_race_with_track`` subprocess timeout (seconds), from stash after collection."""
    return request.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY]
