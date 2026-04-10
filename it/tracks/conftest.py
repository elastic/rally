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

Each ``test_race_with_track`` node sets ``COMPOSE_PROJECT_NAME`` from its pytest
nodeid (Docker-safe) and host logs under ``logs/…`` (see README). While ``es01`` is up, a background
``docker compose logs -f`` process appends merged container output to ``containers.log`` in that directory.
When the worker count does not exceed the
number of configured ES versions, race tests get ``xdist_group`` per version so
``loadgroup`` keeps one version lane per worker; otherwise marks are omitted so
extra workers are not idle. ``pytest.ini`` defaults to ``-n auto`` and ``--dist loadgroup``; the
``pytest_xdist_auto_num_workers`` hook maps ``auto`` to the number of configured
ES versions (see README).
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
from collections.abc import Generator
from pathlib import Path

import pytest

from esrally.utils import compose
from it.tracks import helpers

_TRACKS_DIR = Path(__file__).resolve().parent
os.environ.setdefault("RALLY_COMPOSE_FILE", str(_TRACKS_DIR / "compose.yaml"))
os.environ.setdefault("RALLY_IT_TRACKS_ROOT", "/tracks")


def _ensure_default_it_tracks_host_log_dir() -> None:
    """So ``docker compose build`` / parse sees ``IT_TRACKS_HOST_LOG_DIR`` before the first test."""
    if os.environ.get("IT_TRACKS_HOST_LOG_DIR", "").strip():
        return
    p = helpers.log_root() / "_compose_default"
    p.mkdir(parents=True, exist_ok=True)
    helpers.prepare_compose_bind_mount_dirs(p)
    os.environ["IT_TRACKS_HOST_LOG_DIR"] = str(p.resolve())


_ensure_default_it_tracks_host_log_dir()

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
    return helpers.es_version_worker_count(config)


def pytest_configure(config: pytest.Config) -> None:
    """Resolve ES version list once and stash for ``pytest_generate_tests``."""
    cli_es = config.getoption("--it-tracks-es-versions")
    config.stash[_IT_TRACKS_ES_VERSIONS_KEY] = helpers.resolve_es_versions(cli_es)


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
    """Deselect ``test_race_with_track`` nodes by track name; optionally add xdist groups per ES version.

    ``tryfirst=True`` ensures ``xdist_group`` marks exist before pytest-xdist's worker
    ``pytest_collection_modifyitems`` (see ``xdist.remote.WorkerInteractor``): that hook
    appends ``@<group>`` to ``item._nodeid`` when ``--dist loadgroup``. If this hook ran
    later, nodeids would stay ungrouped and scheduling would load-balance individual tests,
    mixing Elasticsearch versions across workers.

    Per-version groups are skipped when ``-n`` exceeds the ES version count (see
    ``helpers.xdist_group_by_es_version``).
    """
    cli = config.getoption("--it-tracks-name", default=None)
    patterns = helpers.resolve_track_name_patterns(cli, os.environ.get("IT_TRACKS_NAME"))
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
    if xdist_group is not None and helpers.xdist_group_by_es_version(config):
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
    omits items that would skip for ``TrackCase.skip_reason_by_es_version`` (ordered prefix
    pairs; same rule as ``race_test.test_race_with_track``) at the active ES version when no-skip
    mode is off. If
    ``callspec`` cannot be read, the item still counts (conservative budget).

    Stashes ``race_timeout_s_base`` for the controller to send to workers via
    ``pytest_configure_node``. Workers prefer ``workerinput['it_tracks_race_timeout_s']``
    when set so the timeout matches the controller's global ``N``.
    """
    race_items = [i for i in session.items if "test_race_with_track" in i.nodeid]
    no_skip = not helpers.skip_reasons_enabled(session.config)
    n = 0
    for item in race_items:
        try:
            params = item.callspec.params
            case = params["case"]
            es_version = params["elasticsearch"]
        except (AttributeError, KeyError, ValueError):
            n += 1
            continue
        if helpers.race_item_counts_toward_timeout_budget(
            no_skip=no_skip,
            skip_reason_by_es_version=case.skip_reason_by_es_version,
            es_version=es_version,
        ):
            n += 1
    cli_t = session.config.getoption("--it-tracks-total-timeout-minutes")
    tmin = helpers.total_timeout_minutes(cli_t)
    race_timeout_s_base = (tmin * 60) / max(1, n)
    session.config.stash[_IT_TRACKS_RACE_TIMEOUT_BASE_S_KEY] = race_timeout_s_base

    workerinput = getattr(session.config, "workerinput", None)
    if isinstance(workerinput, dict) and "it_tracks_race_timeout_s" in workerinput:
        session.config.stash[_IT_TRACKS_RACE_TIMEOUT_S_KEY] = float(workerinput["it_tracks_race_timeout_s"])
    else:
        if isinstance(workerinput, dict) and "workercount" in workerinput:
            nw = max(1, int(workerinput["workercount"]))
        else:
            nw = helpers.xdist_num_workers(session.config)
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
    nw = max(1, int(wi.get("workercount", helpers.xdist_num_workers(cfg))))
    wi["it_tracks_race_timeout_s"] = race_timeout_s_base * nw


def pytest_report_header() -> str:
    return "it/tracks: isolated from it/conftest.py (via pytest.ini confcutdir)"


_LOG_FOLLOW_STOP_TIMEOUT_S = 15.0
_LOG_FOLLOW_KILL_TIMEOUT_S = 5.0


@pytest.fixture(autouse=True)
def it_tracks_compose_logs_follow(request: pytest.FixtureRequest, elasticsearch: object) -> Generator[None, None, None]:
    """Background ``docker compose logs -f`` into ``containers.log``; stopped before ``elasticsearch`` teardown."""
    dest = helpers.host_log_dir_for_nodeid(helpers.log_root(), request.node.nodeid) / "containers.log"
    proc, log_f = compose.spawn_compose_logs_follow(dest, cfg=compose.ComposeConfig())
    try:
        yield
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=_LOG_FOLLOW_STOP_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=_LOG_FOLLOW_KILL_TIMEOUT_S)
        try:
            log_f.close()
        except OSError:
            pass


def pytest_sessionstart(session: pytest.Session) -> None:
    """pytest-xdist controller: build the shared Rally image once before workers start.

    Workers still run the module ``build_rally`` fixture (cheap when the image exists). The
    stable ``image:`` in ``compose.yaml`` ensures all workers use the same tag regardless of
    per-test ``COMPOSE_PROJECT_NAME``.
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

    Under pytest-xdist, only **workers** run ``docker compose down`` here; the controller must not run it
    while workers still hold containers. Each test sets ``COMPOSE_PROJECT_NAME`` from its nodeid; this hook
    tears down the **last** project name left in the worker env (per-test teardown also runs
    ``teardown_project`` in the ``elasticsearch`` fixture).
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
