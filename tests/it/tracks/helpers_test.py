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

"""Unit tests for ``it.tracks.helpers`` (ES version parsing, name patterns, timeout)."""

from __future__ import annotations

import re
import stat
from types import SimpleNamespace

import pytest

from it.tracks.helpers import (
    DEFAULT_IT_TRACKS_ES_VERSIONS,
    compose_project_name_for_nodeid,
    es_version_worker_count,
    host_log_dir_for_nodeid,
    log_root,
    parse_es_versions_csv,
    prepare_compose_bind_mount_dirs,
    race_item_counts_toward_timeout_budget,
    race_timeout_seconds,
    resolve_es_versions,
    resolve_track_name_patterns,
    skip_reason_for_entries,
    skip_reasons_enabled,
    total_timeout_minutes,
    xdist_group_by_es_version,
    xdist_num_workers,
)

_LEN_DEFAULT_ES_VERSIONS = len(DEFAULT_IT_TRACKS_ES_VERSIONS)

_COMPOSE_PROJECT_SAFE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,200}$")


def test_parse_es_versions_csv_empty_uses_defaults() -> None:
    assert parse_es_versions_csv(None) == DEFAULT_IT_TRACKS_ES_VERSIONS
    assert parse_es_versions_csv("") == DEFAULT_IT_TRACKS_ES_VERSIONS
    assert parse_es_versions_csv("  ") == DEFAULT_IT_TRACKS_ES_VERSIONS


def test_parse_es_versions_csv_splits_and_strips() -> None:
    assert parse_es_versions_csv(" 8.1 , 9.0 ") == ["8.1", "9.0"]


def test_resolve_es_versions_cli_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_ES_VERSIONS", "7.0.0")
    assert resolve_es_versions("8.0.0,9.0.0") == ["8.0.0", "9.0.0"]


def test_resolve_es_versions_env_when_no_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_ES_VERSIONS", "7.1.0")
    assert resolve_es_versions(None) == ["7.1.0"]


def test_resolve_es_versions_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    assert resolve_es_versions(None) == list(DEFAULT_IT_TRACKS_ES_VERSIONS)


def test_resolve_track_name_patterns_none_when_unset() -> None:
    assert resolve_track_name_patterns(None, None) is None
    assert resolve_track_name_patterns("", None) is None


def test_resolve_track_name_patterns_cli_wins_over_env() -> None:
    assert resolve_track_name_patterns("a*", "b*") == ["a*"]


def test_resolve_track_name_patterns_comma_or() -> None:
    assert resolve_track_name_patterns("geo*,http*", None) == ["geo*", "http*"]


@pytest.mark.parametrize(
    ("cli_no_skip", "env", "expect_enabled"),
    [
        (False, "", True),
        (True, "", False),
        (False, "1", False),
        (False, "true", False),
        (False, "YES", False),
        (False, "on", False),
        (False, "ON", False),
        (False, "0", True),
        (False, "", True),
        (False, "maybe", True),
        (False, "tru", True),
        (False, "TRUE", False),
    ],
)
def test_it_tracks_skip_reasons_enabled(
    cli_no_skip: bool,
    env: str,
    expect_enabled: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("IT_TRACKS_NO_SKIP", env)
    cfg = SimpleNamespace(
        getoption=lambda opt, default=False: cli_no_skip if opt == "--it-tracks-no-skip" else default,
    )
    assert skip_reasons_enabled(cfg) is expect_enabled


@pytest.mark.parametrize(
    ("entries", "es_version", "expect_reason"),
    [
        (None, "8.19.14", None),
        ([], "9.3.3", None),
        ([("8.", "eight")], "8.19.14", "eight"),
        ([("8.", "eight")], "9.3.3", None),
        ([("9.", "nine")], "9.3.3", "nine"),
        ([("", "all")], "8.19.14", "all"),
        ([("8.", "specific"), ("", "fallback")], "8.19.14", "specific"),
        ([("8.", "specific"), ("", "fallback")], "9.3.3", "fallback"),
        ([("", "fallback"), ("8.", "specific")], "8.19.14", "fallback"),
    ],
)
def test_it_tracks_skip_reason_for_entries(
    entries: list[tuple[str, str]] | None,
    es_version: str,
    expect_reason: str | None,
) -> None:
    assert skip_reason_for_entries(entries, es_version) == expect_reason


def test_total_timeout_minutes_cli_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_TIMEOUT_MINUTES", "60")
    assert total_timeout_minutes(90) == 90


def test_total_timeout_minutes_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_TIMEOUT_MINUTES", "45")
    assert total_timeout_minutes(None) == 45


def test_total_timeout_minutes_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_TIMEOUT_MINUTES", raising=False)
    assert total_timeout_minutes(None) == 120


def test_race_timeout_formula_matches_conftest() -> None:
    """``(total_min * 60) / max(1, N) * workers`` (``workers`` 1 without xdist)."""
    total_min = 120
    n = 76
    assert race_timeout_seconds(total_min, n) == pytest.approx(7200 / 76)
    assert race_timeout_seconds(total_min, 0) == 7200.0
    assert race_timeout_seconds(total_min, n, xdist_num_workers=2) == pytest.approx(2 * 7200 / 76)


@pytest.mark.parametrize(
    ("numprocesses", "expect", "auto_cli_es", "clear_it_tracks_es_env"),
    [
        (0, 1, None, False),
        (False, 1, None, False),
        (None, 1, None, False),
        (2, 2, None, False),
        ("auto", _LEN_DEFAULT_ES_VERSIONS, None, True),
        ("auto", 3, "7.0.0,8.0.0,9.0.0", True),
    ],
)
def test_it_tracks_xdist_num_workers(
    numprocesses: object,
    expect: int,
    auto_cli_es: str | None,
    clear_it_tracks_es_env: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)
    if clear_it_tracks_es_env:
        monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)

    def getoption(name: str, default: object = None) -> object:
        if name == "--it-tracks-es-versions":
            return default if auto_cli_es is None else auto_cli_es
        return default

    if numprocesses == "auto":
        cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=numprocesses), getoption=getoption)
    else:
        cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=numprocesses))
    assert xdist_num_workers(cfg) == expect


def test_it_tracks_xdist_num_workers_prefers_pytest_xdist_worker_count_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker subprocesses clear ``numprocesses``; xdist sets ``PYTEST_XDIST_WORKER_COUNT``."""
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
    cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=None))
    assert xdist_num_workers(cfg) == 4


@pytest.mark.parametrize(
    ("numprocesses", "cli_es", "expect_group", "needs_getoption"),
    [
        (4, None, False, False),
        (2, "7.0.0,8.0.0,9.0.0,10.0.0", True, True),
        (4, "7.0.0,8.0.0,9.0.0,10.0.0", True, True),
        (0, None, True, False),
        (2, None, True, True),
        ("auto", None, True, True),
    ],
)
def test_it_tracks_xdist_group_by_es_version(
    numprocesses: object,
    cli_es: str | None,
    expect_group: bool,
    needs_getoption: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTEST_XDIST_WORKER_COUNT", raising=False)
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)

    def getoption(name: str, default: object = None) -> object:
        if name == "--it-tracks-es-versions":
            return default if cli_es is None else cli_es
        return default

    if needs_getoption or numprocesses == "auto":
        cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=numprocesses), getoption=getoption)
    else:
        cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=numprocesses))
    assert xdist_group_by_es_version(cfg) is expect_group


def test_it_tracks_xdist_group_by_es_version_false_when_xdist_env_worker_count_exceeds_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirrors xdist worker: ``numprocesses`` is None but env carries the real pool size."""
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
    cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=None))
    assert xdist_group_by_es_version(cfg) is False


def test_it_tracks_es_version_worker_count_uses_defaults_without_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)

    def getoption(name: str, default: object = None) -> object:
        return default

    cfg = SimpleNamespace(getoption=getoption)
    assert es_version_worker_count(cfg) == _LEN_DEFAULT_ES_VERSIONS


def test_it_tracks_es_version_worker_count_uses_cli_csv() -> None:
    def getoption(name: str, default: object = None) -> object:
        if name == "--it-tracks-es-versions":
            return "7.0.0,8.0.0,9.0.0"
        return default

    assert es_version_worker_count(SimpleNamespace(getoption=getoption)) == 3


def test_it_tracks_es_version_worker_count_without_getoption_uses_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    assert es_version_worker_count(SimpleNamespace()) == _LEN_DEFAULT_ES_VERSIONS


@pytest.mark.parametrize(
    ("no_skip", "skip_entries", "es_version", "expect"),
    [
        (True, [("8", "broken")], "8.19.14", True),
        (True, None, "8.19.14", True),
        (False, None, "8.19.14", True),
        (False, [], "8.19.14", True),
        (False, [("8", "skip me")], "8.19.14", False),
        (False, [("8", "")], "8.19.14", False),
        (False, [("8", "skip me")], "9.3.3", True),
        (False, [("9", "other")], "8.19.14", True),
        (False, [("8.", "skip eight")], "8.19.14", False),
        (False, [("8.", "skip eight")], "9.3.3", True),
        (False, [("9.", "nine")], "9.3.3", False),
        (False, [("", "all")], "8.19.14", False),
        (False, [("", "all")], "9.3.3", False),
        (False, [("8.", "specific"), ("", "fallback")], "8.19.14", False),
        (False, [("8.", "specific"), ("", "fallback")], "9.3.3", False),
        (False, [("", "fallback"), ("8.", "specific")], "8.19.14", False),
    ],
)
def test_race_item_counts_toward_timeout_budget(
    no_skip: bool,
    skip_entries: list[tuple[str, str]] | None,
    es_version: str,
    expect: bool,
) -> None:
    assert (
        race_item_counts_toward_timeout_budget(
            no_skip=no_skip,
            skip_reason_by_es_version=skip_entries,
            es_version=es_version,
        )
        is expect
    )


def test_it_tracks_log_root_from_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "lr"
    base.mkdir()
    monkeypatch.setenv("IT_TRACKS_LOG_ROOT", str(base))
    assert log_root() == base.resolve()


def test_it_tracks_log_root_default_name_is_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_LOG_ROOT", raising=False)
    assert log_root().name == "logs"


def test_it_tracks_host_log_dir_for_nodeid_mirrors_pytest_nodeid(tmp_path) -> None:
    log_root = tmp_path / "logs"
    log_root.mkdir()
    nodeid = "it/tracks/race_test.py::test_race_with_track[es_8.19.14-elastic_logs]"
    got = host_log_dir_for_nodeid(log_root, nodeid)
    assert got == log_root / "it" / "tracks" / "race_test.py" / "test_race_with_track[es_8.19.14-elastic_logs]"


def test_it_tracks_host_log_dir_short_nodeid_gets_it_tracks_prefix(tmp_path) -> None:
    log_root = tmp_path / "logs"
    log_root.mkdir()
    nodeid = "race_test.py::test_race_with_track[es_9.3.3-geonames]"
    got = host_log_dir_for_nodeid(log_root, nodeid)
    assert got == log_root / "it" / "tracks" / "race_test.py" / "test_race_with_track[es_9.3.3-geonames]"


def test_it_tracks_host_log_dir_without_double_colon(tmp_path) -> None:
    log_root = tmp_path / "logs"
    log_root.mkdir()
    assert host_log_dir_for_nodeid(log_root, "single/id") == log_root / "single" / "id"


def test_prepare_it_tracks_compose_bind_mount_dirs(tmp_path) -> None:
    prepare_compose_bind_mount_dirs(tmp_path)
    for name in ("es01", "rally"):
        d = tmp_path / name
        assert d.is_dir()
        assert stat.S_IMODE(d.stat().st_mode) == 0o777


def test_compose_project_name_for_nodeid_stable_slug() -> None:
    nodeid = "it/tracks/race_test.py::test_race_with_track[es_8.19.14-elastic_logs]"
    a = compose_project_name_for_nodeid(nodeid)
    b = compose_project_name_for_nodeid(nodeid)
    assert a == b
    assert _COMPOSE_PROJECT_SAFE_RE.fullmatch(a) or a.startswith("ittr_")
