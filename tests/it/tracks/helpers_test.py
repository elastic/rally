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

import dataclasses
import re
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from esrally.utils.cases import cases
from it.tracks import helpers

_LEN_DEFAULT_ES_VERSIONS = len(helpers.DEFAULT_IT_TRACKS_ES_VERSIONS)

_COMPOSE_PROJECT_SAFE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,200}$")


def test_parse_es_versions_csv_empty_uses_defaults() -> None:
    assert helpers.parse_es_versions_csv(None) == helpers.DEFAULT_IT_TRACKS_ES_VERSIONS
    assert helpers.parse_es_versions_csv("") == helpers.DEFAULT_IT_TRACKS_ES_VERSIONS
    assert helpers.parse_es_versions_csv("  ") == helpers.DEFAULT_IT_TRACKS_ES_VERSIONS


def test_parse_es_versions_csv_splits_and_strips() -> None:
    assert helpers.parse_es_versions_csv(" 8.1 , 9.0 ") == ["8.1", "9.0"]


def test_resolve_es_versions_cli_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_ES_VERSIONS", "7.0.0")
    assert helpers.resolve_es_versions("8.0.0,9.0.0") == ["8.0.0", "9.0.0"]


def test_resolve_es_versions_env_when_no_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_ES_VERSIONS", "7.1.0")
    assert helpers.resolve_es_versions(None) == ["7.1.0"]


def test_resolve_es_versions_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    assert helpers.resolve_es_versions(None) == list(helpers.DEFAULT_IT_TRACKS_ES_VERSIONS)


def test_resolve_track_name_patterns_none_when_unset() -> None:
    assert helpers.resolve_track_name_patterns(None, None) is None
    assert helpers.resolve_track_name_patterns("", None) is None


def test_resolve_track_name_patterns_cli_wins_over_env() -> None:
    assert helpers.resolve_track_name_patterns("a*", "b*") == ["a*"]


def test_resolve_track_name_patterns_comma_or() -> None:
    assert helpers.resolve_track_name_patterns("geo*,http*", None) == ["geo*", "http*"]


@dataclasses.dataclass(frozen=True)
class ItSkipXfailAppliesCase:
    """``cli_returns`` is the value of ``getoption('--it-skip-xfail', default=True)`` (``False`` when flag passed)."""

    cli_returns: bool
    env_set: bool
    env_value: str
    want_applies: bool


@cases(
    default_empty_env=ItSkipXfailAppliesCase(True, False, "", True),
    flag_disables_unset_env=ItSkipXfailAppliesCase(False, False, "", False),
    env_true=ItSkipXfailAppliesCase(True, True, "1", True),
    env_true_word=ItSkipXfailAppliesCase(True, True, "true", True),
    env_false=ItSkipXfailAppliesCase(True, True, "0", False),
    env_false_word=ItSkipXfailAppliesCase(True, True, "false", False),
    cli_overrides_env_true=ItSkipXfailAppliesCase(False, True, "1", False),
)
def test_it_skip_xfail_applies(case: ItSkipXfailAppliesCase, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_SKIP_XFAIL", raising=False)
    if case.env_set:
        monkeypatch.setenv("IT_SKIP_XFAIL", case.env_value)
    cfg = SimpleNamespace(
        getoption=lambda opt, default=True: case.cli_returns if opt == "--it-skip-xfail" else default,
    )
    assert helpers.it_skip_xfail_applies(cfg) is case.want_applies


@dataclasses.dataclass(frozen=True)
class ItSkipXfailInvalidEnvCase:
    env: str


@cases(
    on=ItSkipXfailInvalidEnvCase("on"),
    on_upper=ItSkipXfailInvalidEnvCase("ON"),
    yes_upper=ItSkipXfailInvalidEnvCase("YES"),
    true_upper=ItSkipXfailInvalidEnvCase("TRUE"),
    maybe=ItSkipXfailInvalidEnvCase("maybe"),
    tru=ItSkipXfailInvalidEnvCase("tru"),
)
def test_it_skip_xfail_applies_invalid_env_raises(case: ItSkipXfailInvalidEnvCase, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_SKIP_XFAIL", case.env)
    cfg = SimpleNamespace(getoption=lambda opt, default=True: True if opt == "--it-skip-xfail" else default)
    with pytest.raises(ValueError):
        helpers.it_skip_xfail_applies(cfg)


def _xfail_spec(prefix: str, reason: str) -> helpers.ExpectCommandFailure:
    return helpers.ExpectCommandFailure(
        returncode=64,
        stdout="msg",
        reason=reason,
        es_version_prefix=prefix,
    )


@pytest.mark.parametrize(
    ("spec", "es_version", "expect_reason"),
    [
        (None, "8.19.14", None),
        (_xfail_spec("8.", "eight"), "8.19.14", "eight"),
        (_xfail_spec("8.", "eight"), "9.3.3", None),
        (_xfail_spec("9.", "nine"), "9.3.3", "nine"),
        (_xfail_spec("", "all"), "8.19.14", "all"),
        (_xfail_spec("", "all"), "9.3.3", "all"),
    ],
)
def test_expected_xfail_for_es_version(
    spec: helpers.ExpectCommandFailure | None,
    es_version: str,
    expect_reason: str | None,
) -> None:
    got = helpers.expected_xfail_for_es_version(spec, es_version)
    if expect_reason is None:
        assert got is None
    else:
        assert got is not None
        assert got.reason == expect_reason


def test_expect_command_failure_fields_and_version_prefix() -> None:
    spec = helpers.ExpectCommandFailure(
        returncode=3,
        stdout="needle",
        reason="because",
        es_version_prefix="9.",
    )
    assert spec.returncode == 3
    assert spec.stdout == "needle"
    assert spec.reason == "because"
    assert spec.es_version_prefix == "9."
    assert helpers.expected_xfail_for_es_version(spec, "9.3.3") is spec
    assert helpers.expected_xfail_for_es_version(spec, "8.0.0") is None


def test_total_timeout_minutes_cli_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_TIMEOUT_MINUTES", "60")
    assert helpers.total_timeout_minutes(90) == 90


def test_total_timeout_minutes_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_TIMEOUT_MINUTES", "45")
    assert helpers.total_timeout_minutes(None) == 45


def test_total_timeout_minutes_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_TIMEOUT_MINUTES", raising=False)
    assert helpers.total_timeout_minutes(None) == 120


def test_race_timeout_formula_matches_conftest() -> None:
    """``(total_min * 60) / max(1, N) * workers`` (``workers`` 1 without xdist)."""
    total_min = 120
    n = 76
    assert helpers.race_timeout_seconds(total_min, n) == pytest.approx(7200 / 76)
    assert helpers.race_timeout_seconds(total_min, 0) == 7200.0
    assert helpers.race_timeout_seconds(total_min, n, xdist_num_workers=2) == pytest.approx(2 * 7200 / 76)


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
    assert helpers.xdist_num_workers(cfg) == expect


def test_it_tracks_xdist_num_workers_prefers_pytest_xdist_worker_count_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worker subprocesses clear ``numprocesses``; xdist sets ``PYTEST_XDIST_WORKER_COUNT``."""
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
    cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=None))
    assert helpers.xdist_num_workers(cfg) == 4


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
    assert helpers.xdist_group_by_es_version(cfg) is expect_group


def test_it_tracks_xdist_group_by_es_version_false_when_xdist_env_worker_count_exceeds_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirrors xdist worker: ``numprocesses`` is None but env carries the real pool size."""
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "4")
    cfg = SimpleNamespace(option=SimpleNamespace(numprocesses=None))
    assert helpers.xdist_group_by_es_version(cfg) is False


def test_it_tracks_es_version_worker_count_uses_defaults_without_cli(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)

    def getoption(name: str, default: object = None) -> object:
        return default

    cfg = SimpleNamespace(getoption=getoption)
    assert helpers.es_version_worker_count(cfg) == _LEN_DEFAULT_ES_VERSIONS


def test_it_tracks_es_version_worker_count_uses_cli_csv() -> None:
    def getoption(name: str, default: object = None) -> object:
        if name == "--it-tracks-es-versions":
            return "7.0.0,8.0.0,9.0.0"
        return default

    assert helpers.es_version_worker_count(SimpleNamespace(getoption=getoption)) == 3


def test_it_tracks_es_version_worker_count_without_getoption_uses_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IT_TRACKS_ES_VERSIONS", raising=False)
    assert helpers.es_version_worker_count(SimpleNamespace()) == _LEN_DEFAULT_ES_VERSIONS


def _budget_spec(prefix: str, reason: str = "r") -> helpers.ExpectCommandFailure:
    return helpers.ExpectCommandFailure(
        returncode=64,
        stdout="m",
        reason=reason,
        es_version_prefix=prefix,
    )


@pytest.mark.parametrize(
    ("skip_xfail_applies", "failure_spec", "es_version", "expect"),
    [
        (False, _budget_spec("8"), "8.19.14", True),
        (False, None, "8.19.14", True),
        (True, None, "8.19.14", True),
        (True, _budget_spec("8", "skip me"), "8.19.14", False),
        (True, _budget_spec("8", ""), "8.19.14", False),
        (True, _budget_spec("8", "skip me"), "9.3.3", True),
        (True, _budget_spec("9", "other"), "8.19.14", True),
        (True, _budget_spec("8.", "skip eight"), "8.19.14", False),
        (True, _budget_spec("8.", "skip eight"), "9.3.3", True),
        (True, _budget_spec("9.", "nine"), "9.3.3", False),
        (True, _budget_spec("", "all"), "8.19.14", False),
        (True, _budget_spec("", "all"), "9.3.3", False),
    ],
)
def test_race_item_counts_toward_timeout_budget(
    skip_xfail_applies: bool,
    failure_spec: helpers.ExpectCommandFailure | None,
    es_version: str,
    expect: bool,
) -> None:
    assert (
        helpers.race_item_counts_toward_timeout_budget(
            skip_xfail_applies=skip_xfail_applies,
            expect_failure=failure_spec,
            es_version=es_version,
        )
        is expect
    )


def test_it_tracks_log_root_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    base = tmp_path / "lr"
    base.mkdir()
    monkeypatch.setenv("IT_TRACKS_LOG_ROOT", str(base))
    assert helpers.log_root() == base.resolve()


def test_it_tracks_log_root_default_name_is_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IT_TRACKS_LOG_ROOT", raising=False)
    assert helpers.log_root().name == "logs"


def test_it_tracks_host_log_dir_for_nodeid_mirrors_pytest_nodeid(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    root.mkdir()
    nodeid = "it/tracks/race_test.py::test_race[es_8.19.14-elastic_logs]"
    got = helpers.host_log_dir_for_nodeid(root, nodeid)
    assert got == root / "it" / "tracks" / "race_test.py" / "test_race[es_8.19.14-elastic_logs]"


def test_it_tracks_host_log_dir_short_nodeid_gets_it_tracks_prefix(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    root.mkdir()
    nodeid = "race_test.py::test_race[es_9.3.3-geonames]"
    got = helpers.host_log_dir_for_nodeid(root, nodeid)
    assert got == root / "it" / "tracks" / "race_test.py" / "test_race[es_9.3.3-geonames]"


def test_it_tracks_host_log_dir_without_double_colon(tmp_path: Path) -> None:
    root = tmp_path / "logs"
    root.mkdir()
    assert helpers.host_log_dir_for_nodeid(root, "single/id") == root / "single" / "id"


def test_prepare_it_tracks_compose_bind_mount_dirs(tmp_path: Path) -> None:
    helpers.prepare_compose_bind_mount_dirs(tmp_path)
    for name in ("es01", "rally"):
        d = tmp_path / name
        assert d.is_dir()
        assert stat.S_IMODE(d.stat().st_mode) == 0o777


def test_compose_project_name_for_nodeid_stable_slug() -> None:
    nodeid = "it/tracks/race_test.py::test_race[es_8.19.14-elastic_logs]"
    a = helpers.compose_project_name_for_nodeid(nodeid)
    b = helpers.compose_project_name_for_nodeid(nodeid)
    assert a == b
    assert _COMPOSE_PROJECT_SAFE_RE.fullmatch(a) or a.startswith("ittr_")
