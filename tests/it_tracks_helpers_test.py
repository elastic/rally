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

"""Unit tests for ``it.tracks.helpers`` (ES version parsing, name patterns, timeout, no-skip)."""

from __future__ import annotations

import pytest

from it.tracks.helpers import (
    DEFAULT_IT_TRACKS_ES_VERSIONS,
    it_tracks_no_skip_enabled,
    parse_es_versions_csv,
    race_item_counts_toward_timeout_budget,
    resolve_es_versions,
    resolve_track_name_patterns,
    total_timeout_minutes,
)


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
    ("cli", "env", "expect"),
    [
        (False, "", False),
        (True, "", True),
        (False, "1", True),
        (False, "true", True),
        (False, "YES", True),
        (False, "on", True),
        (False, "0", False),
    ],
)
def test_it_tracks_no_skip_enabled(cli: bool, env: str, expect: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IT_TRACKS_NO_SKIP", env)
    assert it_tracks_no_skip_enabled(cli_flag=cli) is expect


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
    """``(total_min * 60) / max(1, N)`` as computed in ``it/tracks/conftest.py``."""
    total_min = 120
    n = 76
    assert (total_min * 60) / max(1, n) == pytest.approx(7200 / 76)
    assert (total_min * 60) / max(1, 0) == 7200.0


@pytest.mark.parametrize(
    ("no_skip", "skip_map", "es_version", "expect"),
    [
        (True, {"8": "broken"}, "8", True),
        (True, None, "8", True),
        (False, None, "8", True),
        (False, {}, "8", True),
        (False, {"8": "skip me"}, "8", False),
        (False, {"8": ""}, "8", False),
        (False, {"8": "skip me"}, "9", True),
        (False, {"9": "other"}, "8", True),
    ],
)
def test_race_item_counts_toward_timeout_budget(
    no_skip: bool,
    skip_map: dict[str, str] | None,
    es_version: str,
    expect: bool,
) -> None:
    assert (
        race_item_counts_toward_timeout_budget(
            no_skip=no_skip,
            skip_reason_by_es_version=skip_map,
            es_version=es_version,
        )
        is expect
    )
