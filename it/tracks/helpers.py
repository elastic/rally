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

"""Pure helpers for ``it/tracks`` pytest options (no pytest imports)."""

from __future__ import annotations

import os

# Default Elasticsearch distribution versions for track race IT (Docker compose es01 image tag).
DEFAULT_IT_TRACKS_ES_VERSIONS: list[str] = ["8.19.14", "9.3.3"]


def parse_es_versions_csv(raw: str | None) -> list[str]:
    """Parse a comma-separated list of ES version strings (e.g. ``8.19.14,9.3.3``)."""
    if not raw or not raw.strip():
        return list(DEFAULT_IT_TRACKS_ES_VERSIONS)
    return [p.strip() for p in raw.split(",") if p.strip()]


def resolve_es_versions(cli_value: str | None) -> list[str]:
    """Resolve ES versions: non-empty CLI string wins, else ``IT_TRACKS_ES_VERSIONS`` env, else defaults."""
    if cli_value is not None and str(cli_value).strip():
        return parse_es_versions_csv(str(cli_value))
    env = os.environ.get("IT_TRACKS_ES_VERSIONS")
    if env is not None and env.strip():
        return parse_es_versions_csv(env)
    return list(DEFAULT_IT_TRACKS_ES_VERSIONS)


def resolve_track_name_patterns(cli_value: str | None, env_value: str | None) -> list[str] | None:
    """Return ``fnmatch`` patterns for ``TrackCase.track_name``, or ``None`` if no filter is active.

    Non-empty CLI wins over env. Comma-separated patterns are OR-ed during matching.
    """
    if cli_value is not None and str(cli_value).strip():
        raw = str(cli_value).strip()
    elif env_value is not None and str(env_value).strip():
        raw = str(env_value).strip()
    else:
        return None
    patterns = [p.strip() for p in raw.split(",") if p.strip()]
    return patterns or None


def it_tracks_no_skip_enabled(*, cli_flag: bool, env_name: str = "IT_TRACKS_NO_SKIP") -> bool:
    """True if ``--it-tracks-no-skip`` was passed or ``IT_TRACKS_NO_SKIP`` is truthy."""
    if cli_flag:
        return True
    v = os.environ.get(env_name, "").strip().lower()
    return v in ("1", "true", "yes", "on")


def it_tracks_no_skip_from_config(config: object) -> bool:
    """True if ``--it-tracks-no-skip`` or ``IT_TRACKS_NO_SKIP`` requests running skip-reason cases."""
    flag = bool(config.getoption("--it-tracks-no-skip", default=False))
    return it_tracks_no_skip_enabled(cli_flag=flag)


def total_timeout_minutes(cli_minutes: int | None, env_name: str = "IT_TRACKS_TIMEOUT_MINUTES", default: int = 120) -> int:
    """Total wall-clock budget (minutes) for all selected race tests before per-node division.

    Precedence: CLI ``--it-tracks-total-timeout-minutes`` if set, else env ``IT_TRACKS_TIMEOUT_MINUTES``, else ``default``.
    """
    if cli_minutes is not None:
        return int(cli_minutes)
    return int(os.environ.get(env_name, str(default)))


def race_item_counts_toward_timeout_budget(
    *,
    no_skip: bool,
    skip_reason_by_es_version: dict[str, str] | None,
    es_version: str,
) -> bool:
    """Whether a parametrized ``test_race_with_track`` node counts toward timeout divisor ``N``.

    When ``no_skip`` is true (``--it-tracks-no-skip`` / ``IT_TRACKS_NO_SKIP``), every collected
    race node counts. Otherwise, nodes that would ``pytest.skip`` for the active ES version
    (``skip_reason_by_es_version.get(es_version) is not None``) do not count—matching
    ``race_test.test_race_with_track``.
    """
    if no_skip:
        return True
    if not skip_reason_by_es_version:
        return True
    return skip_reason_by_es_version.get(es_version) is None


def it_tracks_es_version_worker_count(config: object) -> int:
    """How many xdist workers to use for one-worker-per-ES-version layout (minimum 1).

    Resolves the ES version list like ``resolve_es_versions``: non-empty
    ``config.getoption('--it-tracks-es-versions', default=None)`` when ``getoption``
    exists, else env ``IT_TRACKS_ES_VERSIONS`` / defaults.
    """
    getoption = getattr(config, "getoption", None)
    cli_es = getoption("--it-tracks-es-versions", default=None) if callable(getoption) else None
    return max(1, len(resolve_es_versions(cli_es)))


def it_tracks_xdist_num_workers(config: object) -> int:
    """Return pytest-xdist worker count, or 1 if not distributed.

    On xdist **worker** processes, ``remote.setup_config`` clears ``option.numprocesses``; pytest-xdist
    sets ``PYTEST_XDIST_WORKER_COUNT`` to the real pool size (see ``xdist.remote``), which we read first.

    On the controller or a normal run, use ``config.option.numprocesses``. For ``numprocesses == "auto"``,
    returns :func:`it_tracks_es_version_worker_count` (one worker per configured ES version, not host CPU).
    """
    wc = os.environ.get("PYTEST_XDIST_WORKER_COUNT")
    if wc is not None:
        try:
            return max(1, int(wc))
        except ValueError:
            pass
    opt = getattr(config, "option", None)
    if opt is None:
        return 1
    np = getattr(opt, "numprocesses", 0)
    if np in (0, False, None):
        return 1
    if np == "auto":
        return it_tracks_es_version_worker_count(config)
    try:
        return max(1, int(np))
    except (TypeError, ValueError):
        return 1


def it_tracks_xdist_group_by_es_version(config: object) -> bool:
    """Whether to mark race tests with ``xdist_group`` per ES version under ``--dist loadgroup``.

    When the requested xdist worker count exceeds the number of configured ES versions, per-version
    groups would cap concurrency at the version count; omitting those marks lets all workers run
    races in parallel (each worker still uses its own ``COMPOSE_PROJECT_NAME``).
    """
    return it_tracks_xdist_num_workers(config) <= it_tracks_es_version_worker_count(config)


def it_tracks_race_timeout_seconds(
    total_timeout_minutes: int,
    n: int,
    *,
    xdist_num_workers: int = 1,
) -> float:
    """Per-race subprocess timeout: ``(total_min * 60) / max(1, N) * max(1, xdist_num_workers)``."""
    base = (total_timeout_minutes * 60) / max(1, n)
    return base * max(1, xdist_num_workers)
