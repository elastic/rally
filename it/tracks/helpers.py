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

import hashlib
import os
import re
import stat
from pathlib import Path

from esrally.paths import rally_root

# Default Elasticsearch distribution versions for track race IT (Docker compose es01 image tag).
# Keep in sync with the es-version matrix in .github/workflows/ci.yml (job it-tracks-race).
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
    races in parallel. Each test uses a distinct ``COMPOSE_PROJECT_NAME`` derived from its nodeid.
    """
    return it_tracks_xdist_num_workers(config) <= it_tracks_es_version_worker_count(config)


def it_tracks_log_root() -> Path:
    """Base directory for host-mounted it/tracks logs (gitignored ``logs/`` at the checkout root).

    Default is ``<rally-checkout>/logs`` (parent of the ``esrally`` package directory from
    :func:`rally_root`, then ``logs``). Override with absolute path in env ``IT_TRACKS_LOG_ROOT``.
    """
    raw = os.environ.get("IT_TRACKS_LOG_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path(rally_root()).resolve().parent / "logs"


def prepare_it_tracks_compose_bind_mount_dirs(host_log: Path) -> None:
    """Create ``es01/`` and ``rally/`` under ``host_log`` and allow the stack to write there.

    Compose bind-mounts these paths into the Elasticsearch and Rally images, which run as
    uid **1000**. Pytest on CI often runs as another uid, and on Linux Docker creates
    missing host paths for bind mounts as **root**—either case yields permission denied
    when the JVM opens ``gc.log`` under ``…/es01`` (or Rally writes under ``…/rally``).

    Call this after creating ``host_log`` and before ``docker compose up`` for it/tracks.
    """
    mode = stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO
    for name in ("es01", "rally"):
        d = host_log / name
        d.mkdir(parents=True, exist_ok=True)
        d.chmod(mode)


def it_tracks_host_log_dir_for_nodeid(log_root: Path, nodeid: str) -> Path:
    """Return the host directory for ``es01/`` and ``rally/`` log bind mounts for this pytest node.

    Mirrors the pytest nodeid with ``::`` turned into a path segment (``/``) so
    ``IT_TRACKS_HOST_LOG_DIR`` has no ``:`` characters—Docker Compose treats ``:`` as special in
    volume specs.

    When the file segment is only a basename (typical under ``it/tracks/pytest.ini`` confcutdir),
    e.g. ``race_test.py::test_race_with_track[…]``, the directory is normalized under
    ``…/logs/it/tracks/race_test.py/…``. A full path such as
    ``it/tracks/race_test.py::test_race_with_track[…]`` is left as-is under ``log_root``.
    """
    if "::" not in nodeid:
        return (log_root / nodeid).resolve()
    file_part, test_part = nodeid.split("::", 1)
    path = Path(file_part)
    if path.parent == Path("."):
        path = Path("it") / "tracks" / path.name
    return (log_root / path.parent / path.name / test_part).resolve()


_COMPOSE_PROJECT_SAFE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,200}$")


def compose_project_name_for_nodeid(nodeid: str) -> str:
    """Docker-Compose-safe ``-p`` value, unique per pytest ``nodeid`` (raw nodeid is usually invalid)."""
    lowered = nodeid.lower()
    if _COMPOSE_PROJECT_SAFE.fullmatch(lowered):
        return lowered
    out: list[str] = []
    for ch in lowered:
        if ch.isalnum() or ch in "_-":
            out.append(ch)
        else:
            out.append("_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    if len(slug) > 200:
        slug = slug[:200].rstrip("_")
    if _COMPOSE_PROJECT_SAFE.fullmatch(slug):
        return slug
    return "ittr_" + hashlib.sha256(nodeid.encode("utf-8")).hexdigest()[:48]


def it_tracks_race_timeout_seconds(
    total_timeout_minutes: int,
    n: int,
    *,
    xdist_num_workers: int = 1,
) -> float:
    """Per-race subprocess timeout: ``(total_min * 60) / max(1, N) * max(1, xdist_num_workers)``."""
    base = (total_timeout_minutes * 60) / max(1, n)
    return base * max(1, xdist_num_workers)
