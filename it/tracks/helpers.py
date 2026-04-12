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

"""Helpers for ``it/tracks`` pytest options and race-test failure handling."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pytest

from esrally.paths import rally_root
from esrally.utils import compose, convert

# Default Elasticsearch distribution versions for track race IT (Docker compose es01 image tag).
# Keep in sync with the es-version matrix in .github/workflows/ci.yml (job it-tracks-race).
DEFAULT_IT_TRACKS_ES_VERSIONS: list[str] = ["8.19.14", "9.3.3"]


class PytestConfig(Protocol):
    """Narrow config surface for helpers (``pytest.Config`` satisfies this structurally)."""

    def getoption(self, name: str, default: object = None) -> object: ...


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


@dataclass(frozen=True)
class ExpectCommandFailure:
    """Expected command failure for a track race when the ES version matches ``es_version_prefix``.

    ``es_version_prefix`` defaults to ``""`` (match every version).

    When skip-xfail is off, ``stdout`` is matched as a substring of the **decoded**
    combined stdout from the race subprocess (see ``esrally.utils.compose.decode``).
    """

    returncode: int
    stdout: str
    reason: str
    es_version_prefix: str = ""


def it_skip_xfail_applies(config: PytestConfig) -> bool:
    """True when a matching ``TrackCase.expect_failure`` policy triggers ``pytest.skip`` before the race.

    **Default:** skip-xfail behavior is **on** (omit ``--it-skip-xfail`` and leave ``IT_SKIP_XFAIL``
    unset or empty).

    **CLI:** ``--it-skip-xfail`` uses ``store_false`` semantics: passing the flag **disables**
    skip-xfail (this helper returns ``False``) even though the flag name suggests the opposite.

    **Env ``IT_SKIP_XFAIL``:** If set and non-empty after strip, ``esrally.utils.convert.to_bool``
    decides: literals it maps to ``True`` keep skip-xfail **on**; to ``False`` turn it **off**.
    Any other non-empty string raises ``ValueError`` from ``to_bool`` (not caught here).

    CLI wins: if the flag disables skip-xfail, the env is not consulted.
    """
    if not config.getoption("--it-skip-xfail", default=True):
        return False
    raw = os.environ.get("IT_SKIP_XFAIL", "").strip()
    if not raw:
        return True
    return convert.to_bool(raw)


def expected_xfail_for_es_version(
    spec: ExpectCommandFailure | None,
    es_version: str,
) -> ExpectCommandFailure | None:
    """Return ``spec`` when its ``es_version_prefix`` matches ``es_version``, else ``None``."""
    if spec is None:
        return None
    if es_version.startswith(spec.es_version_prefix):
        return spec
    return None


@contextmanager
def expect_command_failure(
    policy: ExpectCommandFailure | None,
    es_version: str,
    config: PytestConfig,
) -> Generator[None, None, None]:
    """Context manager around ``rally race``: no-op when ``policy`` is ``None``.

    Otherwise skip / xfail-on-match for ``policy`` at ``es_version``:
    uses ``expected_xfail_for_es_version`` and ``it_skip_xfail_applies`` for the skip decision;
    subprocess matching uses decoded combined stdout (``esrally.utils.compose.decode``).
    """
    # Case 1: no expected failure for this track — run the race with no skip/xfail handling.
    if policy is None:
        yield
        return
    # Policy applies only when ``es_version`` matches ``policy.es_version_prefix`` (empty prefix = all versions).
    spec = expected_xfail_for_es_version(policy, es_version)
    skip_xfail_applies = it_skip_xfail_applies(config)

    # Case 2: default "skip-xfail" mode — skip the test before ``rally race`` when this version matches the policy.
    if skip_xfail_applies and spec is not None:
        pytest.skip(spec.reason)

    # When skip-xfail is on, we never translate failures (only skip above). When it is off, we may xfail on match.
    xfail_spec = spec if not skip_xfail_applies else None
    # Case 3: run the race with no xfail translation — either the policy does not apply to this ``es_version``
    # (``spec`` is None), or skip-xfail is on (xfail is only armed when skip-xfail is off).
    if xfail_spec is None:
        yield
        return
    # Case 4: skip-xfail off and version matches — run the race; a matching ``CalledProcessError`` becomes xfail, else re-raise.
    try:
        yield
    except subprocess.CalledProcessError as e:
        decoded = compose.decode(e.stdout)
        if e.returncode == xfail_spec.returncode and xfail_spec.stdout in decoded:
            pytest.xfail(xfail_spec.reason)
        raise


def total_timeout_minutes(cli_minutes: int | None, env_name: str = "IT_TRACKS_TIMEOUT_MINUTES", default: int = 120) -> int:
    """Total wall-clock budget (minutes) for all selected race tests before per-node division.

    Precedence: CLI ``--it-tracks-total-timeout-minutes`` if set, else env ``IT_TRACKS_TIMEOUT_MINUTES``, else ``default``.
    """
    if cli_minutes is not None:
        return int(cli_minutes)
    return int(os.environ.get(env_name, str(default)))


def race_item_counts_toward_timeout_budget(
    *,
    skip_xfail_applies: bool,
    expect_failure: ExpectCommandFailure | None,
    es_version: str,
) -> bool:
    """Whether a parametrized ``test_race`` node counts toward timeout divisor ``N``.

    When ``skip_xfail_applies`` is false (``--it-skip-xfail`` passed, or ``IT_SKIP_XFAIL`` parsed
    as false), every collected race node counts. Otherwise, nodes that would ``pytest.skip`` for
    the active ES version (``expect_failure`` is set and its prefix matches) do not count—matching
    ``race_test.test_race``.
    """
    if not skip_xfail_applies:
        return True
    return expected_xfail_for_es_version(expect_failure, es_version) is None


def es_version_worker_count(config: object) -> int:
    """How many xdist workers to use for one-worker-per-ES-version layout (minimum 1).

    Resolves the ES version list like ``resolve_es_versions``: non-empty
    ``config.getoption('--it-tracks-es-versions', default=None)`` when ``getoption``
    exists, else env ``IT_TRACKS_ES_VERSIONS`` / defaults.
    """
    getoption = getattr(config, "getoption", None)
    cli_es = getoption("--it-tracks-es-versions", default=None) if callable(getoption) else None
    return max(1, len(resolve_es_versions(cli_es)))


def xdist_num_workers(config: object) -> int:
    """Return pytest-xdist worker count, or 1 if not distributed.

    On xdist **worker** processes, ``remote.setup_config`` clears ``option.numprocesses``; pytest-xdist
    sets ``PYTEST_XDIST_WORKER_COUNT`` to the real pool size (see ``xdist.remote``), which we read first.

    On the controller or a normal run, use ``config.option.numprocesses``. For ``numprocesses == "auto"``,
    returns ``es_version_worker_count`` (one worker per configured ES version, not host CPU).
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
        return es_version_worker_count(config)
    try:
        return max(1, int(np))
    except (TypeError, ValueError):
        return 1


def xdist_group_by_es_version(config: object) -> bool:
    """Whether to mark race tests with ``xdist_group`` per ES version under ``--dist loadgroup``.

    When the requested xdist worker count exceeds the number of configured ES versions, per-version
    groups would cap concurrency at the version count; omitting those marks lets all workers run
    races in parallel. Each test uses a distinct ``COMPOSE_PROJECT_NAME`` derived from its nodeid.
    """
    return xdist_num_workers(config) <= es_version_worker_count(config)


def log_root() -> Path:
    """Base directory for host-mounted it/tracks logs (gitignored ``logs/`` at the checkout root).

    Default is ``<rally-checkout>/logs`` (parent of the ``esrally`` package directory from
    ``rally_root``, then ``logs``). Override with absolute path in env ``IT_TRACKS_LOG_ROOT``.
    """
    raw = os.environ.get("IT_TRACKS_LOG_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path(rally_root()).resolve().parent / "logs"


def prepare_compose_bind_mount_dirs(host_log: Path) -> None:
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


def host_log_dir_for_nodeid(log_root: Path, nodeid: str) -> Path:
    """Return the host directory for ``es01/`` and ``rally/`` log bind mounts for this pytest node.

    Mirrors the pytest nodeid with ``::`` turned into a path segment (``/``) so
    ``IT_TRACKS_HOST_LOG_DIR`` has no ``:`` characters—Docker Compose treats ``:`` as special in
    volume specs.

    When the file segment is only a basename (typical under ``it/tracks/pytest.ini`` confcutdir),
    e.g. ``race_test.py::test_race[…]``, the directory is normalized under
    ``…/logs/it/tracks/race_test.py/…``. A full path such as
    ``it/tracks/race_test.py::test_race[…]`` is left as-is under ``log_root``.
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


def race_timeout_seconds(
    total_timeout_minutes: int,
    n: int,
    *,
    xdist_num_workers: int = 1,
) -> float:
    """Per-race subprocess timeout: ``(total_min * 60) / max(1, N) * max(1, xdist_num_workers)``."""
    base = (total_timeout_minutes * 60) / max(1, n)
    return base * max(1, xdist_num_workers)
