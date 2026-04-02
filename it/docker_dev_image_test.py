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
import dataclasses
import difflib
import logging
import os
import subprocess
from collections.abc import Callable, Generator, Mapping
from pathlib import Path

import pytest

import it
from esrally import version
from esrally.utils import cases

LOG = logging.getLogger(__name__)

# Compose file used by both this test module and release-docker-test.sh.
COMPOSE_FILE = os.path.join(it.ROOT_DIR, "docker", "docker-compose-tests.yml")

# Expected ``list tracks`` table (normalized) for the docker dev image IT, under it/resources/.
LIST_TRACKS_GOLDEN_REL_PATH = os.path.join("resources", "docker_dev_image_list_tracks_stdout.golden.txt")
# Set to ``1`` to overwrite that golden from the current docker run (e.g. after track list changes).
UPDATE_DOCKER_LIST_TRACKS_GOLDEN_ENV = "RALLY_UPDATE_DOCKER_LIST_TRACKS_GOLDEN"

# Start of Rally's tabular output; everything before this (e.g. ASCII banner) is ignored for goldens.
_AVAILABLE_TRACKS_MARKER = "Available tracks:"

# Shared race invocation against the ES service defined in docker-compose-tests.yml (es01:9200).
_RACE_FLAGS = (
    "race --pipeline=benchmark-only --test-mode --track=geonames "
    "--challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
)


def tear_down_stack(compose_env: Mapping[str, str]) -> None:
    """Stop and remove the compose project so the next run starts from a clean stack."""
    LOG.info("Tearing down docker stack... (compose_env=%s)", compose_env)
    env = os.environ.copy()
    env.update(compose_env)

    try:
        subprocess.run(
            f"docker compose -f '{COMPOSE_FILE}' down -v",
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        msg = (
            "Docker compose down failed:\n"
            f"  - command: '{err.cmd}'\n"
            f"  - args: {err.args}\n"
            f"  - return code: {err.returncode}\n"
            f"  - output:\n{err.stdout}\n"
        )
        LOG.error(msg)
    else:
        LOG.debug("Compose stack is down (compose_env=%s).", compose_env)


@pytest.fixture(scope="module")
def compose_env() -> Generator[Mapping[str, str]]:
    """Env vars passed to ``docker compose`` so the rally image tag matches the workspace Rally version."""
    env = {
        "RALLY_VERSION": version.__version__,
        "RALLY_VERSION_TAG": version.__version__,
        "RALLY_DOCKER_IMAGE": os.environ.get("RALLY_DOCKER_IMAGE", "elastic/rally"),
        # Isolate project name per test module to avoid clashing with other compose runs on the same host.
        "COMPOSE_PROJECT_NAME": __name__.replace(".", "_"),
    }
    tear_down_stack(env)
    try:
        yield env
    finally:
        if LOG.isEnabledFor(logging.DEBUG):
            logs = subprocess.run(
                f"docker compose -f '{COMPOSE_FILE}' logs -t",
                capture_output=True,
                shell=True,
                check=False,
                env=env,
            )
            LOG.debug("Containers logs:\n%s", logs.stdout.decode("utf-8"))
        tear_down_stack(env)


def assert_list_tracks_stdout_matches_golden(stdout: str) -> None:
    """``want_stdout`` checker: normalize docker ``list tracks`` stdout and compare to the golden file.

    Writes the golden when the file is missing, when it exists but is empty, or when
    ``RALLY_UPDATE_DOCKER_LIST_TRACKS_GOLDEN=1``. Otherwise reads the existing file and
    ``pytest.fail`` on mismatch.
    """
    golden_path = (Path(__file__).resolve().parent / LIST_TRACKS_GOLDEN_REL_PATH).resolve()
    golden_path.parent.mkdir(parents=True, exist_ok=True)
    normalized_actual = normalize_list_tracks_stdout(stdout)
    force_update = os.environ.get(UPDATE_DOCKER_LIST_TRACKS_GOLDEN_ENV, "") == "1"
    # Only read/compare once we know the file exists and is non-empty; otherwise (re)generate the golden.
    missing = not golden_path.is_file()
    empty = not missing and golden_path.stat().st_size == 0
    if force_update or missing or empty:
        if force_update:
            reason = f"{UPDATE_DOCKER_LIST_TRACKS_GOLDEN_ENV}=1"
        elif missing:
            reason = "golden file is missing"
        else:
            reason = "golden file is empty"
        LOG.warning("Overwriting list tracks golden (%s): %s", golden_path, reason)
        golden_path.write_text(normalized_actual + "\n", encoding="utf-8")
        LOG.info("Wrote list tracks golden stdout to %s", golden_path)
        return
    normalized_expected = normalize_list_tracks_stdout(golden_path.read_text(encoding="utf-8"))
    if normalized_actual == normalized_expected:
        return
    diff = "\n".join(
        difflib.unified_diff(
            normalized_expected.splitlines(),
            normalized_actual.splitlines(),
            fromfile="golden",
            tofile="actual",
            lineterm="",
        )
    )
    pytest.fail(
        "list tracks stdout does not match golden file "
        f"{golden_path.name}. Set {UPDATE_DOCKER_LIST_TRACKS_GOLDEN_ENV}=1 to refresh.\n{diff}"
    )


def normalize_list_tracks_stdout(stdout: str) -> str:
    """Return stable text for golden comparison: from ``Available tracks:`` onward, without variable footers."""
    text = stdout.replace("\r\n", "\n")
    idx = text.find(_AVAILABLE_TRACKS_MARKER)
    if idx == -1:
        pytest.fail(
            "stdout did not contain the list-tracks marker "
            f"{_AVAILABLE_TRACKS_MARKER!r}; head:\n{text[:800]!r}"
        )
    body = text[idx:]
    # Drop trailing success banner; elapsed time and rule width vary between runs.
    success_idx = body.find("\n[INFO] SUCCESS")
    if success_idx != -1:
        body = body[:success_idx]
    return body.rstrip()


@dataclasses.dataclass
class ComposeCase:
    # Passed as TEST_COMMAND to the rally service (see docker-compose-tests.yml).
    command: str
    want_return_code: int = 0
    # Exact string match, or a callable that validates ``result.stdout`` (e.g. golden file for list tracks).
    want_stdout: str | Callable[[str], None] | None = None
    want_stderr: str | None = None


@cases.cases(
    help=ComposeCase("--help"),
    list_tracks=ComposeCase("list tracks", want_stdout=assert_list_tracks_stdout_matches_golden),
    race=ComposeCase(_RACE_FLAGS),
    race_explicit_esrally=ComposeCase(f"esrally {_RACE_FLAGS}"),
)
def test_docker_compose(
    case: ComposeCase,
    compose_env: Mapping[str, str],
) -> None:
    """Run one rally subcommand inside the compose-defined rally container (with ES dependency)."""
    LOG.info("Running rally with 'docker compose', command='%s', env=%r", case.command, compose_env)
    env = os.environ.copy()
    env.update(compose_env)
    env["TEST_COMMAND"] = case.command
    try:
        # ``run`` starts dependencies (es01), runs the rally service once, then tears down that one-off container.
        result = subprocess.run(
            f"docker compose -f '{COMPOSE_FILE}' run --remove-orphans rally",
            env=env,
            capture_output=True,
            text=True,
            check=case.want_return_code == 0,
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        pytest.fail(
            "Docker compose run failed:\n"
            f"  - command: {err.cmd}\n"
            f"  - args: {err.args}\n"
            f"  - return code: {err.returncode}\n"
            f"  - stdout:\n{err.stdout}\n"
            f"  - stderr:\n{err.stderr}\n"
        )

    LOG.debug("Docker compose up succeeded. STDOUT:\n%s", result.stdout)
    assert result.returncode == case.want_return_code
    if case.want_stdout is not None:
        if callable(case.want_stdout):
            # Custom assertion path (golden files, partial matchers, etc.).
            case.want_stdout(result.stdout)
        else:
            assert result.stdout == case.want_stdout
    if case.want_stderr is not None:
        assert result.stderr == case.want_stderr
