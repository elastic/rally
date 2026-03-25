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

import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import it
from esrally.client import EsClientFactory
from esrally.utils.cases import cases

LOG = logging.getLogger(__name__)
# Mirror ``rally.log`` lines from each esrally subprocess to stderr at DEBUG without mixing them into
# ``subprocess.run`` capture (assertions depend on stdout/stderr staying clean of file-log content).
_RALLY_LOG_STDERR = logging.getLogger(f"{__name__}.rally_log_mirror")
RESOURCES: Final[Path] = Path(__file__).resolve().parent / "resources"
# rally-prepare-only-it.ini sets reporting to a closed port (INACCESSIBLE_METRICS_PORT) for prepare-only subprocesses.
# Full races use rally-es-it.ini (ES_IT_CONFIG) with the session metrics Elasticsearch; logs must never show contact to 65531.
PREPARE_ONLY_IT_CONFIG: Final[str] = "prepare-only-it"
# Second-phase full race: real reporting ES (session metrics store port), see rally-es-it.ini.
ES_IT_CONFIG: Final[str] = "es-it"
INACCESSIBLE_METRICS_PORT: Final[str] = "65531"
# Unassigned port for --target-hosts (must differ from INACCESSIBLE_METRICS_PORT so logs stay distinguishable).
INACCESSIBLE_TARGET_PORT: Final[str] = "65530"
INACCESSIBLE_ES_HOST: Final[str] = f"127.0.0.1:{INACCESSIBLE_TARGET_PORT}"


def _ensure_rally_log_stderr_mirror() -> None:
    if _RALLY_LOG_STDERR.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    _RALLY_LOG_STDERR.addHandler(handler)
    _RALLY_LOG_STDERR.setLevel(logging.DEBUG)
    _RALLY_LOG_STDERR.propagate = False


def _debug_emit_new_rally_log_tail(rally_home: Path, prev_end: int) -> int:
    log_path = rally_home / ".rally" / "logs" / "rally.log"
    if not log_path.is_file():
        return prev_end
    data = log_path.read_bytes()
    if len(data) < prev_end:
        chunk = data
    else:
        chunk = data[prev_end:]
    text = chunk.decode("utf-8", errors="replace")
    for line in text.splitlines():
        _RALLY_LOG_STDERR.debug("%s", line)
    return len(data)


def _wait_for_cluster_status_at_least_yellow(host: str, http_port: int, *, timeout_sec: float = 120.0, poll_interval_sec: float = 2.0) -> None:
    """
    Poll ``/_cluster/health`` until status is yellow or green (single-node clusters are often yellow).

    Used before the second subprocess for ``benchmark-only`` when the race targets the session Elasticsearch.
    """
    es = EsClientFactory(hosts=[{"host": host, "port": http_port}], client_options={}).create()
    deadline = time.perf_counter() + timeout_sec
    last_exc: Exception | None = None
    while time.perf_counter() < deadline:
        try:
            health = es.cluster.health()
            status = health.get("status", "red")
            if status in ("yellow", "green"):
                LOG.info("Elasticsearch at %s:%s cluster health is %s", host, http_port, status)
                return
        except Exception as exc:
            last_exc = exc
            LOG.debug("cluster.health not ready yet: %s", exc)
        time.sleep(poll_interval_sec)
    msg = f"Elasticsearch at {host}:{http_port} did not reach yellow/green within {timeout_sec}s"
    if last_exc is not None:
        raise TimeoutError(msg) from last_exc
    raise TimeoutError(msg)


def _esrally_cmd_prefix() -> list[str]:
    resolved = shutil.which("esrally")
    if resolved:
        return [resolved]
    return [sys.executable, "-m", "esrally.rally"]


@dataclass(frozen=True)
class PrepareOnlyRaceCase:
    """One table row: pipeline and optional ``--distribution-version`` string."""

    # If set, passed to Rally as ``--pipeline``; if ``None``, that flag is omitted.
    pipeline: str | None = None
    
    # If set, passed to Rally as ``--distribution-version``; if ``None``, that flag is omitted.
    distribution_version: str | None = None

    # It will assert that the target Elasticsearch cluster is prepared in the first subprocess and executed in the second one.
    want_cluster_prepared: bool = False

    def run_rally_subprocess(
        self,
        rally_home: Path,
        prepare_only: bool,
        *,
        target_hosts: str | None = None,
        enable_assertions: bool = False,
        configuration_name: str | None = None,
    ) -> tuple[int, str]:
        """
        Run ``esrally race`` for this case in a subprocess with ``RALLY_HOME`` set.

        :param prepare_only: If True, pass ``--prepare-only`` and ``--target-hosts`` to a closed port (unless
            ``target_hosts`` overrides). If False, omit ``--target-hosts`` when ``target_hosts`` is None so Rally
            applies pipeline defaults (e.g. provisioned cluster); for ``benchmark-only``, pass the session cluster
            via ``target_hosts``.
        :param configuration_name: Rally config name under ``RALLY_HOME/.rally`` (default: ``prepare-only-it``).
        """
        cfg_name = configuration_name if configuration_name is not None else PREPARE_ONLY_IT_CONFIG
        argv: list[str] = ["esrally", "race"]
        if prepare_only:
            argv.append("--prepare-only")
        argv.extend(
            [
                "--test-mode",
                "--track=geonames",
                "--challenge=append-no-conflicts-index-only",
            ]
        )
        if prepare_only:
            hosts = target_hosts if target_hosts is not None else INACCESSIBLE_ES_HOST
            argv.append(f"--target-hosts={hosts}")
        elif target_hosts is not None:
            argv.append(f"--target-hosts={target_hosts}")
        argv.extend(
            [
                "--kill-running-processes",
                "--on-error=abort",
                f"--configuration-name={cfg_name}",
            ]
        )
        if self.pipeline is not None:
            argv.append(f"--pipeline={self.pipeline}")
        if self.distribution_version is not None:
            argv.append(f"--distribution-version={self.distribution_version}")
        if enable_assertions:
            argv.append("--enable-assertions")

        if argv[:1] == ["esrally"]:
            argv = _esrally_cmd_prefix() + argv[1:]
        cmd: str = shlex.join(argv)
        env: dict[str, str] = dict(os.environ)
        env["RALLY_HOME"] = str(rally_home)
        _ensure_rally_log_stderr_mirror()
        log_path = rally_home / ".rally" / "logs" / "rally.log"
        rally_log_end = log_path.stat().st_size if log_path.is_file() else 0
        LOG.info("Running rally subprocess (shell): %s", cmd)
        completed = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            env=env,
            timeout=900,
            check=False,
        )
        _debug_emit_new_rally_log_tail(rally_home, rally_log_end)
        LOG.info(
            "Rally subprocess finished: returncode=%s\nstdout:\n\t%s\n\nstderr:\n\t%s\n\n",
            completed.returncode,
            "\t\n".join((completed.stdout or "").splitlines()),
            "\t\n".join((completed.stderr or "").splitlines()),
        )
        out: str = (completed.stdout or "") + (completed.stderr or "")
        return completed.returncode, out


def _install_prepare_only_test_config(rally_home: Path) -> None:
    """
    Install two Rally configs under ``RALLY_HOME``:

    * ``prepare-only-it`` — fake reporting endpoint (unreachable port) for ``--prepare-only`` runs.
    * ``es-it`` — real reporting Elasticsearch (session metrics store) for full races.
    """
    rally_confdir = rally_home / ".rally"
    rally_confdir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(RESOURCES / f"rally-{PREPARE_ONLY_IT_CONFIG}.ini", rally_confdir / f"rally-{PREPARE_ONLY_IT_CONFIG}.ini")
    shutil.copyfile(RESOURCES / f"rally-{ES_IT_CONFIG}.ini", rally_confdir / f"rally-{ES_IT_CONFIG}.ini")


def _read_rally_log(rally_home: Path) -> str:
    log_path = rally_home / ".rally" / "logs" / "rally.log"
    if not log_path.is_file():
        return ""
    return log_path.read_text(encoding="utf-8", errors="replace")


# Substrings that indicate track corpus transfer (download or progress); second race should not repeat them on stdout/stderr.
_TRACK_CORPUS_TRANSFER_STDOUT_MARKERS: Final[tuple[str, ...]] = (
    "Downloading data from",
    "[INFO] Downloading track data",
    "Downloaded data from",
)


def _corpus_transfer_events_in_log(log: str) -> int:
    return sum(log.count(s) for s in _TRACK_CORPUS_TRANSFER_STDOUT_MARKERS)


_PREPARE_ONLY_STDOUT_PHRASE: Final[str] = "prepare-only mode"


def _subprocess_output_mentions_prepare_only(combined_stdout_stderr: str) -> bool:
    """True if any line of captured subprocess output contains the prepare-only banner phrase (case-insensitive)."""
    needle = _PREPARE_ONLY_STDOUT_PHRASE.casefold()
    return any(needle in line.casefold() for line in combined_stdout_stderr.splitlines())


@cases(
    default=PrepareOnlyRaceCase(),
    benchmark_only=PrepareOnlyRaceCase(pipeline="benchmark-only"),
    from_distribution_=PrepareOnlyRaceCase(pipeline="from-distribution", distribution_version=it.DISTRIBUTIONS[-1], want_cluster_prepared=True),
    with_distribution_version=PrepareOnlyRaceCase(distribution_version=it.DISTRIBUTIONS[-1], want_cluster_prepared=True),
    from_distribution_with_distribution_version=PrepareOnlyRaceCase("from-distribution", it.DISTRIBUTIONS[-1], want_cluster_prepared=True),
    from_sources=PrepareOnlyRaceCase(pipeline="from-sources", want_cluster_prepared=True),
)
def test_prepare_only(case: PrepareOnlyRaceCase, tmp_path: Path) -> None:
    """
    Every row runs two subprocesses in the same ``RALLY_HOME``:

    1. ``--prepare-only`` with ``--configuration-name=prepare-only-it``: reporting points at an unreachable
       Elasticsearch (``INACCESSIBLE_METRICS_PORT``). That port must never appear in ``rally.log`` after this step.
       Rally also forces in-memory reporting in-process for prepare-only, but the on-disk config stays a decoy.
       ``--target-hosts`` uses another closed port so an accidental probe would fail or stall.

    2. Full ``race`` with ``--configuration-name=es-it``: reporting uses the live session metrics Elasticsearch
       (``rally-es-it.ini``). Reuses the same track cache: no repeated corpus-transfer lines on stdout/stderr and the
       same corpus-transfer event count in ``rally.log`` as after step 1. For ``benchmark-only``, the second run targets
       the session cluster from ``it/conftest.py`` and waits for cluster health yellow/green before starting. Other
       pipelines omit ``--target-hosts`` so Rally uses pipeline defaults. ``INACCESSIBLE_METRICS_PORT`` must never appear
       in the log (no contact to the decoy endpoint); logging may reference the real metrics port (e.g. 10200).

    New lines appended to ``rally.log`` after each subprocess are emitted as DEBUG on stderr (logger
    ``it.prepare_only_test.rally_log_mirror``) so CI or local runs show file-equivalent tracing without polluting
    captured subprocess output.
    """
    _install_prepare_only_test_config(tmp_path)

    rc1, out1 = case.run_rally_subprocess(tmp_path, True)
    log_after_prepare = _read_rally_log(tmp_path)

    assert rc1 == 0, f"prepare-only failed ({rc1})\n{out1}\n--- log ---\n{log_after_prepare}"
    assert _subprocess_output_mentions_prepare_only(out1), (
        f"expected {_PREPARE_ONLY_STDOUT_PHRASE!r} on subprocess stdout/stderr:\n{out1}"
    )
    assert "Racing on track" not in out1
    assert "Telling driver to start benchmark." not in log_after_prepare
    assert "Asking mechanic to start the engine." not in log_after_prepare
    assert "Checking if REST API is available." not in log_after_prepare
    assert "Automatically derived distribution flavor" not in log_after_prepare
    assert INACCESSIBLE_METRICS_PORT not in log_after_prepare, "prepare-only must not contact configured metrics Elasticsearch"
    transfers_after_prepare = _corpus_transfer_events_in_log(log_after_prepare)
    assert transfers_after_prepare > 0, (
        "expected at least one corpus download/prepare log event after prepare-only; "
        f"log excerpt:\n{log_after_prepare[-8000:]}"
    )

    if case.pipeline == "benchmark-only":
        from it.conftest import ES_METRICS_STORE
        # TODO: Start a new ES cluster for this test."
        LOG.warning("Second subprocess targets the Elasticsearch metric store because it is missing one as target hosts.")
        session_target = f"127.0.0.1:{ES_METRICS_STORE.cluster.http_port}"
        _wait_for_cluster_status_at_least_yellow("127.0.0.1", ES_METRICS_STORE.cluster.http_port)
        rc2, out2 = case.run_rally_subprocess(
            tmp_path,
            False,
            target_hosts=session_target,
            enable_assertions=True,
            configuration_name=ES_IT_CONFIG,
        )
    else:
        rc2, out2 = case.run_rally_subprocess(
            tmp_path, False, enable_assertions=True, configuration_name=ES_IT_CONFIG
        )

    log_after_full = _read_rally_log(tmp_path)

    assert rc2 == 0, f"full race failed ({rc2})\n{out2}\n--- log ---\n{log_after_full}"
    assert INACCESSIBLE_METRICS_PORT not in log_after_full, (
        "full race must not contact the decoy reporting endpoint (prepare-only-it.ini); es-it metrics traffic may log the real port"
    )
    assert "Racing on track" in out2, f"expected benchmark console banner\n{out2}"
    for marker in _TRACK_CORPUS_TRANSFER_STDOUT_MARKERS:
        assert marker not in out2, f"second run must not repeat track corpus transfer; found {marker!r} in:\n{out2}"

    assert _corpus_transfer_events_in_log(log_after_full) == transfers_after_prepare, (
        "second run must not add corpus download/prepare log lines; "
        f"before={transfers_after_prepare} after={_corpus_transfer_events_in_log(log_after_full)}"
    )
