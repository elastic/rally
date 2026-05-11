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
import base64
import logging
import os
import posixpath
import re
import secrets
import shlex
import subprocess
from pathlib import Path
from typing import Any, BinaryIO

from esrally import config, paths, types

LOG = logging.getLogger(__name__)


def _default_rally_compose_run_name() -> str:
    """Return a Docker-safe unique ``docker compose run --name`` (``rally_`` + 8 URL-safe base64 chars)."""
    raw = base64.urlsafe_b64encode(secrets.token_bytes(9)).decode("ascii").rstrip("=")
    return f"rally_{raw[:8]}"


class ComposeConfig(config.Config):

    @property
    def compose_cmd(self) -> list[str]:
        cmd = self.opts(
            section="compose", key="compose.cmd", default_value=os.environ.get("COMPOSE_COMMAND", "docker compose"), mandatory=False
        )
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if not isinstance(cmd, list):
            raise TypeError(f"Expected compose.cmd to be a string or list of strings, but got [{type(cmd).__name__}]")
        return cmd

    @staticmethod
    def _default_compose_file() -> str:
        return os.environ.get(
            "RALLY_COMPOSE_FILE",
            os.path.join(os.path.dirname(__file__), "resources", "compose.yaml"),
        )

    @property
    def compose_file(self) -> str:
        return self.opts(section="compose", key="compose.file", default_value=self._default_compose_file(), mandatory=False)

    @property
    def compose_dir(self) -> str:
        return self.opts(
            section="compose",
            key="compose.dir",
            default_value=os.path.dirname(paths.rally_root()),
            mandatory=False,
        )


def decode(output: bytes | None) -> str:
    if not output:
        return ""
    return "\n  " + "\n  ".join([line.rstrip() for line in output.decode("utf-8").splitlines()]) + "\n"


def parse_tabulate_simple_table(text: str) -> list[dict[str, str]]:
    """Parse stdout from Rally ``list tracks`` (tabulate *simple* layout) into one dict per data row.

    Column keys are the header cell strings from the table header row.
    """
    lines = [ln.rstrip("\r\n") for ln in text.splitlines()]
    sep_idx: int | None = None
    for i, line in enumerate(lines):
        s = line.strip()
        if s and "-" in s and all(c in "- " for c in s):
            sep_idx = i
            break
    if sep_idx is None or sep_idx < 1:
        return []
    header_line = lines[sep_idx - 1]
    sep_line = lines[sep_idx]
    ranges = [(m.start(), m.end()) for m in re.finditer(r"-+", sep_line)]
    if not ranges:
        return []
    headers = [header_line[a:b].strip() for a, b in ranges]
    rows: list[dict[str, str]] = []
    for line in lines[sep_idx + 1 :]:
        if not line.strip():
            continue
        ls = line.strip()
        if ls and "-" in ls and all(c in "- " for c in ls):
            break
        cells = []
        for a, b in ranges:
            chunk = line[a:b] if a < len(line) else ""
            cells.append(chunk.strip())
        if any(cells):
            rows.append(dict(zip(headers, cells)))
    return rows


def _compose_options_set_project_name(compose_options: list[str] | None) -> bool:
    """True if ``compose_options`` already passes ``-p`` / ``--project-name`` to the CLI."""
    if not compose_options:
        return False
    return any(opt in ("-p", "--project-name") for opt in compose_options)


def _strip_compose_run_name_options(opts: list[str]) -> list[str]:
    """Remove ``--name`` / value pairs from ``docker compose run`` option args (so a single name can win)."""
    out: list[str] = []
    i = 0
    while i < len(opts):
        opt = opts[i]
        if opt == "--name":
            i += 1
            if i < len(opts):
                i += 1
            continue
        if opt.startswith("--name="):
            i += 1
            continue
        out.append(opt)
        i += 1
    return out


def run_compose(
    command: str,
    service: str | None = None,
    args: list[str] | None = None,
    *,
    env: dict[str, str] | None = None,
    cfg: types.Config | None = None,
    compose_dir: str | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    logger: logging.Logger = LOG,
    check: bool = True,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    cfg = ComposeConfig.from_config(cfg)
    cmd = []
    compose_file = compose_file or cfg.compose_file
    if compose_file:
        cmd += ["--file", compose_file]
    cmd += [command]
    if compose_options:
        cmd += compose_options
    if service:
        cmd += [service]
    if args:
        cmd += args

    compose_dir = compose_dir or cfg.compose_dir
    if not kwargs.get("capture_output"):
        kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("cwd", compose_dir)
    run_env = os.environ.copy()
    run_env.update(env or {})
    # Absolute repo root: Compose resolves relative build.context from the compose file path, not cwd.
    run_env["RALLY_DOCKER_DIR"] = compose_dir
    project = (run_env.get("COMPOSE_PROJECT_NAME") or "").strip()
    if project and not _compose_options_set_project_name(compose_options):
        cmd = ["--project-name", project] + cmd
    try:
        logger.debug("Running compose command: %s", cmd)
        result = subprocess.run(cfg.compose_cmd + cmd, check=check, env=run_env, **kwargs)
    except subprocess.CalledProcessError as e:
        logger.error(
            "Compose command returned nonzero exit status (%s): %s\nstdout:%s\nstderr:%s",
            e.returncode,
            e,
            decode(e.stdout),
            decode(e.stderr),
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        raise e

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Compose command finished with exit code %s.\nstdout:%s\nstderr:%s",
            result.returncode,
            decode(result.stdout),
            decode(result.stderr),
        )
    return result


def spawn_compose_logs_follow(  # pylint: disable=consider-using-with
    path: Path,
    *,
    cfg: types.Config | None = None,
    compose_file: str | None = None,
    compose_dir: str | None = None,
    env: dict[str, str] | None = None,
    logger: logging.Logger = LOG,
) -> tuple[subprocess.Popen, BinaryIO]:
    """Start ``docker compose logs -f`` for the current project; write merged service output to ``path``.

    Caller must stop the returned ``subprocess.Popen`` and close the file handle (e.g. in a pytest
    fixture ``finally``). ``stderr`` is discarded to avoid blocking on a full pipe while following.
    Intentionally not using ``with`` so the subprocess and file outlive this function.
    """
    cfg = ComposeConfig.from_config(cfg)
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(path, "wb")
    cmd: list[str] = []
    cf = compose_file or cfg.compose_file
    if cf:
        cmd += ["--file", cf]
    cmd += ["logs", "-f", "--timestamps", "--no-color"]
    cdir = compose_dir or cfg.compose_dir
    run_env = os.environ.copy()
    run_env.update(env or {})
    run_env["RALLY_DOCKER_DIR"] = cdir
    project = (run_env.get("COMPOSE_PROJECT_NAME") or "").strip()
    if project and not _compose_options_set_project_name(None):
        cmd = ["--project-name", project] + cmd
    full_cmd = cfg.compose_cmd + cmd
    logger.debug("Spawning compose logs follow: %s", full_cmd)
    try:
        proc = subprocess.Popen(
            full_cmd,
            stdout=log_file,
            stderr=subprocess.DEVNULL,
            cwd=cdir,
            env=run_env,
        )
    except OSError:
        log_file.close()
        raise
    return proc, log_file


def _cleanup_compose_run_service(
    service: str | None,
    *,
    cfg: types.Config | None = None,
    compose_file: str | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> None:
    """Tear down containers for a service after ``docker compose run``.

    ``docker compose rm`` and ``compose stop`` **exclude** one-off containers (``compose run`` creates
    ``*-run-*`` names with ``com.docker.compose.oneoff=True``; see Docker Compose ``oneOffExclude`` in
    ``pkg/compose/remove.go`` / ``stop.go``). So ``rm --stop`` never sees orphaned run containers when the
    CLI exits early (e.g. ``subprocess`` ``timeout``).

    ``docker compose kill`` **includes** one-off containers (``pkg/compose/kill.go``). We kill, then remove
    IDs from ``compose ps -a -q`` (``ps --all`` includes one-off), via ``docker rm -f``.
    """
    run_kwargs = {k: kwargs[k] for k in ("compose_dir", "env") if k in kwargs}
    run_compose("kill", service, check=False, cfg=cfg, compose_file=compose_file, logger=logger, **run_kwargs)
    listed = run_compose(
        "ps",
        service,
        compose_options=["-a", "-q"],
        check=False,
        stdout=subprocess.PIPE,
        cfg=cfg,
        compose_file=compose_file,
        logger=logger,
        **run_kwargs,
    )
    raw = listed.stdout or b""
    ids = [line.strip() for line in raw.decode("utf-8").splitlines() if line.strip()]
    if not ids:
        return
    rm_env = os.environ.copy()
    extra_env = run_kwargs.get("env")
    if extra_env:
        rm_env.update(extra_env)
    rm_result = subprocess.run(["docker", "rm", "-f", *ids], check=False, capture_output=True, env=rm_env)
    if rm_result.returncode != 0:
        logger.warning(
            "docker rm -f after compose run cleanup failed (service=%s, returncode=%s): stderr:%s",
            service,
            rm_result.returncode,
            decode(rm_result.stderr),
        )


def run_service(
    service: str | None = None,
    args: list[str] | None = None,
    *,
    remove: bool = True,
    name: str | None = None,
    cfg: types.Config | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    """Run a one-off ``docker compose run`` for ``service``."""
    compose_options = list(compose_options or [])
    if name is not None:
        compose_options = _strip_compose_run_name_options(compose_options)
        compose_options = ["--name", name] + compose_options
    if remove:
        compose_options += ["--rm"]
    logger.info("Run service '%s' (options=%s)", service, compose_options)
    try:
        result = run_compose(
            "run", service, args, cfg=cfg, compose_options=compose_options, compose_file=compose_file, logger=logger, **kwargs
        )
        logger.info("Ran service '%s'.", service)
        return result
    finally:
        if remove:
            # When ``remove=True`` (default), ``docker compose run`` uses ``--rm``, but if the parent hits
            # ``subprocess`` ``timeout`` or is killed, the CLI may exit before ``--rm`` runs.
            #
            # ``docker compose rm --stop`` does **not** remove ``compose run`` one-off containers: Compose
            # filters them out (``oneOffExclude``). So we use ``_cleanup_compose_run_service`` instead:
            # ``compose kill`` (includes one-offs), then ``compose ps -a -q`` and ``docker rm -f``.

            cleanup_kw = {k: kwargs[k] for k in ("compose_dir", "env") if k in kwargs}
            try:
                _cleanup_compose_run_service(
                    service,
                    cfg=cfg,
                    compose_file=compose_file,
                    logger=logger,
                    **cleanup_kw,
                )
            except Exception as exc:
                logger.warning("Best-effort cleanup after compose run failed for service=%s: %s", service, exc)


def build_image(
    service: str | None = None,
    *,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    logger.info("Build image (service=%s).", service)
    result = run_compose("build", service=service, logger=logger, **kwargs)
    logger.info("Built image.")
    return result


def start_service(
    service: str | None = None,
    *,
    detach: bool = False,
    compose_options: list[str] | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    if detach:
        compose_options = (compose_options or []) + ["--detach"]

    logger.info("Starting service '%s' (options=%s).", service, compose_options)
    result = run_compose("up", service, compose_options=compose_options, logger=logger, **kwargs)
    logger.info("Started service.")
    return result


def teardown_project(
    *,
    cfg: types.Config | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> None:
    """Best-effort removal of all project containers, including ``compose run`` one-offs, then ``down``.

    Use when exiting abruptly (e.g. ``KeyboardInterrupt``) so fixture teardown or ``run_service`` ``finally``
    may not run: ``compose kill`` / ``docker rm`` covers one-off containers that ``compose rm`` skips,
    then ``docker compose down --volumes --remove-orphans`` drops the rest of the stack.
    """
    run_kwargs = {k: kwargs[k] for k in ("compose_dir", "env", "compose_file") if k in kwargs}
    try:
        _cleanup_compose_run_service(None, cfg=cfg, logger=logger, **run_kwargs)
    except Exception as exc:
        logger.warning("Best-effort cleanup of compose run containers failed: %s", exc)
    try:
        run_compose("down", args=["--volumes", "--remove-orphans"], check=False, cfg=cfg, logger=logger, **run_kwargs)
    except Exception as exc:
        logger.warning("Best-effort compose down failed: %s", exc)


def remove_service(
    service: str | None = None,
    *,
    force: bool = False,
    stop: bool = False,
    check: bool = False,
    volumes: bool = False,
    logger: logging.Logger = LOG,
    compose_options: list[str] | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    """Remove containers for a compose service via ``docker compose rm``.

    Intended for regular service containers (e.g. from ``compose up``). **Does not** remove one-off
    containers created by ``docker compose run`` (Compose applies ``oneOffExclude`` for ``rm``/``stop``);
    ``run_service`` uses ``_cleanup_compose_run_service`` for those.

    ``stop``: if True, pass ``--stop`` so running containers are stopped before removal.

    With ``check=False`` (common for teardown), a non-zero exit from ``docker compose rm`` does
    not raise; callers should treat this as best-effort idempotent cleanup.
    """
    compose_options = compose_options or []
    if force:
        compose_options += ["--force"]
    if stop:
        compose_options += ["--stop"]
    if volumes:
        compose_options += ["--volumes"]
    logger.info("Removing service '%s' (options=%s)", service, compose_options)
    result = run_compose("rm", service, compose_options=compose_options, check=check, logger=logger, **kwargs)
    logger.info("Removed service.")
    return result


def run_rally(
    command: str,
    args: list[str] | None = None,
    *,
    rally_options: list[str] | None = None,
    name: str | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    rally_options = rally_options or []
    args = args or []
    rally_args = [command] + rally_options + args
    effective_name = name if name is not None else _default_rally_compose_run_name()
    logger.info("Running rally (args=%s).", rally_args)
    try:
        return run_service("rally", rally_args, logger=logger, name=effective_name, **kwargs)
    except subprocess.CalledProcessError as e:
        if e.stderr is None and e.stdout:
            logger.error(
                "Rally returned nonzero exit status (%s): %s\ncombined output:%s",
                e.returncode,
                e,
                decode(e.stdout),
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
        else:
            logger.error(
                "Rally returned nonzero exit status (%s): %s\nstdout:%s\nstderr:%s",
                e.returncode,
                e,
                decode(e.stdout),
                decode(e.stderr),
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
        raise e
    finally:
        logger.info("Terminated rally.")


def list_tracks(*, logger: logging.Logger = LOG, **kwargs: Any) -> list[dict[str, str]]:
    logger.info("Listing rally tracks.")
    result = run_rally("list", ["tracks"], stdout=subprocess.PIPE, logger=logger, **kwargs)
    tracks = parse_tabulate_simple_table(result.stdout.decode("utf-8"))
    logger.info("Listed %d rally track(s).", len(tracks))
    return tracks


def rally_race(
    track_name: str,
    *,
    test_mode: bool = False,
    target_hosts: list[str] | None = None,
    pipeline: str | None = "benchmark-only",
    challenge: str | None = None,
    rally_options: list[str] | None = None,
    tracks_root: str | None = None,
    remove: bool = True,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> None:
    """Run ``docker compose run rally …`` for a track race.

    ``remove`` defaults to ``True`` (``--rm`` and ``run_service`` cleanup). Callers may set ``False`` to
    defer one-off removal (unusual; it/tracks uses ``True`` and captures logs via ``docker compose logs -f``).

    Extra keyword arguments are forwarded to ``run_rally`` (e.g. ``name=...`` sets ``docker compose run --name``
    explicitly; otherwise ``run_rally`` picks ``rally_`` plus eight URL-safe base64 characters so parallel runs
    do not clash on the Docker daemon).
    """
    rally_options = rally_options or []
    root = tracks_root if tracks_root is not None else os.environ.get("RALLY_IT_TRACKS_ROOT")
    if root:
        # track_name is the path relative to the rally-tracks repo root inside the container.
        rally_options += ["--track-path", posixpath.join(root, track_name)]
    else:
        rally_options += ["--track", track_name]
    if test_mode:
        rally_options += ["--test-mode"]
    if target_hosts:
        rally_options += ["--target-hosts", ",".join(target_hosts)]
    if pipeline:
        rally_options += ["--pipeline", pipeline]
    if challenge:
        rally_options += ["--challenge", challenge]
    logger.info("Running rally race (options=%s, ES_VERSION=%s).", rally_options, os.environ["ES_VERSION"])
    run_rally(
        "race",
        rally_options=rally_options,
        logger=logger,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        remove=remove,
        **kwargs,
    )
    logger.info("Terminated rally race.")


def start_elasticsearch(service: str, detach: bool = True, *, logger: logging.Logger = LOG, **kwargs: Any) -> None:
    logger.info("Starting Elasticsearch (service=%s, ES_VERSION=%s).", service, os.environ["ES_VERSION"])
    start_service(service, detach=detach, logger=logger, **kwargs)
    logger.info("Started Elasticsearch server.")
