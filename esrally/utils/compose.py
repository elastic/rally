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
import subprocess
from typing import Any

from esrally import config, paths, types

LOG = logging.getLogger(__name__)


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

    DEFAULT_COMPOSE_FILE = os.environ.get("COMPOSE_FILE", os.path.join(os.path.dirname(paths.rally_root()), "compose.yaml"))

    @property
    def compose_file(self) -> str:
        return self.opts(section="compose", key="compose.file", default_value=self.DEFAULT_COMPOSE_FILE, mandatory=False)

    @property
    def compose_dir(self) -> str:
        return self.opts(section="compose", key="compose.dir", default_value=os.path.dirname(self.compose_file), mandatory=False)


def run_compose(
    command: str,
    service: str | None = None,
    args: list[str] | None = None,
    *,
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
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("cwd", compose_dir)
    try:
        logger.debug("Running compose command: %s", cmd)
        result = subprocess.run(cfg.compose_cmd + cmd, check=check, **kwargs)
    except subprocess.CalledProcessError as e:
        logger.warning(
            "Compose command returned nonzero exit status (%s): %s\nstdout: %s\nstderr: %s\n",
            e.returncode,
            e,
            e.stdout.decode("utf-8") if e.stdout else None,
            e.stderr.decode("utf-8") if e.stderr else None,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        raise e
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Compose command finished with exit code %s.\nstdout: %s\nstderr: %s\n",
            result.returncode,
            result.stdout.decode("utf-8") if result.stdout else None,
            result.stderr.decode("utf-8") if result.stderr else None,
        )
    return result


def run_service(
    service: str | None = None,
    args: list[str] | None = None,
    *,
    remove: bool = True,
    cfg: types.Config | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    compose_options = compose_options or []
    if remove:
        compose_options += ["--rm"]
    logger.info("Run service '%s' (options=%s)", service, compose_options)
    result = run_compose("run", service, args, cfg=cfg, compose_options=compose_options, compose_file=compose_file, logger=logger, **kwargs)
    logger.info("Ran service '%s'.", service)
    return result


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


def remove_service(
    service: str | None = None,
    *,
    force: bool = False,
    check: bool = False,
    volumes: bool = False,
    logger: logging.Logger = LOG,
    compose_options: list[str] | None = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    compose_options = compose_options or []
    if force:
        compose_options += ["--force"]
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
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    rally_options = rally_options or []
    args = args or []
    rally_args = [command] + rally_options + args
    logger.info("Running rally (args=%s).", rally_args)
    try:
        return run_service("rally", rally_args, logger=logger, **kwargs)
    except subprocess.CalledProcessError as e:
        logger.error(
            "Rally returned nonzero exit status (%s): %s\nstdout: %s\nstderr: %s\n",
            e.returncode,
            e,
            e.stdout.decode("utf-8") if e.stdout else None,
            e.stderr.decode("utf-8") if e.stderr else None,
            exc_info=logger.isEnabledFor(logging.DEBUG),
        )
        raise e
    finally:
        logger.info("Terminated rally.")


def list_rally_tracks(*, logger: logging.Logger = LOG, **kwargs: Any) -> list[dict[str, str]]:
    logger.info("Listing rally tracks.")
    result = run_rally("list", ["tracks"], stdout=subprocess.PIPE, logger=logger, **kwargs)
    tracks = [t.strip() for t in result.stdout.decode("utf-8").splitlines() if t.strip()]
    logger.info("Listed %d rally track(s).", len(tracks))
    return tracks


def rally_race(
    track_name: str,
    *,
    test_mode: bool = False,
    target_hosts: list[str] | None = None,
    pipeline: str | None = "benchmark-only",
    rally_options: list[str] | None = None,
    logger: logging.Logger = LOG,
    **kwargs: Any,
) -> None:
    rally_options = rally_options or []
    rally_options += ["--track", track_name]
    if test_mode:
        rally_options += ["--test-mode"]
    if target_hosts:
        rally_options += ["--target-hosts", ",".join(target_hosts)]
    if pipeline:
        rally_options += ["--pipeline", pipeline]
    logger.info("Running rally race (options = %s).", rally_options)
    run_rally("race", rally_options=rally_options, logger=logger, **kwargs)
    logger.info("Terminated rally race.")


def start_elasticsearch(
    service: str, version: str | None = None, detach: bool = True, *, logger: logging.Logger = LOG, **kwargs: Any
) -> None:
    if version:
        env = kwargs.setdefault("env", os.environ.copy())
        env["ELASTICSEARCH_VERSION"] = version
    logger.info("Starting Elasticsearch (service=%s, version=%s).", service, version)
    start_service(service, detach=detach, logger=logger, **kwargs)
    logger.info("Started Elasticsearch server.")
