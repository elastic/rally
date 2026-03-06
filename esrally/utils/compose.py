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

from esrally import config, types

COMPOSE_SERVICE = os.environ.get("RALLY_COMPOSE_SERVICE", "rally")

LOG = logging.getLogger(__name__)


class ComposeConfig(config.Config):

    @property
    def compose_cmd(self) -> list[str]:
        cmd = self.opts(
            section="compose", key="compose.cmd", default_value=os.environ.get("RALLY_COMPOSE_COMMAND", "docker compose"), mandatory=False
        )
        if isinstance(cmd, str):
            cmd = shlex.split(cmd)
        if not isinstance(cmd, list):
            raise TypeError(f"Expected compose.cmd to be a string or list of strings, but got [{type(cmd).__name__}]")
        return cmd

    DEFAULT_COMPOSE_SERVICE = os.environ.get("RALLY_COMPOSE_SERVICE", "rally")

    @property
    def compose_service(self) -> str:
        return self.opts(section="compose", key="compose.service", default_value=self.DEFAULT_COMPOSE_SERVICE, mandatory=False)

    DEFAULT_RALLY_COMMAND = os.environ.get("RALLY_COMMAND", "esrally")

    @property
    def rally_command(self) -> list[str]:
        return shlex.split(
            self.opts(section="compose", key="compose.rally_command", default_value=self.DEFAULT_RALLY_COMMAND, mandatory=False)
        )

    DEFAULT_SRC_DIR = os.environ.get("RALLY_SRC_DIR", os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

    def rally_src_dir(self) -> str:
        return self.opts(section="compose", key="compose.rally_src_dir", default_value=self.DEFAULT_SRC_DIR, mandatory=False)

    DEFAULT_COMPOSE_FILE = os.environ.get("RALLY_COMPOSE_FILE", os.path.join(DEFAULT_SRC_DIR, "compose.yaml"))

    @property
    def compose_file(self) -> str:
        return self.opts(
            section="compose",
            key="compose.file",
            default_value=os.environ.get("RALLY_COMPOSE_FILE", self.DEFAULT_COMPOSE_FILE),
            mandatory=False,
        )

    @property
    def compose_dir(self) -> str:
        return self.opts(section="compose", key="compose.dir", default_value=os.path.dirname(self.compose_file), mandatory=False)


def _compose_command(
    cfg: ComposeConfig,
    command: str,
    args: list[str] | None = None,
    *,
    compose_cmd: list[str] | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None,
    compose_service: str | None = None,
) -> list[str]:
    compose_cmd = compose_cmd or cfg.compose_cmd
    compose_options = compose_options or []
    compose_file = compose_file or cfg.compose_file
    if compose_file:
        compose_cmd += ["--file", compose_file]
    compose_service = compose_service or cfg.compose_service
    args = args or []
    return compose_cmd + [command] + compose_options + [compose_service] + args


def run_compose(
    command: str,
    args: list[str] | None = None,
    *,
    cfg: types.Config = None,
    compose_dir: str | None = None,
    compose_cmd: list[str] | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    compose_service: str | None = None,
    compose_logger: logging.Logger = LOG,
    check: bool = True,
    **kwargs,
) -> subprocess.CompletedProcess:
    cfg = ComposeConfig.from_config(cfg)
    cmd = _compose_command(
        cfg,
        command,
        args,
        compose_cmd=compose_cmd,
        compose_options=compose_options,
        compose_file=compose_file,
        compose_service=compose_service,
    )
    compose_dir = compose_dir or cfg.compose_dir
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("cwd", compose_dir)
    try:
        LOG.debug("Running compose command: %s", cmd)
        result = subprocess.run(cmd, check=check, **kwargs)
    except subprocess.CalledProcessError as e:
        LOG.warning(
            "Compose command returned nonzero exit status (%s): %s\nstdout: %s\nstderr: %s\n",
            e.returncode,
            e,
            e.stdout.decode("utf-8") if e.stdout else None,
            e.stderr.decode("utf-8") if e.stderr else None,
            exc_info=compose_logger.isEnabledFor(logging.DEBUG),
        )
        raise e
    if compose_logger.isEnabledFor(logging.DEBUG):
        compose_logger.debug(
            "Compose command finished with exit code %s.\nstdout: %s\nstderr: %s\n",
            result.returncode,
            result.stdout.decode("utf-8") if result.stdout else None,
            result.stderr.decode("utf-8") if result.stderr else None,
        )
    return result


def run(
    args: list[str] | None = None,
    *,
    cfg: types.Config = None,
    compose_cmd: list[str] | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    compose_remove: bool = True,
    compose_service: str | None = None,
    **kwargs,
) -> subprocess.CompletedProcess:
    compose_options = compose_options or []
    if compose_remove:
        compose_options += ["--rm"]
    return run_compose(
        "run",
        args,
        cfg=cfg,
        compose_cmd=compose_cmd,
        compose_options=compose_options,
        compose_file=compose_file,
        compose_service=compose_service,
        **kwargs,
    )


def build(
    *,
    cfg: types.Config = None,
    compose_cmd: list[str] | None = None,
    compose_options: list[str] | None = None,
    compose_file: str | None = None,
    compose_service: str | None = None,
    **kwargs,
) -> subprocess.CompletedProcess:
    compose_options = compose_options or []
    return run_compose(
        "build",
        cfg=cfg,
        compose_cmd=compose_cmd,
        compose_options=compose_options,
        compose_file=compose_file,
        compose_service=compose_service,
        **kwargs,
    )


def _rally_command(
    command: str,
    args: list[str] | None = None,
    *,
    cfg: types.Config = None,
    rally_command: list[str] | None = None,
    rally_options: list[str] | None = None,
) -> list[str]:
    cfg = ComposeConfig.from_config(cfg)
    rally_command = rally_command or cfg.rally_command
    rally_options = rally_options or []
    args = args or []
    return [command] + rally_options + args


def build_rally() -> None:
    build(compose_service="rally")


def run_rally(
    command: str,
    args: list[str] | None = None,
    *,
    cfg: types.Config = None,
    rally_command: list[str] | None = None,
    rally_options: list[str] | None = None,
    rally_logger: logging.Logger = LOG,
    **kwargs,
) -> subprocess.CompletedProcess:
    cmd = _rally_command(command, args, cfg=cfg, rally_command=rally_command, rally_options=rally_options)
    rally_logger.info("Running rally command: %s", cmd)
    try:
        return run(cmd, **kwargs)
    except subprocess.CalledProcessError as e:
        rally_logger.warning(
            "Rally command %r returned nonzero exit status (%s): %s\nstdout: %s\nstderr: %s\n",
            cmd,
            e.returncode,
            e,
            e.stdout.decode("utf-8") if e.stdout else None,
            e.stderr.decode("utf-8") if e.stderr else None,
            exc_info=rally_logger.isEnabledFor(logging.DEBUG),
        )
        raise e


def list_tracks(**kwargs) -> list[dict[str, str]]:
    result = run_rally("list", ["tracks"], stdout=subprocess.PIPE, **kwargs)
    return [t.strip() for t in result.stdout.decode("utf-8").splitlines() if t.strip()]


def race(
    track_name: str,
    *,
    rally_options: list[str] | None = None,
    test_mode: bool = False,
    target_hosts: list[str] | None = None,
    pipeline: str | None = "benchmark-only",
    elasticsearch_version: str | None = None,
    **kwargs,
) -> None:
    rally_options = rally_options or []
    rally_options += ["--track", track_name]
    if test_mode:
        rally_options += ["--test-mode"]
    if target_hosts:
        rally_options += ["--target-hosts", ",".join(target_hosts)]
    if pipeline:
        rally_options += ["--pipeline", pipeline]
    if elasticsearch_version:
        env = kwargs.setdefault("env", os.environ.copy())
        env["ELASTICSEARCH_VERSION"] = elasticsearch_version
    run_rally("race", rally_options=rally_options, **kwargs)
