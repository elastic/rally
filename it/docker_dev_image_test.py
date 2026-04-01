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
import logging
import os
import subprocess
from collections.abc import Generator, Mapping

import pytest

import it
from esrally import version
from esrally.utils import cases

LOG = logging.getLogger(__name__)

COMPOSE_FILE = os.path.join(it.ROOT_DIR, "docker", "docker-compose-tests.yml")
RALLY_DOCKER_IMAGE = os.path.join(it.ROOT_DIR, "docker", "dev", "Dockerfile")


def tear_down_stack(compose_env: Mapping[str, str]) -> None:
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
        pytest.fail()
        LOG.error(
            "Docker compose down failed:\n  - command: '%s'\n  - args: %s\n  - return code: %s\n  - output:\n%s\n",
            err.cmd,
            err.args,
            err.returncode,
            err.stdout,
        )
    else:
        LOG.debug("Compose stack is down (env=%s).", compose_env)


@pytest.fixture(scope="module")
def compose_env() -> Generator[Mapping[str, str]]:
    env = {
        "RALLY_VERSION": version.__version__,
        "RALLY_VERSION_TAG": version.__version__,
        "RALLY_DOCKER_IMAGE": RALLY_DOCKER_IMAGE,
        "COMPOSE_PROJECT_NAME": __name__.replace(".", "_"),
    }
    tear_down_stack(env)
    try:
        yield env
    finally:
        if LOG.isEnabledFor(logging.DEBUG):
            logs = subprocess.run("docker compose logs -t", capture_output=True, shell=True, check=False, env=env)
            LOG.debug("Containers logs:\n%s", logs.stdout.decode("utf-8"))
        tear_down_stack(env)


@dataclasses.dataclass
class ComposeCase:
    command: str
    want_return_code: int = 0
    want_stdout: str | None = None
    want_stderr: str | None = None


@cases.cases(
    help=ComposeCase("--help"),
    race=ComposeCase(
        "race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    ),
)
def test_docker_compose(case: ComposeCase, compose_env: Mapping[str, str]) -> None:
    LOG.info("Running rally with 'docker compose', command='%s', env=%r", case.command, compose_env)
    env = os.environ.copy()
    env.update(compose_env)
    env["TEST_COMMAND"] = case.command
    try:
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
        assert result.stdout == case.want_stdout
    if case.want_stderr is not None:
        assert result.stderr == case.want_stderr
