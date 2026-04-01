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

import pytest

import it
from esrally import version
from esrally.utils import cases

LOG = logging.getLogger(__name__)

@dataclasses.dataclass
class ComposeCase:
    command: str
    want_return_code: int = 0
    want_stdout: str | None = None
    want_stderr: str | None = None
    compose_file: str = os.path.join(it.ROOT_DIR, "docker", "docker-compose-tests.yml")
    image_file: str = os.path.join(it.ROOT_DIR, "docker", "dev", "Dockerfile")

    @property
    def env(self) -> dict[str, str]:
        return {
            "TEST_COMMAND": self.command,
            "RALLY_VERSION": version.__version__,
            "RALLY_VERSION_TAG": version.__version__,
            "RALLY_DOCKER_IMAGE": self.image_file,
        }


@cases.cases(
    help=ComposeCase("--help"),
    race=ComposeCase(
        "race --pipeline=benchmark-only --test-mode --track=geonames --challenge=append-no-conflicts-index-only --target-hosts=es01:9200"
    ),
)
def test_docker_compose(case: ComposeCase):
    LOG.info("Running rally with 'docker compose', command='%s', env=%r", case.command, case.env)
    env = os.environ.copy()
    env.update(case.env)
    try:
        result = subprocess.run(
            f"docker compose -f '{case.compose_file}' up --abort-on-container-exit",
            env=env,
            capture_output=False,  # It defines streams manually
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=case.want_return_code == 0,
            shell=True,
        )
    except subprocess.CalledProcessError as err:
        pytest.fail(
            "Docker compose up failed:\n"
            f"  - command: {err.cmd}\n"
            f"  - args: {err.args}\n"
            f"  - return code: {err.returncode}\n"
            f"  - stdout:\n{err.stdout}\n"
            f"  - stderr:\n{err.stderr}\n"
        )
    else:
        LOG.debug("Docker compose up succeeded. STDOUT:\n%s", result.stdout)
        assert result.returncode == case.want_return_code
        if case.want_stdout is not None:
            assert result.stdout == case.want_stdout
        if case.want_stderr is not None:
            assert result.stderr == case.want_stderr
    finally:
        try:
            LOG.debug("Tear down compose stack...")
            subprocess.run(
                f"docker compose -f '{case.compose_file}' down -v",
                env=case.env,
                capture_output=False,  # It defines streams manually
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=True,
                shell=True,
            )
        except subprocess.CalledProcessError as err:
            LOG.error(
                "Docker compose down failed:\n  - command: '%s'\n  - args: %s\n  - return code: %s\n  - output:\n%s\n",
                err.cmd,
                err.args,
                err.returncode,
                err.stdout,
            )
        else:
            LOG.debug("Compose stack is down.")
