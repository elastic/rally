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

import errno
import functools
import json
import logging
import os
import random
import socket
import subprocess
import time
from subprocess import CompletedProcess

import pytest

from esrally import client, version
from esrally.utils import process

CONFIG_NAMES = ["in-memory-it", "es-it"]
DISTRIBUTIONS = ["8.19.13", "9.2.7"]
TRACKS = ["geonames", "nyc_taxis", "http_logs", "nested"]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


LOG = logging.getLogger(__name__)


def all_rally_configs(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", CONFIG_NAMES)
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def random_rally_config(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", [random.choice(CONFIG_NAMES)])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def rally_in_mem(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["in-memory-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def rally_es(t):
    @functools.wraps(t)
    @pytest.mark.parametrize("cfg", ["es-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)

    return wrapper


def esrally_command_line_for(cfg: str, command_line: str) -> str:
    return f"esrally {command_line} --configuration-name='{cfg}'"


def esrally(cfg: str, command_line: str, check: bool = True) -> CompletedProcess:
    """
    Run ``esrally`` for subcommands other than ``race`` (see ``race``).

    With ``check=True`` (default), a non-zero exit code fails the test via
    ``pytest.fail`` and the captured stdout is included in the message.
    With ``check=False``, returns ``subprocess.CompletedProcess`` so callers
    can inspect ``returncode`` and ``stdout``.
    """
    command_line = esrally_command_line_for(cfg, command_line)
    LOG.info("Running rally: %r", command_line)
    try:
        return subprocess.run(command_line, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as err:
        output = "    ".join([""] + (err.stdout or "").splitlines(keepends=True))
        pytest.fail(f"Failed running esrally:\n - command line: {command_line}\n - output: {output}\n")


def race(cfg: str, command_line: str, enable_assertions: bool = True, check: bool = True) -> CompletedProcess:
    """
    Run ``esrally race`` with integration-test defaults (kill running processes,
    abort on error, optional ``--enable-assertions``).

    The ``check`` argument is forwarded to ``esrally``: ``True`` fails the test
    on non-zero exit; ``False`` returns ``subprocess.CompletedProcess`` for
    ``returncode`` / ``stdout`` inspection.
    """
    cmd = f"race {command_line} --kill-running-processes --on-error='abort'"
    if enable_assertions:
        cmd += " --enable-assertions"
    return esrally(cfg, cmd, check=check)


def shell_cmd(command_line):
    """
    Executes a given command_line in a subshell.

    :param command_line: (str) The command to execute
    :return: (int) the exit code
    """

    return subprocess.call(command_line, shell=True)


def command_in_docker(command_line, python_version):
    docker_command = f"docker run --rm -v {ROOT_DIR}:/rally_ro:ro python:{python_version} bash -c '{command_line}'"
    return subprocess.run(docker_command, shell=True, check=True).returncode


def wait_until_port_is_free(port_number=39200, timeout=120):
    start = time.perf_counter()
    end = start + timeout
    while time.perf_counter() < end:
        c = socket.socket()
        connect_result = c.connect_ex(("127.0.0.1", port_number))
        # noinspection PyBroadException
        try:
            if connect_result == errno.ECONNREFUSED:
                c.close()
                return
            else:
                c.close()
                time.sleep(0.5)
        except Exception:
            pass

    raise TimeoutError(f"Port [{port_number}] is occupied after [{timeout}] seconds")


class TestCluster:
    def __init__(self, cfg):
        self.cfg = cfg
        self.installation_id = None
        self.http_port = None

    def install(self, distribution_version, node_name, car, http_port):
        self.http_port = http_port
        transport_port = http_port + 100
        try:
            output = process.run_subprocess_with_output(
                "esrally install --configuration-name={cfg} --quiet --distribution-version={dist} --build-type=tar "
                "--http-port={http_port} --node={node_name} --master-nodes={node_name} --car={car} "
                '--seed-hosts="127.0.0.1:{transport_port}" --cluster-name={cfg}'.format(
                    cfg=self.cfg,
                    dist=distribution_version,
                    http_port=http_port,
                    node_name=node_name,
                    car=car,
                    transport_port=transport_port,
                )
            )
            self.installation_id = json.loads("".join(output))["installation-id"]
        except BaseException as e:
            raise AssertionError(f"Failed to install Elasticsearch {distribution_version}.", e)

    def start(self, race_id):
        cmd = f'start --runtime-jdk="bundled" --installation-id={self.installation_id} --race-id={race_id}'
        esrally(self.cfg, cmd)
        es = client.EsClientFactory(hosts=[{"host": "127.0.0.1", "port": self.http_port}], client_options={}).create()
        client.wait_for_rest_layer(es)
        assert es.info()["cluster_name"] == self.cfg

    def stop(self):
        if self.installation_id:
            r = esrally(self.cfg, f"stop --installation-id={self.installation_id}", check=False)
            if r.returncode != 0:
                raise AssertionError("Failed to stop Elasticsearch test cluster.")

    def __str__(self):
        return f"TestCluster[installation-id={self.installation_id}]"


def find_log_line(log_file, text) -> str | None:
    with open(log_file) as f:
        for line in f:
            if text in line:
                return line
    return None
