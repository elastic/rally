# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
import random

import pytest

from esrally.utils import process, io

CONFIG_NAMES = ["in-memory-it", "es-it"]


def all_rally_configs(t):
    @pytest.mark.parametrize("cfg", CONFIG_NAMES)
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)
    return wrapper


def random_rally_config(t):
    @pytest.mark.parametrize("cfg", [random.choice(CONFIG_NAMES)])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)
    return wrapper


def rally_in_mem(t):
    @pytest.mark.parametrize("cfg", ["in-memory-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)
    return wrapper


def rally_es(t):
    @pytest.mark.parametrize("cfg", ["es-it"])
    def wrapper(cfg, *args, **kwargs):
        t(cfg, *args, **kwargs)
    return wrapper


def esrally(cfg, command_line):
    return os.system("esrally {} --configuration-name=\"{}\"".format(command_line, cfg))


def wait_until_port_is_free(port_number=39200):
    import socket
    s = socket.socket()
    while True:
        try:
            s.connect(("127.0.0.1", port_number))
            s.close()
            break
        except Exception:
            pass


def check_prerequisites():
    if process.run_subprocess_with_logging("docker") != 0:
        raise AssertionError("Docker is required to run integration tests.")
    if process.run_subprocess_with_logging("docker ps") != 0:
        raise AssertionError("Docker daemon must be up and running to run integration tests.")
    if process.run_subprocess_with_logging("docker-compose --help") != 0:
        raise AssertionError("Docker Compose is required to run integration tests.")


class ConfigFile:
    def __init__(self, config_name):
        self.user_home = os.path.expanduser("~")
        self.rally_home = os.path.join(self.user_home, ".rally")
        self.config_file_name = "rally-{}.ini".format(config_name)
        self.source_path = os.path.join(os.path.dirname(__file__), "resources", self.config_file_name)
        self.target_path = os.path.join(self.rally_home, self.config_file_name)


class TestCluster:
    def __init__(self, cfg):
        self.cfg = cfg
        self.installation_id = None

    def install(self, distribution_version, node_name, http_port):
        transport_port = http_port + 100
        try:
            output = process.run_subprocess_with_output(
                "esrally install --configuration-name={} --quiet --distribution-version={} --build-type=tar "
                "--http-port={} --node={} --master-nodes={} --seed-hosts=\"127.0.0.1:{}\"".format(
                    self.cfg, distribution_version, http_port, node_name, node_name, transport_port))

            self.installation_id = json.loads("".join(output))["installation-id"]
        except BaseException as e:
            raise AssertionError("Failed to install Elasticsearch {}.".format(distribution_version), e)

    def start(self, race_id):
        cmd = "start --runtime-jdk=\"bundled\" --installation-id={} --race-id={}".format(self.installation_id, race_id)
        if esrally(self.cfg, cmd) != 0:
            raise AssertionError("Failed to start Elasticsearch test cluster.")

    def stop(self):
        if self.installation_id:
            if esrally(self.cfg, "stop --installation-id={}".format(self.installation_id)) != 0:
                raise AssertionError("Failed to stop Elasticsearch test cluster.")


class EsMetricsStore:
    VERSION = "7.6.0"

    def __init__(self):
        self.cluster = TestCluster("in-memory-it")

    def start(self):
        self.cluster.install(distribution_version=EsMetricsStore.VERSION, node_name="metrics-store", http_port=10200)
        self.cluster.start(race_id="metrics-store")

    def stop(self):
        self.cluster.stop()


def install_integration_test_config():
    def copy_config(name):
        f = ConfigFile(name)
        io.ensure_dir(f.rally_home)
        with open(f.target_path, "w", encoding="UTF-8") as target:
            with open(f.source_path, "r", encoding="UTF-8") as src:
                contents = src.read().replace("${USER_HOME}", f.user_home)
                target.write(contents)

    for n in CONFIG_NAMES:
        copy_config(n)


def remove_integration_test_config():
    for n in CONFIG_NAMES:
        os.remove(ConfigFile(n).target_path)


ES_METRICS_STORE = EsMetricsStore()


def setup_module():
    check_prerequisites()
    install_integration_test_config()
    ES_METRICS_STORE.start()


def teardown_module():
    ES_METRICS_STORE.stop()
    remove_integration_test_config()

