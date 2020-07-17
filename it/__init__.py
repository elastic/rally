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

import functools
import json
import os
import random
from string import Template

import pytest

from esrally import client, version
from esrally.utils import process, io

CONFIG_NAMES = ["in-memory-it", "es-it"]
DISTRIBUTIONS = ["2.4.6", "5.6.16", "6.8.0", "7.6.0"]
TRACKS = ["geonames", "nyc_taxis", "http_logs", "nested"]
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


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


def esrally_command_line_for(cfg, command_line):
    return f"esrally {command_line} --kill-running-processes --configuration-name='{cfg}'"


def esrally(cfg, command_line):
    """
    This method should be used for rally invocations of the all commands besides race.
    These commands may have different CLI options than race.
    """
    return os.system(esrally_command_line_for(cfg, command_line))


def race(cfg, command_line):
    """
    This method should be used for rally invocations of the default race command.
    It sets up some defaults for how the integration tests expect to run races.
    """
    return esrally(cfg, f"race {command_line} --on-error='abort'")


def wait_until_port_is_free(port_number=39200, timeout=120):
    import errno
    import time
    import socket

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


def check_prerequisites():
    if process.run_subprocess_with_logging("docker ps") != 0:
        raise AssertionError("Docker must be installed and the daemon must be up and running to run integration tests.")
    if process.run_subprocess_with_logging("docker-compose --help") != 0:
        raise AssertionError("Docker Compose is required to run integration tests.")


class ConfigFile:
    def __init__(self, config_name):
        self.user_home = os.getenv("RALLY_HOME", os.path.expanduser("~"))
        self.rally_home = os.path.join(self.user_home, ".rally")
        self.config_file_name = "rally-{}.ini".format(config_name)
        self.source_path = os.path.join(os.path.dirname(__file__), "resources", self.config_file_name)
        self.target_path = os.path.join(self.rally_home, self.config_file_name)


class TestCluster:
    def __init__(self,cfg):
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
                "--seed-hosts=\"127.0.0.1:{transport_port}\"".format(cfg=self.cfg,
                                                                     dist=distribution_version,
                                                                     http_port=http_port,
                                                                     node_name=node_name,
                                                                     car=car,
                                                                     transport_port=transport_port))
            self.installation_id = json.loads("".join(output))["installation-id"]
        except BaseException as e:
            raise AssertionError("Failed to install Elasticsearch {}.".format(distribution_version), e)

    def start(self, race_id):
        cmd = "start --runtime-jdk=\"bundled\" --installation-id={} --race-id={}".format(self.installation_id, race_id)
        if esrally(self.cfg, cmd) != 0:
            raise AssertionError("Failed to start Elasticsearch test cluster.")
        es = client.EsClientFactory(hosts=[{"host": "127.0.0.1", "port": self.http_port}], client_options={}).create()
        client.wait_for_rest_layer(es)

    def stop(self):
        if self.installation_id:
            if esrally(self.cfg, "stop --installation-id={}".format(self.installation_id)) != 0:
                raise AssertionError("Failed to stop Elasticsearch test cluster.")

    def __str__(self):
        return f"TestCluster[installation-id={self.installation_id}]"


class EsMetricsStore:
    VERSION = "7.6.0"

    def __init__(self):
        self.cluster = TestCluster("in-memory-it")

    def start(self):
        self.cluster.install(distribution_version=EsMetricsStore.VERSION,
                             node_name="metrics-store",
                             car="defaults",
                             http_port=10200)
        self.cluster.start(race_id="metrics-store")

    def stop(self):
        self.cluster.stop()


def install_integration_test_config():
    def copy_config(name):
        f = ConfigFile(name)
        io.ensure_dir(f.rally_home)
        with open(f.target_path, "w", encoding="UTF-8") as target:
            with open(f.source_path, "r", encoding="UTF-8") as src:
                contents = src.read()
                # Rally allows for a RALLY_HOME homedir override. This honors the change for it tests
                user_home = os.getenv("RALLY_HOME", f.user_home)
                target.write(Template(contents).substitute(USER_HOME=user_home))

    for n in CONFIG_NAMES:
        copy_config(n)


def remove_integration_test_config(config_names=None):
    for n in config_names or CONFIG_NAMES:
        os.remove(ConfigFile(n).target_path)


ES_METRICS_STORE = EsMetricsStore()


def get_license():
    with open(os.path.join(ROOT_DIR, 'LICENSE')) as license_file:
        return license_file.readlines()[1].strip()


def build_docker_image():
    rally_version = version.__version__

    env_variables = os.environ.copy()
    env_variables['RALLY_VERSION'] = rally_version
    env_variables['RALLY_LICENSE'] = get_license()

    command = f"docker build -t elastic/rally:{rally_version} --build-arg RALLY_VERSION --build-arg RALLY_LICENSE " \
              f"-f {ROOT_DIR}/docker/Dockerfiles/Dockerfile-dev {ROOT_DIR}"

    if process.run_subprocess_with_logging(command, env=env_variables) != 0:
        raise AssertionError("It was not possible to build the docker image from Dockerfile-dev")


def setup_module():
    check_prerequisites()
    install_integration_test_config()
    ES_METRICS_STORE.start()
    build_docker_image()


def teardown_module():
    ES_METRICS_STORE.stop()
    remove_integration_test_config()
