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

import os
import shutil
import tempfile

import pytest

from esrally import config, version
from esrally.utils import process
from it import CONFIG_NAMES, ROOT_DIR, TestCluster


def check_prerequisites():
    print("Checking prerequisites...")
    if process.run_subprocess_with_logging("docker ps") != 0:
        raise AssertionError("Docker must be installed and the daemon must be up and running to run integration tests.")
    if process.run_subprocess_with_logging("docker-compose --help") != 0:
        raise AssertionError("Docker Compose is required to run integration tests.")


def install_integration_test_config():
    def copy_config(name):
        source_path = os.path.join(os.path.dirname(__file__), "resources", f"rally-{name}.ini")
        f = config.ConfigFile(name)
        f.store_default_config(template_path=source_path)

    print("Installing integration test configs...")
    for n in CONFIG_NAMES:
        copy_config(n)


def get_license():
    with open(os.path.join(ROOT_DIR, "LICENSE")) as license_file:
        return license_file.readlines()[1].strip()


def build_docker_image():
    print("Building docker image...")
    rally_version = version.__version__

    env_variables = os.environ.copy()
    env_variables["RALLY_VERSION"] = rally_version
    env_variables["RALLY_LICENSE"] = get_license()

    command = (
        f"docker build -t elastic/rally:{rally_version} --build-arg RALLY_VERSION --build-arg RALLY_LICENSE "
        f"-f {ROOT_DIR}/docker/Dockerfiles/dev/Dockerfile {ROOT_DIR}"
    )

    if process.run_subprocess_with_logging(command, env=env_variables) != 0:
        raise AssertionError("It was not possible to build the docker image from Dockerfile-dev")


def remove_integration_test_config():
    for config_name in CONFIG_NAMES:
        os.remove(config.ConfigFile(config_name).location)


class EsMetricsStore:
    VERSION = "8.5.1"

    def __init__(self):
        self.cluster = TestCluster("in-memory-it")

    def start(self):
        print("Starting Elasticsearch metrics store...")
        self.cluster.install(
            distribution_version=EsMetricsStore.VERSION,
            node_name="metrics-store",
            car="defaults,basic-license",
            http_port=10200,
        )
        self.cluster.start(race_id="metrics-store")

    def stop(self):
        print("Stopping Elasticsearch metrics store...")
        self.cluster.stop()


ES_METRICS_STORE = EsMetricsStore()


@pytest.fixture(scope="session", autouse=True)
def shared_setup():
    print("\nStarting shared setup...")
    check_prerequisites()
    install_integration_test_config()
    ES_METRICS_STORE.start()
    build_docker_image()
    yield
    print("\nStopping shared setup...")
    ES_METRICS_STORE.stop()
    remove_integration_test_config()


class ConfigFile:
    def __init__(self, config_name):
        self.user_home = os.getenv("RALLY_HOME", os.path.expanduser("~"))
        self.rally_home = os.path.join(self.user_home, ".rally")
        if config_name is not None:
            self.config_file_name = f"rally-{config_name}.ini"
        else:
            self.config_file_name = "rally.ini"
        self.source_path = os.path.join(os.path.dirname(__file__), "resources", self.config_file_name)
        self.target_path = os.path.join(self.rally_home, self.config_file_name)


# ensures that a fresh log file is available
@pytest.fixture(scope="function")
def fresh_log_file():
    cfg = ConfigFile(config_name=None)
    log_file = os.path.join(cfg.rally_home, "logs", "rally.log")

    if os.path.exists(log_file):
        bak = os.path.join(tempfile.mkdtemp(), "rally.log")
        shutil.move(log_file, bak)
        yield log_file
        # append log lines to the original file and move it back to its original
        with open(log_file) as src:
            with open(bak, "a") as dst:
                dst.write(src.read())
        shutil.move(bak, log_file)
    else:
        yield log_file
