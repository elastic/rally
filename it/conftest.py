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
import socket
import tempfile
from collections.abc import Generator

import pytest

from esrally import config, version
from esrally.utils import process
from it import CONFIG_NAMES, ROOT_DIR, TestCluster, ensure_benchmark_http_port_free


def check_prerequisites():
    print("Checking prerequisites...")
    if process.run_subprocess_with_logging("docker ps") != 0:
        raise AssertionError("Docker must be installed and the daemon must be up and running to run integration tests.")
    if process.run_subprocess_with_logging("docker compose version") != 0:
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
    """Session-scoped Elasticsearch used as the integration-test metrics store."""

    VERSION = "8.5.1"
    HTTP_PORT = 10200

    def __init__(self) -> None:
        self.cluster = TestCluster("in-memory-it")

    def start(self) -> None:
        """Ensure metrics Elasticsearch is up on :attr:`HTTP_PORT`, installing it if needed."""
        port = self.HTTP_PORT
        cluster = self.cluster
        name = cluster.probe_cluster_on_port(port)
        if name == cluster.cfg:
            print("Elasticsearch metrics store already running; waiting for cluster health (yellow)...")
            cluster.http_port = port
            cluster.wait_for_cluster_health()
            return
        if name is not None:
            raise AssertionError(
                f"Port {port} answers Elasticsearch but reports cluster name {name!r}; "
                f"integration tests expect {cluster.cfg!r} for the metrics store. "
                f"Stop the other cluster or change EsMetricsStore.HTTP_PORT."
            )
        probe_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if probe_sock.connect_ex(("127.0.0.1", port)) == 0:
                raise AssertionError(
                    f"Something is listening on 127.0.0.1:{port} but the Elasticsearch REST probe did not succeed "
                    f"(expected cluster name {cluster.cfg!r} when reusing the metrics store). "
                    f"Free the port or check firewall/proxy. If Elasticsearch is still starting, wait and retry."
                )
        finally:
            probe_sock.close()
        print("Starting Elasticsearch metrics store...")
        self.cluster.install(
            distribution_version=EsMetricsStore.VERSION,
            node_name="metrics-store",
            car="defaults,basic-license",
            http_port=self.HTTP_PORT,
        )
        self.cluster.start(race_id="metrics-store")
        print("Waiting for metrics store cluster health (yellow)...")
        self.cluster.wait_for_cluster_health()

    def stop(self) -> None:
        print("Stopping Elasticsearch metrics store...")
        self.cluster.stop()


ES_METRICS_STORE = EsMetricsStore()


@pytest.fixture(scope="session", autouse=True)
def isolate_git_global_config() -> Generator[None]:
    """
    Point ``GIT_CONFIG_GLOBAL`` at :data:`os.devnull` so integration tests are not affected by the
    developer's ``~/.gitconfig`` (e.g. per-URL ``proxy=""`` can let git ignore env proxies).

    Session-scoped fixtures cannot use pytest's function-scoped ``monkeypatch`` fixture; use a
    dedicated :class:`pytest.MonkeyPatch` instance and :meth:`~pytest.MonkeyPatch.undo` after the session.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    yield
    mp.undo()


@pytest.fixture(scope="session", autouse=True)
def shared_setup(isolate_git_global_config: None) -> Generator[None]:
    """
    Session autouse hook: prerequisites, Rally IT config, metrics store, and docker image.

    Depends on :func:`isolate_git_global_config` so git runs with a neutral global config first.
    """
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


@pytest.fixture
def free_benchmark_http_port() -> Generator[int]:
    """
    Before and after the test, clear Rally IT benchmark HTTP port (19200): stop orphaned
    ``rally-benchmark`` / ``in-memory-it`` Elasticsearch (Docker or host JVM) and wait until the port is free.

    See :func:`it.ensure_benchmark_http_port_free` for rationale on the fixed port and teardown behavior.
    """
    port = ensure_benchmark_http_port_free()
    yield port
    ensure_benchmark_http_port_free(port)


@pytest.fixture(scope="module")
def free_benchmark_http_port_module() -> Generator[int]:
    """
    Module-scoped variant of :func:`free_benchmark_http_port` for module fixtures (e.g. ``test_cluster``).

    See :func:`it.ensure_benchmark_http_port_free` for rationale on the fixed port and teardown behavior.
    """
    port = ensure_benchmark_http_port_free()
    yield port
    ensure_benchmark_http_port_free(port)


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
