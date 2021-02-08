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
# pylint: disable=protected-access

import io
import os
import sys
import uuid
from datetime import datetime
from unittest import TestCase, mock
from unittest.mock import mock_open

import elasticsearch
import psutil

from esrally import config, exceptions, telemetry
from esrally.mechanic import launcher, cluster
from esrally.mechanic.provisioner import NodeConfiguration
from esrally.metrics import InMemoryMetricsStore


class MockClientFactory:
    def __init__(self, hosts, client_options):
        self.client_options = client_options

    def create(self):
        return MockClient(self.client_options)


class MockClient:
    def __init__(self, client_options):
        self.client_options = client_options
        self.cluster = SubClient({
            "cluster_name": "rally-benchmark-cluster",
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "Nefarius",
                    "host": "127.0.0.1"
                }
            }
        })
        self.nodes = SubClient({
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "Nefarius",
                    "host": "127.0.0.1",
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation"
                    }
                }
            }
        })
        self._info = {
            "version":
                {
                    "number": "5.0.0",
                    "build_hash": "abc123"
                }
        }

    def info(self):
        if self.client_options.get("raise-error-on-info", False):
            raise elasticsearch.TransportError(401, "Unauthorized")
        return self._info

    def search(self, *args, **kwargs):
        return {}


class SubClient:
    def __init__(self, info):
        self._info = info

    def stats(self, *args, **kwargs):
        return self._info

    def info(self, *args, **kwargs):
        return self._info


class MockPopen:
    def __init__(self, *args, **kwargs):
        # Currently, the only code that checks returncode directly during
        # ProcessLauncherTests are telemetry.  If we return 1 for them we can skip
        # mocking them as their optional functionality is disabled.
        self.returncode = 1
        self.stdout = io.StringIO()

    def communicate(self, input=None, timeout=None):
        return [b"", b""]

    def poll(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # process.run_subprocess_with_logging uses Popen context manager, so let its return be 0
        self.returncode = 0

    def wait(self):
        return 0


class MockProcess:
    def __init__(self, pid):
        self.pid = pid

    def name(self):
        return "p{pid}".format(pid=self.pid)

    def wait(self, timeout=None):
        raise psutil.TimeoutExpired(timeout)

    def terminate(self):
        pass

    def kill(self):
        pass


class TerminatedProcess:
    def __init__(self, pid):
        raise psutil.NoSuchProcess(pid)


def get_metrics_store(cfg):
    ms = InMemoryMetricsStore(cfg)
    ms.open(race_id=str(uuid.uuid4()),
            race_timestamp=datetime.now(),
            track_name="test",
            challenge_name="test",
            car_name="test")
    return ms


MOCK_PID_VALUE = 1234


class ProcessLauncherTests(TestCase):
    @mock.patch('subprocess.Popen', new=MockPopen)
    @mock.patch('esrally.mechanic.java_resolver.java_home', return_value=(12, "/java_home/"))
    @mock.patch('esrally.utils.jvm.supports_option', return_value=True)
    @mock.patch('esrally.utils.io.get_size')
    @mock.patch('os.chdir')
    @mock.patch('esrally.mechanic.launcher.wait_for_pidfile', return_value=MOCK_PID_VALUE)
    @mock.patch('psutil.Process', new=MockProcess)
    def test_daemon_start_stop(self, wait_for_pidfile, chdir, get_size, supports, java_home):
        cfg = config.Config()
        cfg.add(config.Scope.application, "node", "root.dir", "test")
        cfg.add(config.Scope.application, "mechanic", "runtime.jdk", None)
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "telemetry", "params", None)
        cfg.add(config.Scope.application, "system", "env.name", "test")

        ms = get_metrics_store(cfg)
        proc_launcher = launcher.ProcessLauncher(cfg)

        node_configs = []
        for node in range(2):
            node_configs.append(NodeConfiguration(build_type="tar", car_runtime_jdks="12,11", car_provides_bundled_jdk=True,
                                                  ip="127.0.0.1", node_name="testnode-{}".format(node),
                                                  node_root_path="/tmp", binary_path="/tmp", data_paths="/tmp"))

        nodes = proc_launcher.start(node_configs)
        self.assertEqual(len(nodes), 2)
        self.assertEqual(nodes[0].pid, MOCK_PID_VALUE)

        stopped_nodes = proc_launcher.stop(nodes, ms)
        # all nodes should be stopped
        self.assertEqual(nodes, stopped_nodes)

    @mock.patch('psutil.Process', new=TerminatedProcess)
    def test_daemon_stop_with_already_terminated_process(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "node", "root.dir", "test")
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "telemetry", "params", None)
        cfg.add(config.Scope.application, "system", "env.name", "test")

        ms = get_metrics_store(cfg)
        proc_launcher = launcher.ProcessLauncher(cfg)

        nodes = [
            cluster.Node(pid=-1,
                         binary_path="/bin",
                         host_name="localhost",
                         node_name="rally-0",
                         telemetry=telemetry.Telemetry())
        ]

        stopped_nodes = proc_launcher.stop(nodes, ms)
        # no nodes should have been stopped (they were already stopped)
        self.assertEqual([], stopped_nodes)

    # flight recorder shows a warning for several seconds before continuing
    @mock.patch("esrally.time.sleep")
    def test_env_options_order(self, sleep):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "test")

        proc_launcher = launcher.ProcessLauncher(cfg)

        node_telemetry = [
            telemetry.FlightRecorder(telemetry_params={}, log_root="/tmp/telemetry", java_major_version=8)
        ]
        t = telemetry.Telemetry(["jfr"], devices=node_telemetry)
        env = proc_launcher._prepare_env(node_name="node0", java_home="/java_home", t=t)

        self.assertEqual("/java_home/bin" + os.pathsep + os.environ["PATH"], env["PATH"])
        self.assertEqual("-XX:+ExitOnOutOfMemoryError -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                         "-XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                         "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,dumponexitpath=/tmp/telemetry/profile.jfr " # pylint: disable=line-too-long
                         "-XX:StartFlightRecording=defaultrecording=true", env["ES_JAVA_OPTS"])

    def test_bundled_jdk_not_in_path(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "test")
        os.environ["JAVA_HOME"] = "/path/to/java"

        proc_launcher = launcher.ProcessLauncher(cfg)

        t = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = proc_launcher._prepare_env(node_name="node0", java_home=None, t=t)

        # unmodified
        self.assertEqual(os.environ["PATH"], env["PATH"])
        self.assertIsNone(env.get("JAVA_HOME"))

    def test_pass_env_vars(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "test")
        cfg.add(config.Scope.application, "system", "passenv", "JAVA_HOME,FOO1")
        os.environ["JAVA_HOME"] = "/path/to/java"
        os.environ["FOO1"] = "BAR1"

        proc_launcher = launcher.ProcessLauncher(cfg)

        t = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = proc_launcher._prepare_env(node_name="node0", java_home=None, t=t)

        # unmodified
        self.assertEqual(os.environ["JAVA_HOME"], env["JAVA_HOME"])
        self.assertEqual(os.environ["FOO1"], env["FOO1"])
        self.assertEqual(env["ES_JAVA_OPTS"], "-XX:+ExitOnOutOfMemoryError")

    def test_pass_java_opts(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "test")
        cfg.add(config.Scope.application, "system", "passenv", "ES_JAVA_OPTS")
        os.environ["ES_JAVA_OPTS"] = "-XX:-someJunk"

        proc_launcher = launcher.ProcessLauncher(cfg)

        t = telemetry.Telemetry()
        # no JAVA_HOME -> use the bundled JDK
        env = proc_launcher._prepare_env(node_name="node0", java_home=None, t=t)

        # unmodified
        self.assertEqual(os.environ["ES_JAVA_OPTS"], env["ES_JAVA_OPTS"])

    @mock.patch("esrally.time.sleep")
    def test_pidfile_wait_race(self, sleep):
        mo = mock_open()
        with self.assertRaises(exceptions.LaunchError):
            mo.side_effect = FileNotFoundError
            testclock = TestClock(IterationBasedStopWatch(1))
            with mock.patch("builtins.open", mo):
                launcher.wait_for_pidfile("testpidfile", clock=testclock)
        with self.assertRaises(exceptions.LaunchError):
            mo = mock_open()
            testclock = TestClock(IterationBasedStopWatch(1))
            with mock.patch("builtins.open", mo):
                launcher.wait_for_pidfile("testpidfile", clock=testclock)
        mo = mock_open(read_data="1234")
        testclock = TestClock(IterationBasedStopWatch(1))
        with mock.patch("builtins.open", mo):
            ret = launcher.wait_for_pidfile("testpidfile", clock=testclock)
            self.assertEqual(ret, 1234)

        def mock_open_with_delayed_write(read_data):
            mo = mock_open(read_data=read_data)
            handle = mo.return_value
            old_read_se = handle.read.side_effect
            handle.has_been_read = False

            def _stub_first_read(*args, **kwargs):
                if not handle.has_been_read:
                    handle.has_been_read = True
                    return ""
                else:
                    return old_read_se(*args, *kwargs)
            handle.read.side_effect = _stub_first_read
            return mo

        testclock = TestClock(IterationBasedStopWatch(2))
        with mock.patch("builtins.open", mock_open_with_delayed_write(read_data="4321")):
            ret = launcher.wait_for_pidfile("testpidfile", clock=testclock)
            self.assertEqual(ret, 4321)


class IterationBasedStopWatch:
    __test__ = False

    def __init__(self, max_iterations):
        self.iterations = 0
        self.max_iterations = max_iterations

    def start(self):
        self.iterations = 0

    def split_time(self):
        if self.iterations < self.max_iterations:
            self.iterations += 1
            return 0
        else:
            return sys.maxsize


class TestClock:
    __test__ = False

    def __init__(self, stop_watch):
        self._stop_watch = stop_watch

    def stop_watch(self):
        return self._stop_watch


class DockerLauncherTests(TestCase):
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    def test_starts_container_successfully(self, run_subprocess_with_output, run_subprocess_with_logging):
        run_subprocess_with_logging.return_value = 0
        # Docker container id (from docker-compose ps), Docker container id (from docker ps --filter ...)
        run_subprocess_with_output.side_effect = [["de604d0d"], ["de604d0d"]]
        cfg = config.Config()
        docker = launcher.DockerLauncher(cfg)

        node_config = NodeConfiguration(build_type="docker", car_runtime_jdks="12,11", car_provides_bundled_jdk=True,
                                        ip="127.0.0.1", node_name="testnode", node_root_path="/tmp", binary_path="/bin",
                                        data_paths="/tmp")

        nodes = docker.start([node_config])
        self.assertEqual(1, len(nodes))
        node = nodes[0]

        self.assertEqual(0, node.pid)
        self.assertEqual("/bin", node.binary_path)
        self.assertEqual("127.0.0.1", node.host_name)
        self.assertEqual("testnode", node.node_name)
        self.assertIsNotNone(node.telemetry)

        run_subprocess_with_logging.assert_called_once_with("docker-compose -f /bin/docker-compose.yml up -d")
        run_subprocess_with_output.assert_has_calls([
            mock.call("docker-compose -f /bin/docker-compose.yml ps -q"),
            mock.call('docker ps -a --filter "id=de604d0d" --filter "status=running" --filter "health=healthy" -q')
        ])

    @mock.patch("esrally.time.sleep")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    @mock.patch("esrally.utils.process.run_subprocess_with_output")
    def test_container_not_started(self, run_subprocess_with_output, run_subprocess_with_logging, sleep):
        run_subprocess_with_logging.return_value = 0
        # Docker container id (from docker-compose ps), but NO Docker container id (from docker ps --filter...) twice
        run_subprocess_with_output.side_effect = [["de604d0d"], [], []]
        cfg = config.Config()
        # ensure we only check the status two times
        stop_watch = IterationBasedStopWatch(max_iterations=2)
        docker = launcher.DockerLauncher(cfg, clock=TestClock(stop_watch=stop_watch))

        node_config = NodeConfiguration(build_type="docker", car_runtime_jdks="12,11", car_provides_bundled_jdk=True,
                                        ip="127.0.0.1", node_name="testnode", node_root_path="/tmp", binary_path="/bin",
                                        data_paths="/tmp")

        with self.assertRaisesRegex(exceptions.LaunchError, "No healthy running container after 600 seconds!"):
            docker.start([node_config])

    @mock.patch("esrally.telemetry.add_metadata_for_node")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_stops_container_successfully_with_metrics_store(self, run_subprocess_with_logging, add_metadata_for_node):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "test")

        metrics_store = get_metrics_store(cfg)
        docker = launcher.DockerLauncher(cfg)

        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]

        docker.stop(nodes, metrics_store=metrics_store)

        add_metadata_for_node.assert_called_once_with(metrics_store, "testnode", "127.0.0.1")

        run_subprocess_with_logging.assert_called_once_with("docker-compose -f /bin/docker-compose.yml down")

    @mock.patch("esrally.telemetry.add_metadata_for_node")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_stops_container_when_no_metrics_store_is_provided(self, run_subprocess_with_logging, add_metadata_for_node):
        cfg = config.Config()
        metrics_store = None
        docker = launcher.DockerLauncher(cfg)

        nodes = [cluster.Node(0, "/bin", "127.0.0.1", "testnode", telemetry.Telemetry())]

        docker.stop(nodes, metrics_store=metrics_store)

        self.assertEqual(0, add_metadata_for_node.call_count)

        run_subprocess_with_logging.assert_called_once_with("docker-compose -f /bin/docker-compose.yml down")
