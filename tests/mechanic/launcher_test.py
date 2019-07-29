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
import io
from unittest import TestCase, mock

from esrally import config, exceptions, paths
from esrally.mechanic import launcher
from esrally.utils import opts


class MockMetricsStore:
    def add_meta_info(self, scope, scope_key, key, value):
        pass


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
            import elasticsearch
            raise elasticsearch.ConnectionError("Unittest error")
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # process.run_subprocess_with_logging uses Popen context manager, so let its return be 0
        self.returncode = 0

    def wait(self):
        return 0


MOCK_PID_VALUE = 1234


class ProcessLauncherTests(TestCase):
    @mock.patch('os.path.join', return_value="/telemetry")
    @mock.patch('os.kill')
    @mock.patch('subprocess.Popen',new=MockPopen)
    @mock.patch('esrally.mechanic.java_resolver.java_home', return_value=(12, "/java_home/"))
    @mock.patch('esrally.utils.jvm.supports_option', return_value=True)
    @mock.patch('os.chdir')
    @mock.patch('esrally.config.Config')
    @mock.patch('esrally.metrics.MetricsStore')
    @mock.patch('esrally.mechanic.provisioner.NodeConfiguration')
    @mock.patch('esrally.mechanic.launcher.wait_for_pidfile', return_value=MOCK_PID_VALUE)
    @mock.patch('psutil.Process')
    def test_daemon_start_stop(self, process, wait_for_pidfile, node_config, ms, cfg, chdir, supports, java_home, kill, path):
        proc_launcher = launcher.ProcessLauncher(cfg, ms, paths.races_root(cfg))

        nodes = proc_launcher.start([node_config])
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].pid, MOCK_PID_VALUE)

        proc_launcher.keep_running = False
        proc_launcher.stop(nodes)
        self.assertTrue(kill.called)


class ExternalLauncherTests(TestCase):
    test_host = opts.TargetHosts("127.0.0.1:9200,10.17.0.5:19200")
    client_options = opts.ClientOptions("timeout:60")

    def test_setup_external_cluster_single_node(self):
        cfg = config.Config()

        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)

        m = launcher.ExternalLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        m.start()

        # automatically determined by launcher on attach
        self.assertEqual(cfg.opts("mechanic", "distribution.version"), "5.0.0")

    def test_setup_external_cluster_multiple_nodes(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)
        cfg.add(config.Scope.application, "mechanic", "distribution.version", "2.3.3")

        m = launcher.ExternalLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        m.start()
        # did not change user defined value
        self.assertEqual(cfg.opts("mechanic", "distribution.version"), "2.3.3")

    def test_setup_external_cluster_cannot_determine_version(self):
        client_options = opts.ClientOptions("timeout:60,raise-error-on-info:true")
        cfg = config.Config()

        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", client_options)

        m = launcher.ExternalLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        m.start()

        # automatically determined by launcher on attach
        self.assertIsNone(cfg.opts("mechanic", "distribution.version"))


class ClusterLauncherTests(TestCase):
    test_host = opts.TargetHosts("10.0.0.10:9200,10.0.0.11:9200")
    client_options = opts.ClientOptions('timeout:60')

    def test_launches_cluster(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        cfg.add(config.Scope.application, "mechanic", "skip.rest.api.check", False)

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        cluster = cluster_launcher.start()

        self.assertEqual([{"host": "10.0.0.10", "port": 9200}, {"host": "10.0.0.11", "port": 9200}], cluster.hosts)
        self.assertIsNotNone(cluster.telemetry)

    def test_launches_cluster_with_telemetry_client_timeout_enabled(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        cfg.add(config.Scope.application, "mechanic", "skip.rest.api.check", False)

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        cluster = cluster_launcher.start()

        for telemetry_device in cluster.telemetry.devices:
            if hasattr(telemetry_device, "clients"):
                # Process all clients options for multi cluster aware telemetry devices, like CcrStats
                for _, client in telemetry_device.clients.items():
                    self.assertDictEqual({"retry-on-timeout": True, "timeout": 60}, client.client_options)
            else:
                self.assertDictEqual({"retry-on-timeout": True, "timeout": 60}, telemetry_device.client.client_options)

    @mock.patch("time.sleep")
    def test_error_on_cluster_launch(self, sleep):
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        # Simulate that the client will raise an error upon startup
        cfg.add(config.Scope.application, "client", "options", opts.ClientOptions("raise-error-on-info:true"))
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})
        cfg.add(config.Scope.application, "mechanic", "preserve.install", False)
        cfg.add(config.Scope.application, "mechanic", "skip.rest.api.check", False)

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        with self.assertRaisesRegex(exceptions.LaunchError,
                                    "Elasticsearch REST API layer is not available. Forcefully terminated cluster."):
            cluster_launcher.start()
