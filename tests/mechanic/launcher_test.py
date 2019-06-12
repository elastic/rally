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
import datetime
import logging
import os
from unittest import TestCase, mock

from esrally import config, exceptions, metrics, paths
from esrally.mechanic import launcher, provisioner, team
from esrally.utils import opts
from esrally.utils.io import guess_java_home


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


logging.basicConfig(level=logging.DEBUG)
HOME_DIR = os.path.expanduser("~")


def create_config():
    cfg = config.Config()
    # collect some mandatory config here
    cfg.add(config.Scope.application, "node", "root.dir", os.path.join(HOME_DIR, ".rally", "benchmarks"))
    cfg.add(config.Scope.application, 'reporting', 'datastore.type', None)
    cfg.add(config.Scope.application, 'track', 'params', None)
    cfg.add(config.Scope.application, 'system', 'env.name', "unittest")
    cfg.add(config.Scope.application, 'mechanic', 'keep.running', False)
    cfg.add(config.Scope.application, 'mechanic', 'runtime.jdk', 12)
    cfg.add(config.Scope.application, 'mechanic', 'telemetry.devices', [])
    cfg.add(config.Scope.application, 'mechanic', 'telemetry.params', None)

    return cfg


def create_metrics_store(cfg, car):
    cls = metrics.metrics_store_class(cfg)
    metrics_store = cls(cfg)
    metrics_store.lap = 0

    metrics_store.open(trial_id="test",
                       track_name="test",
                       trial_timestamp=datetime.datetime.now(),
                       challenge_name="test",
                       car_name=car.name)
    return metrics_store


def create_default_car():
    return team.load_car(HOME_DIR + "/.rally/benchmarks/teams/default",
                         ["defaults"],
                         None)


def create_provisioner(car, ver):
    installer = provisioner.ElasticsearchInstaller(car=car,
                                                   java_home=guess_java_home(),
                                                   node_name="rally-node-0",
                                                   node_root_dir= os.path.join(HOME_DIR, ".rally", "benchmarks",
                                                                               "races", "unittest"),
                                                   ip="0.0.0.0",
                                                   all_node_ips=["0.0.0.0"],
                                                   http_port=9200)
    p = provisioner.BareProvisioner(cluster_settings={"indices.query.bool.max_clause_count": 50000},
                                    es_installer=installer,
                                    plugin_installers=[],
                                    preserve=True,
                                    distribution_version=ver)
    return p


class MockPopen:
    def __init__(self, *args, **kwargs):
        self.returncode = 1

    def communicate(self, input=None, timeout=None):
        return [b"", b""]

    def wait(self):
        return 0


MOCK_PID_VALUE = 1234


class ProcessLauncherTests(TestCase):
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
    def test_daemon_start_stop(self, process, wait_for_pidfile, node_config, ms, cfg, chdir, supports, java_home, kill):
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

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        with self.assertRaisesRegex(exceptions.LaunchError,
                                    "Elasticsearch REST API layer is not available. Forcefully terminated cluster."):
            cluster_launcher.start()
