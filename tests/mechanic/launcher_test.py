from unittest import TestCase, mock

from esrally import config, exceptions
from esrally.utils import opts
from esrally.mechanic import launcher


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


class ExternalLauncherTests(TestCase):
    test_host = opts.TargetHosts("127.0.0.1:9200,10.17.0.5:19200")
    client_options = opts.ClientOptions("timeout:60")

    def test_setup_external_cluster_single_node(self):
        cfg = config.Config()

        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options",self.client_options)

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

    def test_launches_cluster_with_post_launch_handler(self):
        on_post_launch = mock.Mock()
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(),
                                                    on_post_launch=on_post_launch, client_factory_class=MockClientFactory)
        cluster = cluster_launcher.start()

        self.assertEqual([{"host": "10.0.0.10", "port":9200}, {"host": "10.0.0.11", "port":9200}], cluster.hosts)
        self.assertIsNotNone(cluster.telemetry)
        # this requires at least Python 3.6
        # on_post_launch.assert_called_once()
        self.assertEqual(1, on_post_launch.call_count)

    def test_launches_cluster_without_post_launch_handler(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        cfg.add(config.Scope.application, "client", "options", self.client_options)
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        cluster = cluster_launcher.start()

        self.assertEqual([{"host": "10.0.0.10", "port":9200}, {"host": "10.0.0.11", "port":9200}], cluster.hosts)
        self.assertIsNotNone(cluster.telemetry)

    @mock.patch("time.sleep")
    def test_error_on_cluster_launch(self, sleep):
        on_post_launch = mock.Mock()
        cfg = config.Config()
        cfg.add(config.Scope.application, "client", "hosts", self.test_host)
        # Simulate that the client will raise an error upon startup
        cfg.add(config.Scope.application, "client", "options", opts.ClientOptions("raise-error-on-info:true"))
        #cfg.add(config.Scope.application, "client", "options", {"raise-error-on-info": True})
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
        cfg.add(config.Scope.application, "mechanic", "telemetry.params", {})

        cluster_launcher = launcher.ClusterLauncher(cfg, MockMetricsStore(),
                                                    on_post_launch=on_post_launch, client_factory_class=MockClientFactory)
        with self.assertRaisesRegex(exceptions.LaunchError,
                                    "Elasticsearch REST API layer is not available. Forcefully terminated cluster."):
            cluster_launcher.start()
        self.assertEqual(0, on_post_launch.call_count)
