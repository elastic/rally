from unittest import TestCase

from esrally import config, exceptions
from esrally.mechanic import launcher
from esrally.track import track


class MockMetricsStore:
    def add_meta_info(self, scope, scope_key, key, value):
        pass


class MockCluster:
    def __init__(self, hosts, nodes, metrics_store, telemetry):
        self.hosts = hosts
        self.nodes = nodes
        self.metrics_store = metrics_store
        self.telemetry = telemetry

    def info(self):
        return {
            "version":
                {
                    "build_hash": "abc123"
                }
        }

    def nodes_stats(self, *args, **kwargs):
        return {
            "cluster_name": "rally-benchmark-cluster",
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "Nefarius",
                    "host": "127.0.0.1"
                    }
            }
        }

    def wait_for_status_green(self):
        pass


class MockClusterFactory:
    def create(self, hosts, nodes, metrics_store, telemetry):
        return MockCluster(hosts, nodes, metrics_store, telemetry)


class MockTrackSetup:
    def __init__(self):
        self.benchmark = {}


class ExternalLauncherTests(TestCase):
    def test_setup_external_cluster_single_node(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "launcher", "external.target.hosts", ["localhost:9200"])

        m = launcher.ExternalLauncher(cfg, cluster_factory_class=MockClusterFactory)
        cluster = m.start(None, MockTrackSetup(), MockMetricsStore())

        self.assertEqual(cluster.hosts, [{"host": "localhost", "port": "9200"}])

    def test_setup_external_cluster_multiple_nodes(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "launcher", "external.target.hosts",
                ["search.host-a.internal:9200", "search.host-b.internal:9200"])

        m = launcher.ExternalLauncher(cfg, cluster_factory_class=MockClusterFactory)
        cluster = m.start(None, MockTrackSetup(), MockMetricsStore())

        self.assertEqual(cluster.hosts,
                         [{"host": "search.host-a.internal", "port": "9200"}, {"host": "search.host-b.internal", "port": "9200"}])

    def test_raise_system_setup_exception_on_invalid_list(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "launcher", "external.target.hosts",
                ["search.host-a.internal", "search.host-b.internal:9200"])

        m = launcher.ExternalLauncher(cfg, cluster_factory_class=MockClusterFactory)
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            m.start(None, MockTrackSetup(), MockMetricsStore())
            self.assertTrue("Could not initialize external cluster. Invalid format for [search.host-a.internal, "
                            "search.host-b.internal:9200]. Expected a comma-separated list of host:port pairs, "
                            "e.g. host1:9200,host2:9200." in ctx.exception)
