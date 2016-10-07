from unittest import TestCase

from esrally import config
from esrally.mechanic import launcher


class MockMetricsStore:
    def add_meta_info(self, scope, scope_key, key, value):
        pass


class MockClientFactory:
    def __init__(self, hosts, client_options):
        pass

    def create(self):
        return MockClient()


class MockClient:
    def __init__(self):
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
        return self._info


class SubClient:
    def __init__(self, info):
        self._info = info

    def stats(self, *args, **kwargs):
        return self._info

    def info(self, *args, **kwargs):
        return self._info


class ExternalLauncherTests(TestCase):
    def test_setup_external_cluster_single_node(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "launcher", "external.target.hosts", ["10.0.0.10:9200", "10.0.0.11:9200"])
        cfg.add(config.Scope.application, "launcher", "client.options", [])

        m = launcher.ExternalLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        m.start()

        # automatically determined by launcher on attach
        self.assertEqual(cfg.opts("source", "distribution.version"), "5.0.0")

    def test_setup_external_cluster_multiple_nodes(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        cfg.add(config.Scope.application, "launcher", "external.target.hosts", ["10.0.0.10:9200", "10.0.0.11:9200"])
        cfg.add(config.Scope.application, "launcher", "client.options", [])
        cfg.add(config.Scope.application, "source", "distribution.version", "2.3.3")

        m = launcher.ExternalLauncher(cfg, MockMetricsStore(), client_factory_class=MockClientFactory)
        m.start()
        # did not change user defined value
        self.assertEqual(cfg.opts("source", "distribution.version"), "2.3.3")
