import unittest.mock as mock
from unittest import TestCase

from esrally import config, metrics, telemetry, cluster, car


class MockClientFactory:
    def __init__(self, hosts):
        pass

    def create(self):
        return None


class MockTelemetryDevice(telemetry.InternalTelemetryDevice):
    def __init__(self, cfg, metrics_store, mock_env):
        super().__init__(cfg, metrics_store)
        self.mock_env = mock_env

    def instrument_env(self, car, candidate_id):
        return self.mock_env


class TelemetryTests(TestCase):
    def test_instrument_candidate_env(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", "jfr")
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "challenge-root")
        cfg.add(config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

        # we don't need one for this test
        metrics_store = None

        devices = [
            MockTelemetryDevice(cfg, metrics_store, {"ES_JAVA_OPTS": "-Xms256M"}),
            MockTelemetryDevice(cfg, metrics_store, {"ES_JAVA_OPTS": "-Xmx512M"}),
            MockTelemetryDevice(cfg, metrics_store, {"ES_NET_HOST": "127.0.0.1"})
        ]

        t = telemetry.Telemetry(cfg, metrics_store, devices)

        default_car = car.Car(name="default-car")
        opts = t.instrument_candidate_env(default_car, "default-node")

        self.assertTrue(opts)
        self.assertEqual(len(opts), 2)
        self.assertEqual("-Xms256M -Xmx512M", opts["ES_JAVA_OPTS"])
        self.assertEqual("127.0.0.1", opts["ES_NET_HOST"])


class MergePartsDeviceTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("builtins.open")
    @mock.patch("os.listdir")
    def test_store_nothing_if_no_metrics_present(self, listdir_mock, open_mock, metrics_store_put_value, metrics_store_put_count):
        listdir_mock.return_value = [open_mock]
        open_mock.side_effect = [
            mock.mock_open(read_data="no data to parse").return_value
        ]
        cfg = self.create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        merge_parts_device = telemetry.MergeParts(cfg, metrics_store)
        merge_parts_device.on_benchmark_stop(phase=None)

        metrics_store_put_value.assert_not_called()
        metrics_store_put_count.assert_not_called()

    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("builtins.open")
    @mock.patch("os.listdir")
    def test_store_calculated_metrics(self, listdir_mock, open_mock, metrics_store_put_value, metrics_store_put_count):
        log_file = '''
        INFO: System starting up
        INFO: 100 msec to merge doc values [500 docs]
        INFO: Something unrelated
        INFO: 250 msec to merge doc values [1350 docs]
        INFO: System shutting down
        '''
        listdir_mock.return_value = [open_mock]
        open_mock.side_effect = [
            mock.mock_open(read_data=log_file).return_value
        ]
        config = self.create_config()
        metrics_store = metrics.EsMetricsStore(config)
        merge_parts_device = telemetry.MergeParts(config, metrics_store)
        merge_parts_device.on_benchmark_stop(phase=None)

        metrics_store_put_value.assert_called_with("merge_parts_total_time_doc_values", 350, "ms")
        metrics_store_put_count.assert_called_with("merge_parts_total_docs_doc_values", 1850)

    def create_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "launcher", "candidate.log.dir", "/unittests/var/log/elasticsearch")
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "reporting", "datastore.host", "localhost")
        cfg.add(config.Scope.application, "reporting", "datastore.port", "0")
        cfg.add(config.Scope.application, "reporting", "datastore.secure", False)
        cfg.add(config.Scope.application, "reporting", "datastore.user", "")
        cfg.add(config.Scope.application, "reporting", "datastore.password", "")
        return cfg


class EnvironmentInfoTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    @mock.patch("esrally.cluster.Cluster.info")
    @mock.patch("esrally.cluster.Cluster.nodes_info")
    def test_stores_cluster_level_metrics_on_attach(self, nodes_info, cluster_info, metrics_store_add_meta_info):
        nodes_info.return_value = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
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
        }
        cluster_info.return_value = {
            "version":
                {
                    "build_hash": "abc123"
                }
        }
        cfg = self.create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        env_device = telemetry.EnvironmentInfo(cfg, metrics_store)
        t = telemetry.Telemetry(cfg, metrics_store, devices=[env_device])
        t.attach_to_cluster(cluster.Cluster([{"host": "::1:9200"}], [], metrics_store, t, client_factory_class=MockClientFactory))

        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74")
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    @mock.patch("esrally.utils.sysstats.os_name")
    @mock.patch("esrally.utils.sysstats.os_version")
    @mock.patch("esrally.utils.sysstats.logical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.physical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.cpu_model")
    def test_stores_node_level_metrics_on_attach(self, cpu_model, physical_cpu_cores, logical_cpu_cores, os_version, os_name,
                                                 metrics_store_add_meta_info):
        cpu_model.return_value = "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
        physical_cpu_cores.return_value = 4
        logical_cpu_cores.return_value = 8
        os_version.return_value = "4.2.0-18-generic"
        os_name.return_value = "Linux"

        cfg = self.create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        node = cluster.Node(None, "io", "rally0", None)
        env_device = telemetry.EnvironmentInfo(cfg, metrics_store)
        env_device.attach_to_node(node)

        calls = [
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Linux"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "4.2.0-18-generic"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_physical_cores", 4),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_model", "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "io"),
        ]

        metrics_store_add_meta_info.assert_has_calls(calls)

    def create_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "reporting", "datastore.host", "localhost")
        cfg.add(config.Scope.application, "reporting", "datastore.port", "0")
        cfg.add(config.Scope.application, "reporting", "datastore.secure", False)
        cfg.add(config.Scope.application, "reporting", "datastore.user", "")
        cfg.add(config.Scope.application, "reporting", "datastore.password", "")
        # only internal devices are active
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        return cfg


class ExternalEnvironmentInfoTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    @mock.patch("esrally.cluster.Cluster.info")
    @mock.patch("esrally.cluster.Cluster.nodes_info")
    @mock.patch("esrally.cluster.Cluster.nodes_stats")
    def test_stores_cluster_level_metrics_on_attach(self, nodes_stats, nodes_info, cluster_info, metrics_store_add_meta_info):
        nodes_stats.return_value = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1"
                }
            }
        }

        nodes_info.return_value = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
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
        }
        cluster_info.return_value = {
            "version":
                {
                    "build_hash": "abc123"
                }
        }
        cfg = self.create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        env_device = telemetry.ExternalEnvironmentInfo(cfg, metrics_store)
        t = telemetry.Telemetry(cfg, metrics_store, devices=[env_device])
        t.attach_to_cluster(cluster.Cluster([{"host": "::1:9200"}], [], metrics_store, t, client_factory_class=MockClientFactory))

        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "127.0.0.1"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74")
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)

    def create_config(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "reporting", "datastore.host", "localhost")
        cfg.add(config.Scope.application, "reporting", "datastore.port", "0")
        cfg.add(config.Scope.application, "reporting", "datastore.secure", False)
        cfg.add(config.Scope.application, "reporting", "datastore.user", "")
        cfg.add(config.Scope.application, "reporting", "datastore.password", "")
        # only internal devices are active
        cfg.add(config.Scope.application, "telemetry", "devices", [])
        return cfg
