import random
import collections
import unittest.mock as mock
from unittest import TestCase

from esrally import config, metrics
from esrally.mechanic import telemetry, team, cluster


def create_config():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    # concrete path does not matter
    cfg.add(config.Scope.application, "node", "rally.root", "/some/root/path")

    cfg.add(config.Scope.application, "reporting", "datastore.host", "localhost")
    cfg.add(config.Scope.application, "reporting", "datastore.port", "0")
    cfg.add(config.Scope.application, "reporting", "datastore.secure", False)
    cfg.add(config.Scope.application, "reporting", "datastore.user", "")
    cfg.add(config.Scope.application, "reporting", "datastore.password", "")
    # only internal devices are active
    cfg.add(config.Scope.application, "mechanic", "telemetry.devices", [])
    return cfg


class MockTelemetryDevice(telemetry.InternalTelemetryDevice):
    def __init__(self, mock_env):
        super().__init__()
        self.mock_env = mock_env

    def instrument_env(self, car, candidate_id):
        return self.mock_env


class TelemetryTests(TestCase):
    def test_merges_options_set_by_different_devices(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "mechanic", "telemetry.devices", "jfr")
        cfg.add(config.Scope.application, "system", "challenge.root.dir", "challenge-root")
        cfg.add(config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

        devices = [
            MockTelemetryDevice({"ES_JAVA_OPTS": "-Xms256M"}),
            MockTelemetryDevice({"ES_JAVA_OPTS": "-Xmx512M"}),
            MockTelemetryDevice({"ES_NET_HOST": "127.0.0.1"})
        ]

        t = telemetry.Telemetry(enabled_devices=None, devices=devices)

        default_car = team.Car(name="default-car", config_paths=["/tmp/rally-config"])
        opts = t.instrument_candidate_env(default_car, "default-node")

        self.assertTrue(opts)
        self.assertEqual(len(opts), 2)
        self.assertEqual("-Xms256M -Xmx512M", opts["ES_JAVA_OPTS"])
        self.assertEqual("127.0.0.1", opts["ES_NET_HOST"])


class MergePartsDeviceTests(TestCase):
    def setUp(self):
        self.cfg = create_config()
        self.cfg.add(config.Scope.application, "launcher", "candidate.log.dir", "/unittests/var/log/elasticsearch")

    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("builtins.open")
    @mock.patch("os.listdir")
    def test_store_nothing_if_no_metrics_present(self, listdir_mock, open_mock, metrics_store_put_value, metrics_store_put_count):
        listdir_mock.return_value = [open_mock]
        open_mock.side_effect = [
            mock.mock_open(read_data="no data to parse").return_value
        ]
        metrics_store = metrics.EsMetricsStore(self.cfg)
        merge_parts_device = telemetry.MergeParts(self.cfg, metrics_store)
        merge_parts_device.on_benchmark_stop()

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
        metrics_store = metrics.EsMetricsStore(self.cfg)
        merge_parts_device = telemetry.MergeParts(metrics_store, node_log_dir="/var/log")
        merge_parts_device.on_benchmark_stop()

        metrics_store_put_value.assert_called_with("merge_parts_total_time_doc_values", 350, "ms")
        metrics_store_put_count.assert_called_with("merge_parts_total_docs_doc_values", 1850)


class Client:
    def __init__(self, nodes=None, info=None, indices=None):
        self.nodes = nodes
        self._info = info
        self.indices = indices

    def info(self):
        return self._info


class SubClient:
    def __init__(self, stats=None, info=None):
        self._stats = stats
        self._info = info

    def stats(self, *args, **kwargs):
        return self._stats

    def info(self, *args, **kwargs):
        return self._info


class GcTests(TestCase):
    def test_sets_options_for_pre_java_9(self):
        gc = telemetry.Gc("/var/log", java_major_version=random.randint(0, 8))
        env = gc.java_opts("/var/log/defaults-node-0.gc.log")
        self.assertEqual(1, len(env))
        self.assertEqual("-Xloggc:/var/log/defaults-node-0.gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
                         "-XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintTenuringDistribution",
                         env["ES_JAVA_OPTS"])

    def test_sets_options_for_java_9_or_above(self):
        gc = telemetry.Gc("/var/log", java_major_version=random.randint(9, 999))
        env = gc.java_opts("/var/log/defaults-node-0.gc.log")
        self.assertEqual(1, len(env))
        self.assertEqual(
            "-Xlog:gc*=info,safepoint=info,age*=trace:file=/var/log/defaults-node-0.gc.log:utctime,uptimemillis,level,tags:filecount=0",
            env["ES_JAVA_OPTS"])


class EnvironmentInfoTests(TestCase):
    def setUp(self):
        self.cfg = create_config()

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_stores_cluster_level_metrics_on_attach(self, metrics_store_add_meta_info):
        nodes_info = {"nodes": collections.OrderedDict()}
        nodes_info["nodes"]["FCFjozkeTiOpN-SI88YEcg"] = {
            "name": "rally0",
            "host": "127.0.0.1",
            "attributes": {
                "group": "cold_nodes"
            },
            "os": {
                "name": "Mac OS X",
                "version": "10.11.4",
                "available_processors": 8
            },
            "jvm": {
                "version": "1.8.0_74",
                "vm_vendor": "Oracle Corporation"
            },
            "plugins": [
                {
                    "name": "ingest-geoip",
                    "version": "5.0.0",
                    "description": "Ingest processor that uses looksup geo data ...",
                    "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                    "has_native_controller": False
                }
            ]
        }
        nodes_info["nodes"]["EEEjozkeTiOpN-SI88YEcg"] = {
            "name": "rally1",
            "host": "127.0.0.1",
            "attributes": {
                "group": "hot_nodes"
            },
            "os": {
                "name": "Mac OS X",
                "version": "10.11.5",
                "available_processors": 8
            },
            "jvm": {
                "version": "1.8.0_102",
                "vm_vendor": "Oracle Corporation"
            },
            "plugins": [
                {
                    "name": "ingest-geoip",
                    "version": "5.0.0",
                    "description": "Ingest processor that uses looksup geo data ...",
                    "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                    "has_native_controller": False
                }
            ]
        }

        cluster_info = {
            "version":
                {
                    "build_hash": "abc123",
                    "number": "6.0.0-alpha1"
                }
        }

        client = Client(nodes=SubClient(info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.EnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(self.cfg, devices=[env_device])
        t.attach_to_cluster(cluster.Cluster([], [], t))
        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123"),
            mock.call(metrics.MetaInfoScope.cluster, None, "distribution_version", "6.0.0-alpha1"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "jvm_version", "1.8.0_102"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally1", "plugins", ["ingest-geoip"]),
            # can push up to cluster level as all nodes have the same plugins installed
            mock.call(metrics.MetaInfoScope.cluster, None, "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally0", "attribute_group", "cold_nodes"),
            mock.call(metrics.MetaInfoScope.node, "rally1", "attribute_group", "hot_nodes"),
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

        metrics_store = metrics.EsMetricsStore(self.cfg)
        node = cluster.Node(None, "io", "rally0", None)
        env_device = telemetry.EnvironmentInfo(None, metrics_store)
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


class ExternalEnvironmentInfoTests(TestCase):
    def setUp(self):
        self.cfg = create_config()

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_stores_cluster_level_metrics_on_attach(self, metrics_store_add_meta_info):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1"
                }
            }
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "attributes": {
                        "az": "us_east1"
                    },
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation"
                    },
                    "plugins": [
                        {
                            "name": "ingest-geoip",
                            "version": "5.0.0",
                            "description": "Ingest processor that uses looksup geo data ...",
                            "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                            "has_native_controller": False
                        }
                    ]
                }
            }
        }
        cluster_info = {
            "version":
                {
                    "build_hash": "253032b",
                    "number": "5.0.0"

                }
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.ExternalEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(devices=[env_device])
        t.attach_to_cluster(cluster.Cluster([], [], t))

        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "253032b"),
            mock.call(metrics.MetaInfoScope.cluster, None, "distribution_version", "5.0.0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "127.0.0.1"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.cluster, None, "plugins", ["ingest-geoip"]),
            mock.call(metrics.MetaInfoScope.node, "rally0", "attribute_az", "us_east1"),
            mock.call(metrics.MetaInfoScope.cluster, None, "attribute_az", "us_east1"),
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)

    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    def test_fallback_when_host_not_available(self, metrics_store_add_meta_info):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                }
            }
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
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
        cluster_info = {
            "version":
                {
                    "build_hash": "253032b",
                    "number": "5.0.0"

                }
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(self.cfg)
        env_device = telemetry.ExternalEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(self.cfg, devices=[env_device])
        t.attach_to_cluster(cluster.Cluster([], [], t))

        calls = [
            mock.call(metrics.MetaInfoScope.cluster, None, "source_revision", "253032b"),
            mock.call(metrics.MetaInfoScope.cluster, None, "distribution_version", "5.0.0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "unknown"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74")
        ]
        metrics_store_add_meta_info.assert_has_calls(calls)


class ClusterMetaDataInfoTests(TestCase):
    def setUp(self):
        self.cfg = create_config()

    def test_enriches_cluster_nodes_for_elasticsearch_after_1_x(self):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "os": {
                        "mem": {
                            "total_in_bytes": 17179869184
                        }
                    },
                    "fs": {
                        "data": [
                            {
                                "mount": "/usr/local/var/elasticsearch/data1",
                                "type": "hfs"
                            },
                            {
                                "mount": "/usr/local/var/elasticsearch/data2",
                                "type": "ntfs"
                            }
                        ]
                    }
                }
            }
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1",
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8,
                        "allocated_processors": 4
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation"
                    },
                    "plugins": [
                        {
                            "name": "analysis-icu",
                            "version": "5.0.0",
                            "description": "The ICU Analysis plugin integrates Lucene ICU module ...",
                            "classname": "org.elasticsearch.plugin.analysis.icu.AnalysisICUPlugin",
                            "has_native_controller": False
                        },
                        {
                            "name": "ingest-geoip",
                            "version": "5.0.0",
                            "description": "Ingest processor that uses looksup geo data ...",
                            "classname": "org.elasticsearch.ingest.geoip.IngestGeoIpPlugin",
                            "has_native_controller": False
                        },
                        {
                            "name": "ingest-user-agent",
                            "version": "5.0.0",
                            "description": "Ingest processor that extracts information from a user agent",
                            "classname": "org.elasticsearch.ingest.useragent.IngestUserAgentPlugin",
                            "has_native_controller": False
                        }
                    ]
                }
            }
        }
        cluster_info = {
            "version":
                {
                    "build_hash": "253032b",
                    "number": "5.0.0"
                }
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)

        t = telemetry.Telemetry(devices=[telemetry.ClusterMetaDataInfo(client)])

        c = cluster.Cluster(hosts=[{"host": "localhost", "port": 39200}],
                            nodes=[cluster.Node(process=None, host_name="local", node_name="rally0", telemetry=None)],
                            telemetry=t)

        t.attach_to_cluster(c)

        self.assertEqual("5.0.0", c.distribution_version)
        self.assertEqual("253032b", c.source_revision)
        self.assertEqual(1, len(c.nodes))
        n = c.nodes[0]
        self.assertEqual("127.0.0.1", n.ip)
        self.assertEqual("Mac OS X", n.os["name"])
        self.assertEqual("10.11.4", n.os["version"])
        self.assertEqual("Oracle Corporation", n.jvm["vendor"])
        self.assertEqual("1.8.0_74", n.jvm["version"])
        self.assertEqual(8, n.cpu["available_processors"])
        self.assertEqual(4, n.cpu["allocated_processors"])
        self.assertEqual(17179869184, n.memory["total_bytes"])

        self.assertEqual(2, len(n.fs))
        self.assertEqual("/usr/local/var/elasticsearch/data1", n.fs[0]["mount"])
        self.assertEqual("hfs", n.fs[0]["type"])
        self.assertEqual("unknown", n.fs[0]["spins"])
        self.assertEqual("/usr/local/var/elasticsearch/data2", n.fs[1]["mount"])
        self.assertEqual("ntfs", n.fs[1]["type"])
        self.assertEqual("unknown", n.fs[1]["spins"])
        self.assertEqual(["analysis-icu", "ingest-geoip", "ingest-user-agent"], n.plugins)

    def test_enriches_cluster_nodes_for_elasticsearch_1_x(self):
        nodes_stats = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "fs": {
                        "data": [
                            {
                                "mount": "/usr/local/var/elasticsearch/data1",
                                "type": "hfs"
                            },
                            {
                                "mount": "/usr/local/var/elasticsearch/data2",
                                "type": "ntfs"
                            }
                        ]
                    }
                }
            }
        }

        nodes_info = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1",
                    "os": {
                        "name": "Mac OS X",
                        "version": "10.11.4",
                        "available_processors": 8,
                        "mem": {
                            "total_in_bytes": 17179869184
                        }
                    },
                    "jvm": {
                        "version": "1.8.0_74",
                        "vm_vendor": "Oracle Corporation"
                    }
                }
            }
        }
        cluster_info = {
            "version":
                {
                    "build_hash": "c730b59357f8ebc555286794dcd90b3411f517c9",
                    "number": "1.7.5"
                }
        }
        client = Client(nodes=SubClient(stats=nodes_stats, info=nodes_info), info=cluster_info)

        t = telemetry.Telemetry(devices=[telemetry.ClusterMetaDataInfo(client)])

        c = cluster.Cluster(hosts=[{"host": "localhost", "port": 39200}],
                            nodes=[cluster.Node(process=None, host_name="local", node_name="rally0", telemetry=None)],
                            telemetry=t)

        t.attach_to_cluster(c)

        self.assertEqual("1.7.5", c.distribution_version)
        self.assertEqual("c730b59357f8ebc555286794dcd90b3411f517c9", c.source_revision)
        self.assertEqual(1, len(c.nodes))
        n = c.nodes[0]
        self.assertEqual("127.0.0.1", n.ip)
        self.assertEqual("Mac OS X", n.os["name"])
        self.assertEqual("10.11.4", n.os["version"])
        self.assertEqual("Oracle Corporation", n.jvm["vendor"])
        self.assertEqual("1.8.0_74", n.jvm["version"])
        self.assertEqual(8, n.cpu["available_processors"])
        self.assertIsNone(n.cpu["allocated_processors"])
        self.assertEqual(17179869184, n.memory["total_bytes"])

        self.assertEqual(2, len(n.fs))
        self.assertEqual("/usr/local/var/elasticsearch/data1", n.fs[0]["mount"])
        self.assertEqual("hfs", n.fs[0]["type"])
        self.assertEqual("unknown", n.fs[0]["spins"])
        self.assertEqual("/usr/local/var/elasticsearch/data2", n.fs[1]["mount"])
        self.assertEqual("ntfs", n.fs[1]["type"])
        self.assertEqual("unknown", n.fs[1]["spins"])


class NodeStatsTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_stores_only_diff_of_gc_times(self, metrics_store_node_level, metrics_store_cluster_level):
        nodes_stats_at_start = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "gc": {
                            "collectors": {
                                "old": {
                                    "collection_time_in_millis": 1000
                                },
                                "young": {
                                    "collection_time_in_millis": 500
                                }
                            }
                        }
                    }
                }
            }
        }

        client = Client(nodes=SubClient(nodes_stats_at_start))
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.NodeStats(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_start()
        # now we'd need to change the node stats response
        nodes_stats_at_end = {
            "nodes": {
                "FCFjozkeTiOpN-SI88YEcg": {
                    "name": "rally0",
                    "host": "127.0.0.1",
                    "jvm": {
                        "gc": {
                            "collectors": {
                                "old": {
                                    "collection_time_in_millis": 2500
                                },
                                "young": {
                                    "collection_time_in_millis": 1200
                                }
                            }
                        }
                    }
                }
            }
        }
        client.nodes = SubClient(nodes_stats_at_end)
        t.on_benchmark_stop()

        metrics_store_node_level.assert_has_calls([
            mock.call("rally0", "node_young_gen_gc_time", 700, "ms"),
            mock.call("rally0", "node_old_gen_gc_time", 1500, "ms")
        ])

        metrics_store_cluster_level.assert_has_calls([
            mock.call("node_total_young_gen_gc_time", 700, "ms"),
            mock.call("node_total_old_gen_gc_time", 1500, "ms")
        ])


class IndexStatsTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    def test_stores_available_index_stats(self, metrics_store_cluster_count, metrics_store_cluster_value):
        client = Client(indices=SubClient({
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 0
                    },
                    "merges": {
                        "total_time_in_millis": 0,
                        "total_throttled_time_in_millis": 0
                    },
                    "indexing": {
                        "index_time_in_millis": 0
                    },
                    "refresh": {
                        "total_time_in_millis": 0
                    },
                    "flush": {
                        "total_time_in_millis": 0
                    }
                }
            }
        }))
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexStats(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        t.on_benchmark_start()

        client.indices = SubClient({
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 5,
                        "memory_in_bytes": 2048,
                        "stored_fields_memory_in_bytes": 1024,
                        "doc_values_memory_in_bytes": 128,
                        "terms_memory_in_bytes": 256,
                        "points_memory_in_bytes": 512
                    },
                    "merges": {
                        "total_time_in_millis": 300,
                        "total_throttled_time_in_millis": 120
                    },
                    "indexing": {
                        "index_time_in_millis": 2000
                    },
                    "refresh": {
                        "total_time_in_millis": 200
                    },
                    "flush": {
                        "total_time_in_millis": 100
                    }
                }
            }
        })

        t.on_benchmark_stop()

        metrics_store_cluster_count.assert_has_calls([
            mock.call("segments_count", 5)
        ])
        metrics_store_cluster_value.assert_has_calls([
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("merges_total_time", 300, "ms"),
            mock.call("merges_total_throttled_time", 120, "ms"),
            mock.call("indexing_total_time", 2000, "ms"),
            mock.call("refresh_total_time", 200, "ms"),
            mock.call("flush_total_time", 100, "ms"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
            # we don't have norms, so nothing should have been called
            mock.call("segments_points_memory_in_bytes", 512, "byte"),
        ], any_order=True)

    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    def test_index_stats_are_per_lap(self, metrics_store_cluster_count, metrics_store_cluster_value):
        client = Client(indices=SubClient({
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 0
                    },
                    "merges": {
                        "total_time_in_millis": 0,
                        "total_throttled_time_in_millis": 0
                    },
                    "indexing": {
                        "index_time_in_millis": 0
                    },
                    "refresh": {
                        "total_time_in_millis": 0
                    },
                    "flush": {
                        "total_time_in_millis": 0
                    }
                }
            }
        }))
        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexStats(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        # lap 1
        t.on_benchmark_start()

        client.indices = SubClient({
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 5,
                        "memory_in_bytes": 2048,
                        "stored_fields_memory_in_bytes": 1024,
                        "doc_values_memory_in_bytes": 128,
                        "terms_memory_in_bytes": 256
                    },
                    "merges": {
                        "total_time_in_millis": 300,
                        "total_throttled_time_in_millis": 120
                    },
                    "indexing": {
                        "index_time_in_millis": 2000
                    },
                    "refresh": {
                        "total_time_in_millis": 200
                    },
                    "flush": {
                        "total_time_in_millis": 100
                    }
                }
            }
        })

        t.on_benchmark_stop()
        # lap 2
        t.on_benchmark_start()

        client.indices = SubClient({
            "_all": {
                "primaries": {
                    "segments": {
                        "count": 7,
                        "memory_in_bytes": 2048,
                        "stored_fields_memory_in_bytes": 1024,
                        "doc_values_memory_in_bytes": 128,
                        "terms_memory_in_bytes": 256
                    },
                    "merges": {
                        "total_time_in_millis": 900,
                        "total_throttled_time_in_millis": 120
                    },
                    "indexing": {
                        "index_time_in_millis": 8000
                    },
                    "refresh": {
                        "total_time_in_millis": 500
                    },
                    "flush": {
                        "total_time_in_millis": 300
                    }
                }
            }
        })

        t.on_benchmark_stop()

        metrics_store_cluster_value.assert_has_calls([
            # 1st lap
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("merges_total_time", 300, "ms"),
            mock.call("merges_total_throttled_time", 120, "ms"),
            mock.call("indexing_total_time", 2000, "ms"),
            mock.call("refresh_total_time", 200, "ms"),
            mock.call("flush_total_time", 100, "ms"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
            # we don't have norms or points, so nothing should have been called

            # 2nd lap
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("merges_total_time", 600, "ms"),
            mock.call("merges_total_throttled_time", 0, "ms"),
            mock.call("indexing_total_time", 6000, "ms"),
            mock.call("refresh_total_time", 300, "ms"),
            mock.call("flush_total_time", 200, "ms"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
        ], any_order=True)


class IndexSizeTests(TestCase):
    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_stores_index_size_for_data_paths(self, run_subprocess, metrics_store_cluster_count, get_size):
        get_size.side_effect = [2048, 16384]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexSize(["/var/elasticsearch/data/1", "/var/elasticsearch/data/2"], metrics_store)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        t.attach_to_cluster(None)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_cluster(None)

        metrics_store_cluster_count.assert_has_calls([
            mock.call("final_index_size_bytes", 18432, "byte")
        ])

        run_subprocess.assert_has_calls([
            mock.call("find /var/elasticsearch/data/1 -ls", header="index files:"),
            mock.call("find /var/elasticsearch/data/2 -ls", header="index files:")
        ])

    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    @mock.patch("esrally.utils.process.run_subprocess_with_logging")
    def test_stores_nothing_if_no_data_path(self, run_subprocess, metrics_store_cluster_count, get_size):
        get_size.return_value = 2048

        cfg = create_config()

        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexSize(data_paths=[], metrics_store=metrics_store)
        t = telemetry.Telemetry(devices=[device])
        t.attach_to_cluster(None)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_cluster(None)

        run_subprocess.assert_not_called()
        metrics_store_cluster_count.assert_not_called()
        get_size.assert_not_called()
