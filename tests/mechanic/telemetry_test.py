import random
import collections
import unittest.mock as mock
import elasticsearch

from unittest import TestCase
from esrally import config, metrics, exceptions
from esrally.mechanic import telemetry, team, cluster
from esrally.metrics import MetaInfoScope


def create_config():
    cfg = config.Config()
    cfg.add(config.Scope.application, "system", "env.name", "unittest")
    cfg.add(config.Scope.application, "track", "params", {})
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

        default_car = team.Car(names="default-car", root_path=None, config_paths=["/tmp/rally-config"])
        opts = t.instrument_candidate_env(default_car, "default-node")

        self.assertTrue(opts)
        self.assertEqual(len(opts), 2)
        self.assertEqual("-Xms256M -Xmx512M", opts["ES_JAVA_OPTS"])
        self.assertEqual("127.0.0.1", opts["ES_NET_HOST"])


class StartupTimeTests(TestCase):
    @mock.patch("esrally.time.StopWatch")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    def test_store_calculated_metrics(self, metrics_store_put_value, stop_watch):
        stop_watch.total_time.return_value = 2
        metrics_store = metrics.EsMetricsStore(create_config())
        node = cluster.Node(None, "io", "rally0", None)
        startup_time = telemetry.StartupTime(metrics_store)
        # replace with mock
        startup_time.timer = stop_watch

        startup_time.on_pre_node_start(node.node_name)
        # ... nodes starts up ...
        startup_time.attach_to_node(node)

        metrics_store_put_value.assert_called_with("rally0", "node_startup_time", 2, "s")


class MergePartsDeviceTests(TestCase):
    def setUp(self):
        self.cfg = create_config()
        self.cfg.add(config.Scope.application, "launcher", "candidate.log.dir", "/unittests/var/log/elasticsearch")

    @mock.patch("esrally.metrics.EsMetricsStore.put_count_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
    @mock.patch("builtins.open")
    @mock.patch("os.listdir")
    def test_store_nothing_if_no_metrics_present(self, listdir_mock, open_mock, metrics_store_put_value, metrics_store_put_count):
        listdir_mock.return_value = [open_mock]
        open_mock.side_effect = [
            mock.mock_open(read_data="no data to parse").return_value
        ]
        metrics_store = metrics.EsMetricsStore(self.cfg)
        node = cluster.Node(None, "io", "rally0", None)
        merge_parts_device = telemetry.MergeParts(self.cfg, metrics_store)
        merge_parts_device.attach_to_node(node)
        merge_parts_device.on_benchmark_stop()

        self.assertEqual(0, metrics_store_put_value.call_count)
        self.assertEqual(0, metrics_store_put_count.call_count)

    @mock.patch("esrally.metrics.EsMetricsStore.put_count_node_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_node_level")
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
        node = cluster.Node(None, "io", "rally0", None)
        merge_parts_device = telemetry.MergeParts(metrics_store, node_log_dir="/var/log")
        merge_parts_device.attach_to_node(node)
        merge_parts_device.on_benchmark_stop()

        metrics_store_put_value.assert_called_with("rally0", "merge_parts_total_time_doc_values", 350, "ms")
        metrics_store_put_count.assert_called_with("rally0", "merge_parts_total_docs_doc_values", 1850)


class Client:
    def __init__(self, nodes=None, info=None, indices=None, transport_client=None):
        self.nodes = nodes
        self._info = info
        self.indices = indices
        if transport_client:
            self.transport = transport_client

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


class TransportClient:
    def __init__(self, response=None, force_error=False):
        self._response = response
        self._force_error = force_error

    def perform_request(self, *args, **kwargs):
        if self._force_error:
            raise elasticsearch.TransportError
        else:
            return self._response


class JfrTests(TestCase):
    def test_sets_options_for_pre_java_9_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(0, 8))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                         "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,"
                         "dumponexitpath=/var/log/test-recording.jfr -XX:StartFlightRecording=defaultrecording=true", java_opts)

    def test_sets_options_for_java_9_or_10_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(9, 10))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UnlockCommercialFeatures "
                         "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename=/var/log/test-recording.jfr",
                         java_opts)

    def test_sets_options_for_java_11_or_above_default_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={}, log_root="/var/log", java_major_version=random.randint(11, 999))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                         "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,filename=/var/log/test-recording.jfr",
                         java_opts)

    def test_sets_options_for_pre_java_9_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={"recording-template": "profile"},
                                       log_root="/var/log",
                                       java_major_version=random.randint(0, 8))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UnlockCommercialFeatures -XX:+FlightRecorder "
                         "-XX:FlightRecorderOptions=disk=true,maxage=0s,maxsize=0,dumponexit=true,"
                         "dumponexitpath=/var/log/test-recording.jfr -XX:StartFlightRecording=defaultrecording=true,settings=profile",
                         java_opts)

    def test_sets_options_for_java_9_or_10_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={"recording-template": "profile"},
                                       log_root="/var/log",
                                       java_major_version=random.randint(9, 10))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -XX:+UnlockCommercialFeatures "
                         "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,"
                         "filename=/var/log/test-recording.jfr,settings=profile",
                         java_opts)

    def test_sets_options_for_java_11_or_above_custom_recording_template(self):
        jfr = telemetry.FlightRecorder(telemetry_params={"recording-template": "profile"},
                                       log_root="/var/log",
                                       java_major_version=random.randint(11, 999))
        java_opts = jfr.java_opts("/var/log/test-recording.jfr")
        self.assertEqual("-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                         "-XX:StartFlightRecording=maxsize=0,maxage=0s,disk=true,dumponexit=true,"
                         "filename=/var/log/test-recording.jfr,settings=profile",
                         java_opts)


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


class CcrStatsTests(TestCase):
    def test_negative_sample_interval_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {
            "ccr-stats-sample-interval": -1 * random.random()
        }
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "The telemetry parameter 'ccr-stats-sample-interval' must be greater than zero but was .*\."):
            telemetry.CcrStats(telemetry_params, clients, metrics_store)

    def test_wrong_cluster_name_in_ccr_stats_indices_forbidden(self):
        clients = {"default": Client(), "cluster_b": Client()}
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {
            "ccr-stats-indices":{
                "default": ["leader"],
                "wrong_cluster_name": ["follower"]
            }
        }
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "The telemetry parameter 'ccr-stats-indices' must be a JSON Object with keys matching "
                                    "the cluster names \[{}] specified in --target-hosts "
                                    "but it had \[wrong_cluster_name\].".format(",".join(sorted(clients.keys())))
                                    ):
            telemetry.CcrStats(telemetry_params, clients, metrics_store)


class CcrStatsRecorderTests(TestCase):
    java_signed_maxlong = (2**63) - 1

    def test_raises_exception_on_transport_error(self):
        client = Client(transport_client=TransportClient(response={}, force_error=True))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        with self.assertRaisesRegexp(exceptions.RallyError,
                                     "A transport error occurred while collecting CCR stats from the endpoint \[/_ccr/stats\] on "
                                     "cluster \[remote\]"):
            telemetry.CcrStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=1).record()

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_ccr_stats(self, metrics_store_put_doc):
        java_signed_maxlong = CcrStatsRecorderTests.java_signed_maxlong

        shard_id = random.randint(0, 999)
        leader_index = "leader_cluster:leader"
        follower_index = "follower"
        leader_global_checkpoint = random.randint(0, java_signed_maxlong)
        leader_max_seq_no = random.randint(0, java_signed_maxlong)
        follower_global_checkpoint = random.randint(0, java_signed_maxlong)
        follower_max_seq_no = random.randint(0, java_signed_maxlong)
        last_requested_seq_no = random.randint(0, java_signed_maxlong)
        number_of_concurrent_reads = random.randint(0, java_signed_maxlong)
        number_of_concurrent_writes = random.randint(0, java_signed_maxlong)
        number_of_queued_writes = random.randint(0, java_signed_maxlong)
        mapping_version = random.randint(0, java_signed_maxlong)
        total_fetch_time_millis = random.randint(0, java_signed_maxlong)
        number_of_successful_fetches = random.randint(0, java_signed_maxlong)
        number_of_failed_fetches = random.randint(0, java_signed_maxlong)
        operations_received = random.randint(0, java_signed_maxlong)
        total_transferred_bytes = random.randint(0, java_signed_maxlong)
        total_index_time_millis = random.randint(0, java_signed_maxlong)
        time_since_last_fetch_millis = random.randint(0, java_signed_maxlong)
        number_of_successful_bulk_operations = random.randint(0, java_signed_maxlong)
        number_of_failed_bulk_operations = random.randint(0, java_signed_maxlong)
        number_of_operations_indexed = random.randint(0, java_signed_maxlong)

        ccr_stats_follower_response = {
            "indices": [
                {
                    "index": follower_index,
                    "shards": [
                        {
                            "shard_id": shard_id,
                            "follower_global_checkpoint": follower_global_checkpoint,
                            "leader_index": leader_index,
                            "follower_index": follower_index,
                            "follower_max_seq_no": follower_max_seq_no,
                            "mapping_version": mapping_version,
                            "last_requested_seq_no": last_requested_seq_no,
                            "leader_global_checkpoint": leader_global_checkpoint,
                            "leader_max_seq_no": leader_max_seq_no,
                            "number_of_concurrent_reads": number_of_concurrent_reads,
                            "number_of_concurrent_writes": number_of_concurrent_writes,
                            "number_of_failed_bulk_operations": number_of_failed_bulk_operations,
                            "number_of_failed_fetches": number_of_failed_fetches,
                            "number_of_operations_indexed": number_of_operations_indexed,
                            "number_of_queued_writes": number_of_queued_writes,
                            "number_of_successful_bulk_operations": number_of_successful_bulk_operations,
                            "number_of_successful_fetches": number_of_successful_fetches,
                            "operations_received": operations_received,
                            "time_since_last_fetch_mills": time_since_last_fetch_millis,
                            "total_fetch_time_millis": total_fetch_time_millis,
                            "total_index_time_millis": total_index_time_millis,
                            "total_transferred_bytes": total_transferred_bytes
                        }
                    ]
                }
            ]
        }

        client = Client(transport_client=TransportClient(response=ccr_stats_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder(cluster_name="remote", client=client, metrics_store=metrics_store, sample_interval=1)
        recorder.record()

        shard_metadata = {
            "cluster": "remote",
            "index": follower_index,
            "shard": shard_id,
            "name": "ccr-stats"
        }

        metrics_store_put_doc.assert_called_with(
            ccr_stats_follower_response["indices"][0]["shards"][0],
            level=MetaInfoScope.cluster,
            meta_data=shard_metadata
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_ccr_stats_many_shards(self, metrics_store_put_doc):
        java_signed_maxlong = CcrStatsRecorderTests.java_signed_maxlong

        leader_index="leader_cluster:leader"
        follower_index="follower"
        shard_range = range(2)
        leader_global_checkpoint = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        leader_max_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        follower_global_checkpoint = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        follower_max_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        last_requested_seq_no = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_concurrent_reads = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_concurrent_writes = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_queued_writes = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        mapping_version = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_fetch_time_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_successful_fetches = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_failed_fetches = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        operations_received = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_transferred_bytes = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        total_index_time_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        time_since_last_fetch_millis = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_successful_bulk_operations = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_failed_bulk_operations = [random.randint(0, java_signed_maxlong) for _ in shard_range]
        number_of_operations_indexed = [random.randint(0, java_signed_maxlong) for _ in shard_range]

        ccr_stats_follower_response = {
            "indices": [
                {
                    "index": follower_index,
                    "shards": [
                        {
                            "shard_id": shard_id,
                            "follower_global_checkpoint": follower_global_checkpoint[shard_id],
                            "leader_index": leader_index,
                            "follower_index": follower_index,
                            "follower_max_seq_no": follower_max_seq_no[shard_id],
                            "mapping_version": mapping_version[shard_id],
                            "last_requested_seq_no": last_requested_seq_no[shard_id],
                            "leader_global_checkpoint": leader_global_checkpoint[shard_id],
                            "leader_max_seq_no": leader_max_seq_no[shard_id],
                            "number_of_concurrent_reads": number_of_concurrent_reads[shard_id],
                            "number_of_concurrent_writes": number_of_concurrent_writes[shard_id],
                            "number_of_failed_bulk_operations": number_of_failed_bulk_operations[shard_id],
                            "number_of_failed_fetches": number_of_failed_fetches[shard_id],
                            "number_of_operations_indexed": number_of_operations_indexed[shard_id],
                            "number_of_queued_writes": number_of_queued_writes[shard_id],
                            "number_of_successful_bulk_operations": number_of_successful_bulk_operations[shard_id],
                            "number_of_successful_fetches": number_of_successful_fetches[shard_id],
                            "operations_received": operations_received[shard_id],
                            "total_fetch_time_millis": total_fetch_time_millis[shard_id],
                            "total_index_time_millis": total_index_time_millis[shard_id],
                            "total_transferred_bytes": total_transferred_bytes[shard_id],
                            "time_since_last_fetch_millis": time_since_last_fetch_millis[shard_id]
                        } for shard_id in shard_range
                    ]
                }
            ]
        }

        client = Client(transport_client=TransportClient(response=ccr_stats_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder("remote", client, metrics_store, 1)
        recorder.record()

        shard_metadata = [
            {
                "cluster": "remote",
                "index": "follower",
                "shard": 0,
                "name": "ccr-stats"
            },
            {
                "cluster": "remote",
                "index": "follower",
                "shard": 1,
                "name": "ccr-stats"
            }
        ]

        metrics_store_put_doc.assert_has_calls([
            mock.call(ccr_stats_follower_response["indices"][0]["shards"][0], level=MetaInfoScope.cluster, meta_data=shard_metadata[0]),
            mock.call(ccr_stats_follower_response["indices"][0]["shards"][1], level=MetaInfoScope.cluster, meta_data=shard_metadata[1])
            ],
            any_order=True
        )

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_filtered_ccr_stats(self, metrics_store_put_doc):
        java_signed_maxlong = CcrStatsRecorderTests.java_signed_maxlong

        leader_global_checkpoint = random.randint(0, java_signed_maxlong)
        leader_max_seq_no = random.randint(0, java_signed_maxlong)
        follower_global_checkpoint = random.randint(0, java_signed_maxlong)
        leader_index1 = "leader_cluster:leader1"
        follower_index1 = "follower1"
        leader_index2 = "leader_cluster:leader2"
        follower_index2 = "follower2"
        follower_max_seq_no = random.randint(0, java_signed_maxlong)
        last_requested_seq_no = random.randint(0, java_signed_maxlong)
        number_of_concurrent_reads = random.randint(0, java_signed_maxlong)
        number_of_concurrent_writes = random.randint(0, java_signed_maxlong)
        number_of_queued_writes = random.randint(0, java_signed_maxlong)
        mapping_version = random.randint(0, java_signed_maxlong)
        total_fetch_time_millis = random.randint(0, java_signed_maxlong)
        number_of_successful_fetches = random.randint(0, java_signed_maxlong)
        number_of_failed_fetches = random.randint(0, java_signed_maxlong)
        operations_received = random.randint(0, java_signed_maxlong)
        total_transferred_bytes = random.randint(0, java_signed_maxlong)
        total_index_time_millis = random.randint(0, java_signed_maxlong)
        time_since_last_fetch_millis = random.randint(0, java_signed_maxlong)
        number_of_successful_bulk_operations = random.randint(0, java_signed_maxlong)
        number_of_failed_bulk_operations = random.randint(0, java_signed_maxlong)
        number_of_operations_indexed = random.randint(0, java_signed_maxlong)

        ccr_stats_follower_response = {
            "indices": [
                {
                    "index": "follower1",
                    "shards": [
                        {
                            "shard_id": 0,
                            "leader_index": leader_index1,
                            "follower_index": follower_index1,
                            "follower_global_checkpoint": follower_global_checkpoint,
                            "follower_max_seq_no": follower_max_seq_no,
                            "mapping_version": mapping_version,
                            "last_requested_seq_no": last_requested_seq_no,
                            "leader_global_checkpoint": leader_global_checkpoint,
                            "leader_max_seq_no": leader_max_seq_no,
                            "number_of_concurrent_reads": number_of_concurrent_reads,
                            "number_of_concurrent_writes": number_of_concurrent_writes,
                            "number_of_failed_bulk_operations": number_of_failed_bulk_operations,
                            "number_of_failed_fetches": number_of_failed_fetches,
                            "number_of_operations_indexed": number_of_operations_indexed,
                            "number_of_queued_writes": number_of_queued_writes,
                            "number_of_successful_bulk_operations": number_of_successful_bulk_operations,
                            "number_of_successful_fetches": number_of_successful_fetches,
                            "operations_received": operations_received,
                            "time_since_last_fetch_millis": time_since_last_fetch_millis,
                            "total_fetch_time_millis": total_fetch_time_millis,
                            "total_index_time_millis": total_index_time_millis,
                            "total_transferred_bytes": total_transferred_bytes
                        }
                    ]
                },
                {
                    "index": "follower2",
                    "shards": [
                        {
                            "shard_id": 1,
                            "leader_index": leader_index2,
                            "follower_index": follower_index2,
                            "follower_global_checkpoint": follower_global_checkpoint,
                            "follower_max_seq_no": follower_max_seq_no,
                            "mapping_version": mapping_version,
                            "last_requested_seq_no": last_requested_seq_no,
                            "leader_global_checkpoint": leader_global_checkpoint,
                            "leader_max_seq_no": leader_max_seq_no,
                            "number_of_concurrent_reads": number_of_concurrent_reads,
                            "number_of_concurrent_writes": number_of_concurrent_writes,
                            "number_of_failed_bulk_operations": number_of_failed_bulk_operations,
                            "number_of_failed_fetches": number_of_failed_fetches,
                            "number_of_operations_indexed": number_of_operations_indexed,
                            "number_of_queued_writes": number_of_queued_writes,
                            "number_of_successful_bulk_operations": number_of_successful_bulk_operations,
                            "number_of_successful_fetches": number_of_successful_fetches,
                            "operations_received": operations_received,
                            "time_since_last_fetch_millis": time_since_last_fetch_millis,
                            "total_fetch_time_millis": total_fetch_time_millis,
                            "total_index_time_millis": total_index_time_millis,
                            "total_transferred_bytes": total_transferred_bytes
                        }
                        ]
                }
            ]
        }

        client = Client(transport_client=TransportClient(response=ccr_stats_follower_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        recorder = telemetry.CcrStatsRecorder("remote", client, metrics_store, 1, indices=[follower_index1])
        recorder.record()

        shard_metadata = {
            "cluster": "remote",
            "index": follower_index1,
            "shard": 0,
            "name": "ccr-stats"
        }

        metrics_store_put_doc.assert_has_calls([
            mock.call(ccr_stats_follower_response["indices"][0]["shards"][0], level=MetaInfoScope.cluster, meta_data=shard_metadata)
            ],
            any_order=True
        )


class NodeStatsRecorderTests(TestCase):
    node_stats_response = {
        "cluster_name" : "elasticsearch",
        "nodes" : {
            "Zbl_e8EyRXmiR47gbHgPfg" : {
                "timestamp" : 1524379617017,
                "name" : "rally0",
                "transport_address" : "127.0.0.1:9300",
                "host" : "127.0.0.1",
                "ip" : "127.0.0.1:9300",
                "roles" : [
                    "master",
                    "data",
                    "ingest"
                ],
                "indices" : {
                    "docs" : {
                        "count" : 0,
                        "deleted" : 0
                    },
                    "store" : {
                        "size_in_bytes" : 0
                    },
                    "indexing" : {
                        "is_throttled" : False,
                        "throttle_time_in_millis" : 0
                    },
                    "search" : {
                        "open_contexts" : 0,
                        "query_total" : 0,
                        "query_time_in_millis" : 0
                    },
                    "merges" : {
                        "current" : 0,
                        "current_docs" : 0,
                        "current_size_in_bytes" : 0
                    },
                    "query_cache" : {
                        "memory_size_in_bytes" : 0,
                        "total_count" : 0,
                        "hit_count" : 0,
                        "miss_count" : 0,
                        "cache_size" : 0,
                        "cache_count" : 0,
                        "evictions" : 0
                    },
                    "completion" : {
                        "size_in_bytes" : 0
                    },
                    "segments" : {
                        "count" : 0,
                        "memory_in_bytes" : 0,
                        "max_unsafe_auto_id_timestamp" : -9223372036854775808,
                        "file_sizes" : { }
                    },
                    "translog" : {
                        "operations" : 0,
                        "size_in_bytes" : 0,
                        "uncommitted_operations" : 0,
                        "uncommitted_size_in_bytes" : 0
                    },
                    "request_cache" : {
                        "memory_size_in_bytes" : 0,
                        "evictions" : 0,
                        "hit_count" : 0,
                        "miss_count" : 0
                    },
                    "recovery" : {
                        "current_as_source" : 0,
                        "current_as_target" : 0,
                        "throttle_time_in_millis" : 0
                    }
                },
                "jvm" : {
                    "buffer_pools" : {
                        "mapped" : {
                            "count" : 7,
                            "used_in_bytes" : 3120,
                            "total_capacity_in_bytes" : 9999
                        },
                        "direct" : {
                            "count" : 6,
                            "used_in_bytes" : 73868,
                            "total_capacity_in_bytes" : 73867
                        }
                    },
                    "classes" : {
                        "current_loaded_count" : 9992,
                        "total_loaded_count" : 9992,
                        "total_unloaded_count" : 0
                    },
                    "mem": {
                        "heap_used_in_bytes": 119073552,
                        "heap_used_percent": 19,
                        "heap_committed_in_bytes": 626393088,
                        "heap_max_in_bytes": 626393088,
                        "non_heap_used_in_bytes": 110250424,
                        "non_heap_committed_in_bytes": 118108160,
                        "pools": {
                            "young": {
                                "used_in_bytes": 66378576,
                                "max_in_bytes": 139591680,
                                "peak_used_in_bytes": 139591680,
                                "peak_max_in_bytes": 139591680
                            },
                            "survivor": {
                                "used_in_bytes": 358496,
                                "max_in_bytes": 17432576,
                                "peak_used_in_bytes": 17432576,
                                "peak_max_in_bytes": 17432576
                            },
                            "old": {
                                "used_in_bytes": 52336480,
                                "max_in_bytes": 469368832,
                                "peak_used_in_bytes": 52336480,
                                "peak_max_in_bytes": 469368832
                            }
                        }
                    }
                },
                "process": {
                    "timestamp": 1526045135857,
                    "open_file_descriptors": 312,
                    "max_file_descriptors": 1048576,
                    "cpu": {
                        "percent": 10,
                        "total_in_millis": 56520
                    },
                    "mem": {
                        "total_virtual_in_bytes": 2472173568
                    }
                },
                "thread_pool" : {
                    "generic" : {
                        "threads" : 4,
                        "queue" : 0,
                        "active" : 0,
                        "rejected" : 0,
                        "largest" : 4,
                        "completed" : 8
                    }
                },
                "breakers" : {
                    "parent" : {
                        "limit_size_in_bytes" : 726571417,
                        "limit_size" : "692.9mb",
                        "estimated_size_in_bytes" : 0,
                        "estimated_size" : "0b",
                        "overhead" : 1.0,
                        "tripped" : 0
                    }
                }
            }
        }
    }

    indices_stats_response_flattened = collections.OrderedDict({
        "indices_docs_count": 0,
        "indices_docs_deleted": 0,
        "indices_store_size_in_bytes": 0,
        "indices_indexing_throttle_time_in_millis": 0,
        "indices_search_open_contexts": 0,
        "indices_search_query_total": 0,
        "indices_search_query_time_in_millis": 0,
        "indices_merges_current": 0,
        "indices_merges_current_docs": 0,
        "indices_merges_current_size_in_bytes": 0,
        "indices_query_cache_memory_size_in_bytes": 0,
        "indices_query_cache_total_count": 0,
        "indices_query_cache_hit_count": 0,
        "indices_query_cache_miss_count": 0,
        "indices_query_cache_cache_size": 0,
        "indices_query_cache_cache_count": 0,
        "indices_query_cache_evictions": 0,
        "indices_completion_size_in_bytes": 0,
        "indices_segments_count": 0,
        "indices_segments_memory_in_bytes": 0,
        "indices_segments_max_unsafe_auto_id_timestamp": -9223372036854775808,
        "indices_translog_operations": 0,
        "indices_translog_size_in_bytes": 0,
        "indices_translog_uncommitted_operations": 0,
        "indices_translog_uncommitted_size_in_bytes": 0,
        "indices_request_cache_memory_size_in_bytes": 0,
        "indices_request_cache_evictions": 0,
        "indices_request_cache_hit_count": 0,
        "indices_request_cache_miss_count": 0,
        "indices_recovery_current_as_source": 0,
        "indices_recovery_current_as_target": 0,
        "indices_recovery_throttle_time_in_millis": 0
    })

    default_stats_response_flattened = collections.OrderedDict({
        "jvm_buffer_pools_mapped_count": 7,
        "jvm_buffer_pools_mapped_used_in_bytes": 3120,
        "jvm_buffer_pools_mapped_total_capacity_in_bytes": 9999,
        "jvm_buffer_pools_direct_count": 6,
        "jvm_buffer_pools_direct_used_in_bytes": 73868,
        "jvm_buffer_pools_direct_total_capacity_in_bytes": 73867,
        "jvm_mem_heap_used_in_bytes": 119073552,
        "jvm_mem_heap_used_percent": 19,
        "jvm_mem_heap_committed_in_bytes": 626393088,
        "jvm_mem_heap_max_in_bytes": 626393088,
        "jvm_mem_non_heap_used_in_bytes": 110250424,
        "jvm_mem_non_heap_committed_in_bytes": 118108160,
        "jvm_mem_pools_young_used_in_bytes": 66378576,
        "jvm_mem_pools_young_max_in_bytes": 139591680,
        "jvm_mem_pools_young_peak_used_in_bytes": 139591680,
        "jvm_mem_pools_young_peak_max_in_bytes": 139591680,
        "jvm_mem_pools_survivor_used_in_bytes": 358496,
        "jvm_mem_pools_survivor_max_in_bytes": 17432576,
        "jvm_mem_pools_survivor_peak_used_in_bytes": 17432576,
        "jvm_mem_pools_survivor_peak_max_in_bytes": 17432576,
        "jvm_mem_pools_old_used_in_bytes": 52336480,
        "jvm_mem_pools_old_max_in_bytes": 469368832,
        "jvm_mem_pools_old_peak_used_in_bytes": 52336480,
        "jvm_mem_pools_old_peak_max_in_bytes": 469368832,
        "process_cpu_percent": 10,
        "process_cpu_total_in_millis": 56520,
        "breakers_parent_limit_size_in_bytes": 726571417,
        "breakers_parent_estimated_size_in_bytes": 0,
        "breakers_parent_overhead": 1.0,
        "breakers_parent_tripped": 0,
        "thread_pool_generic_threads": 4,
        "thread_pool_generic_queue": 0,
        "thread_pool_generic_active": 0,
        "thread_pool_generic_rejected": 0,
        "thread_pool_generic_largest": 4,
        "thread_pool_generic_completed": 8
    })

    def test_negative_sample_interval_forbidden(self):
        client = Client()
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {
            "node-stats-sample-interval": -1 * random.random()
        }
        with self.assertRaisesRegex(exceptions.SystemSetupError,
                                    "The telemetry parameter 'node-stats-sample-interval' must be greater than zero but was .*\."):
            telemetry.NodeStatsRecorder(telemetry_params, cluster_name="default", client=client, metrics_store=metrics_store)

    def test_flatten_indices_fields(self):
        client = Client(nodes=SubClient(stats=NodeStatsRecorderTests.node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        telemetry_params = {}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        flattened_fields = recorder.flatten_stats_fields(
            prefix="indices",
            stats=NodeStatsRecorderTests.node_stats_response["nodes"]["Zbl_e8EyRXmiR47gbHgPfg"]["indices"]
        )
        self.assertDictEqual(NodeStatsRecorderTests.indices_stats_response_flattened, flattened_fields)

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_default_nodes_stats(self, metrics_store_put_doc):
        client = Client(nodes=SubClient(stats=NodeStatsRecorderTests.node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        metrics_store_meta_data = {"cluster": "remote"}
        telemetry_params = {}
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        recorder.record()

        expected_doc = collections.OrderedDict()
        expected_doc["name"] = "node-stats"
        expected_doc.update(NodeStatsRecorderTests.default_stats_response_flattened)

        metrics_store_put_doc.assert_called_once_with(expected_doc,
                                                      level=MetaInfoScope.node,
                                                      node_name="rally0",
                                                      meta_data=metrics_store_meta_data)

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    def test_stores_all_nodes_stats(self, metrics_store_put_doc):
        node_stats_response = {
            "cluster_name": "elasticsearch",
            "nodes": {
                "Zbl_e8EyRXmiR47gbHgPfg": {
                    "timestamp": 1524379617017,
                    "name": "rally0",
                    "transport_address": "127.0.0.1:9300",
                    "host": "127.0.0.1",
                    "ip": "127.0.0.1:9300",
                    "roles": [
                        "master",
                        "data",
                        "ingest"
                    ],
                    "indices": {
                        "docs": {
                            "count": 76892364,
                            "deleted": 324530
                        },
                        "store": {
                            "size_in_bytes": 983409834
                        },
                        "indexing": {
                            "is_throttled": False,
                            "throttle_time_in_millis": 0
                        },
                        "search": {
                            "open_contexts": 0,
                            "query_total": 0,
                            "query_time_in_millis": 0
                        },
                        "merges": {
                            "current": 0,
                            "current_docs": 0,
                            "current_size_in_bytes": 0
                        },
                        "query_cache": {
                            "memory_size_in_bytes": 0,
                            "total_count": 0,
                            "hit_count": 0,
                            "miss_count": 0,
                            "cache_size": 0,
                            "cache_count": 0,
                            "evictions": 0
                        },
                        "fielddata": {
                            "memory_size_in_bytes": 6936,
                            "evictions": 17
                        },
                        "completion": {
                            "size_in_bytes": 0
                        },
                        "segments": {
                            "count": 0,
                            "memory_in_bytes": 0,
                            "max_unsafe_auto_id_timestamp": -9223372036854775808,
                            "file_sizes": {}
                        },
                        "translog": {
                            "operations": 0,
                            "size_in_bytes": 0,
                            "uncommitted_operations": 0,
                            "uncommitted_size_in_bytes": 0
                        },
                        "request_cache": {
                            "memory_size_in_bytes": 0,
                            "evictions": 0,
                            "hit_count": 0,
                            "miss_count": 0
                        },
                        "recovery": {
                            "current_as_source": 0,
                            "current_as_target": 0,
                            "throttle_time_in_millis": 0
                        }
                    },
                    "jvm": {
                        "buffer_pools": {
                            "mapped": {
                                "count": 7,
                                "used_in_bytes": 3120,
                                "total_capacity_in_bytes": 9999
                            },
                            "direct": {
                                "count": 6,
                                "used_in_bytes": 73868,
                                "total_capacity_in_bytes": 73867
                            }
                        },
                        "classes": {
                            "current_loaded_count": 9992,
                            "total_loaded_count": 9992,
                            "total_unloaded_count": 0
                        },
                        "mem": {
                            "heap_used_in_bytes": 119073552,
                            "heap_used_percent": 19,
                            "heap_committed_in_bytes": 626393088,
                            "heap_max_in_bytes": 626393088,
                            "non_heap_used_in_bytes": 110250424,
                            "non_heap_committed_in_bytes": 118108160,
                            "pools": {
                                "young": {
                                    "used_in_bytes": 66378576,
                                    "max_in_bytes": 139591680,
                                    "peak_used_in_bytes": 139591680,
                                    "peak_max_in_bytes": 139591680
                                },
                                "survivor": {
                                    "used_in_bytes": 358496,
                                    "max_in_bytes": 17432576,
                                    "peak_used_in_bytes": 17432576,
                                    "peak_max_in_bytes": 17432576
                                },
                                "old": {
                                    "used_in_bytes": 52336480,
                                    "max_in_bytes": 469368832,
                                    "peak_used_in_bytes": 52336480,
                                    "peak_max_in_bytes": 469368832
                                }
                            }
                        }
                    },
                    "process": {
                        "timestamp": 1526045135857,
                        "open_file_descriptors": 312,
                        "max_file_descriptors": 1048576,
                        "cpu": {
                            "percent": 10,
                            "total_in_millis": 56520
                        },
                        "mem": {
                            "total_virtual_in_bytes": 2472173568
                        }
                    },
                    "thread_pool": {
                        "generic": {
                            "threads": 4,
                            "queue": 0,
                            "active": 0,
                            "rejected": 0,
                            "largest": 4,
                            "completed": 8
                        }
                    },
                    "transport": {
                        "server_open": 12,
                        "rx_count": 77,
                        "rx_size_in_bytes": 98723498,
                        "tx_count": 88,
                        "tx_size_in_bytes": 23879803
                    },
                    "breakers": {
                        "parent": {
                            "limit_size_in_bytes": 726571417,
                            "limit_size": "692.9mb",
                            "estimated_size_in_bytes": 0,
                            "estimated_size": "0b",
                            "overhead": 1.0,
                            "tripped": 0
                        }
                    }
                }
            }
        }

        client = Client(nodes=SubClient(stats=node_stats_response))
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        metrics_store_meta_data = {"cluster": "remote"}
        telemetry_params = {
            "node-stats-include-indices": True
        }
        recorder = telemetry.NodeStatsRecorder(telemetry_params, cluster_name="remote", client=client, metrics_store=metrics_store)
        recorder.record()

        metrics_store_put_doc.assert_called_once_with(
            {"name": "node-stats",
             "indices_docs_count": 76892364,
             "indices_docs_deleted": 324530,
             "indices_fielddata_evictions": 17,
             "indices_fielddata_memory_size_in_bytes": 6936,
             "indices_indexing_throttle_time_in_millis": 0,
             "indices_merges_current": 0,
             "indices_merges_current_docs": 0,
             "indices_merges_current_size_in_bytes": 0,
             "indices_query_cache_cache_count": 0,
             "indices_query_cache_cache_size": 0,
             "indices_query_cache_evictions": 0,
             "indices_query_cache_hit_count": 0,
             "indices_query_cache_memory_size_in_bytes": 0,
             "indices_query_cache_miss_count": 0,
             "indices_query_cache_total_count": 0,
             "indices_request_cache_evictions": 0,
             "indices_request_cache_hit_count": 0,
             "indices_request_cache_memory_size_in_bytes": 0,
             "indices_request_cache_miss_count": 0,
             "indices_search_open_contexts": 0,
             "indices_search_query_time_in_millis": 0,
             "indices_search_query_total": 0,
             "indices_segments_count": 0,
             "indices_segments_max_unsafe_auto_id_timestamp": -9223372036854775808,
             "indices_segments_memory_in_bytes": 0,
             "indices_store_size_in_bytes": 983409834,
             "indices_translog_operations": 0,
             "indices_translog_size_in_bytes": 0,
             "indices_translog_uncommitted_operations": 0,
             "indices_translog_uncommitted_size_in_bytes": 0,
             "thread_pool_generic_active": 0,
             "thread_pool_generic_completed": 8,
             "thread_pool_generic_largest": 4,
             "thread_pool_generic_queue": 0,
             "thread_pool_generic_rejected": 0,
             "thread_pool_generic_threads": 4,
             "breakers_parent_estimated_size_in_bytes": 0,
             "breakers_parent_limit_size_in_bytes": 726571417,
             "breakers_parent_overhead": 1.0,
             "breakers_parent_tripped": 0,
             "jvm_buffer_pools_direct_count": 6,
             "jvm_buffer_pools_direct_total_capacity_in_bytes": 73867,
             "jvm_buffer_pools_direct_used_in_bytes": 73868,
             "jvm_buffer_pools_mapped_count": 7,
             "jvm_buffer_pools_mapped_total_capacity_in_bytes": 9999,
             "jvm_buffer_pools_mapped_used_in_bytes": 3120,
             "jvm_mem_heap_committed_in_bytes": 626393088,
             "jvm_mem_heap_max_in_bytes": 626393088,
             "jvm_mem_heap_used_in_bytes": 119073552,
             "jvm_mem_heap_used_percent": 19,
             "jvm_mem_non_heap_committed_in_bytes": 118108160,
             "jvm_mem_non_heap_used_in_bytes": 110250424,
             "jvm_mem_pools_old_max_in_bytes": 469368832,
             "jvm_mem_pools_old_peak_max_in_bytes": 469368832,
             "jvm_mem_pools_old_peak_used_in_bytes": 52336480,
             "jvm_mem_pools_old_used_in_bytes": 52336480,
             "jvm_mem_pools_survivor_max_in_bytes": 17432576,
             "jvm_mem_pools_survivor_peak_max_in_bytes": 17432576,
             "jvm_mem_pools_survivor_peak_used_in_bytes": 17432576,
             "jvm_mem_pools_survivor_used_in_bytes": 358496,
             "jvm_mem_pools_young_max_in_bytes": 139591680,
             "jvm_mem_pools_young_peak_max_in_bytes": 139591680,
             "jvm_mem_pools_young_peak_used_in_bytes": 139591680,
             "jvm_mem_pools_young_used_in_bytes": 66378576,
             "transport_rx_count": 77,
             "transport_rx_size_in_bytes": 98723498,
             "transport_server_open": 12,
             "transport_tx_count": 88,
             "transport_tx_size_in_bytes": 23879803,
             "process_cpu_percent": 10,
             "process_cpu_total_in_millis": 56520},
            level=MetaInfoScope.node,
            node_name="rally0",
            meta_data=metrics_store_meta_data)


class ClusterEnvironmentInfoTests(TestCase):
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

        cfg = create_config()
        client = Client(nodes=SubClient(info=nodes_info), info=cluster_info)
        metrics_store = metrics.EsMetricsStore(cfg)
        env_device = telemetry.ClusterEnvironmentInfo(client, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[env_device])
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


class NodeEnvironmentInfoTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.add_meta_info")
    @mock.patch("esrally.utils.sysstats.os_name")
    @mock.patch("esrally.utils.sysstats.os_version")
    @mock.patch("esrally.utils.sysstats.logical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.physical_cpu_cores")
    @mock.patch("esrally.utils.sysstats.cpu_model")
    def test_stores_node_level_metrics_on_attach(self, cpu_model, physical_cpu_cores, logical_cpu_cores,
                                                 os_version, os_name, metrics_store_add_meta_info):
        cpu_model.return_value = "Intel(R) Core(TM) i7-4870HQ CPU @ 2.50GHz"
        physical_cpu_cores.return_value = 4
        logical_cpu_cores.return_value = 8
        os_version.return_value = "4.2.0-18-generic"
        os_name.return_value = "Linux"

        metrics_store = metrics.EsMetricsStore(create_config())
        node = cluster.Node(None, "io", "rally0", None)
        env_device = telemetry.NodeEnvironmentInfo(metrics_store)
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
    def test_stores_all_node_metrics_on_attach(self, metrics_store_add_meta_info):
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
            mock.call(metrics.MetaInfoScope.node, "rally0", "node_name", "rally0"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "host_name", "127.0.0.1"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_name", "Mac OS X"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "os_version", "10.11.4"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "cpu_logical_cores", 8),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_vendor", "Oracle Corporation"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "jvm_version", "1.8.0_74"),
            mock.call(metrics.MetaInfoScope.node, "rally0", "plugins", ["ingest-geoip"]),
            # these are automatically pushed up to cluster level (additionally) if all nodes match
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


class GcTimesSummaryTests(TestCase):
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
        device = telemetry.GcTimesSummary(client, metrics_store)
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
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    def test_stores_available_index_stats(self, metrics_store_cluster_count, metrics_store_cluster_value, metrics_store_put_doc):
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

        response = {
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
                        "total_time_in_millis": 509341,
                        "total_throttled_time_in_millis": 98925
                    },
                    "indexing": {
                        "index_time_in_millis": 1065688
                    },
                    "refresh": {
                        "total_time_in_millis": 158465
                    },
                    "flush": {
                        "total_time_in_millis": 19082
                    }
                },
                "total": {
                    "store": {
                        "size_in_bytes": 2113867510
                    },
                    "translog": {
                        "operations": 6840000,
                        "size_in_bytes": 2647984713,
                        "uncommitted_operations": 0,
                        "uncommitted_size_in_bytes": 430
                    }
                }
            },
            "indices": {
                "idx-001": {
                    "shards": {
                        "0": [
                            {
                                "routing": {
                                    "primary": False
                                },
                                "indexing": {
                                    "index_total": 2280171,
                                    "index_time_in_millis": 533662,
                                    "throttle_time_in_millis": 0
                                },
                                "merges": {
                                    "total_time_in_millis": 280689,
                                    "total_stopped_time_in_millis": 0,
                                    "total_throttled_time_in_millis": 58846,
                                    "total_auto_throttle_in_bytes": 8085428
                                },
                                "refresh": {
                                    "total_time_in_millis": 81004
                                },
                                "flush": {
                                    "total_time_in_millis": 9879
                                }
                            }
                        ],
                        "1": [
                            {
                                "routing": {
                                    "primary": True,
                                },
                                "indexing": {
                                    "index_time_in_millis": 532026,
                                },
                                "merges": {
                                    "total_time_in_millis": 228652,
                                    "total_throttled_time_in_millis": 40079,
                                },
                                "refresh": {
                                    "total_time_in_millis": 77461,
                                },
                                "flush": {
                                    "total_time_in_millis": 9203
                                }
                            }
                        ]
                    }
                },
                "idx-002": {
                    "shards": {
                        "0": [
                            {
                                "routing": {
                                    "primary": True,
                                },
                                "indexing": {
                                    "index_time_in_millis": 533662,
                                },
                                "merges": {
                                    "total_time_in_millis": 280689,
                                    "total_throttled_time_in_millis": 58846,
                                },
                                "refresh": {
                                    "total_time_in_millis": 81004,
                                },
                                "flush": {
                                    "total_time_in_millis": 9879
                                }
                            }
                        ],
                        "1": [
                            {
                                "routing": {
                                    "primary": False,
                                },
                                "indexing": {
                                    "index_time_in_millis": 532026,
                                    "throttle_time_in_millis": 296
                                },
                                "merges": {
                                    "total_time_in_millis": 228652,
                                    "total_throttled_time_in_millis": 40079,
                                },
                                "refresh": {
                                    "total_time_in_millis": 77461,
                                },
                                "flush": {
                                    "total_time_in_millis": 9203
                                }
                            }
                        ]
                    }
                }
            }
        }

        client.indices = SubClient(response)

        t.on_benchmark_stop()

        # we cannot rely on stable iteration order so we need to extract the values at runtime from the dict
        primary_shards = []
        for idx, shards in response["indices"].items():
            for shard_number, shard in shards["shards"].items():
                for shard_metrics in shard:
                    if shard_metrics["routing"]["primary"]:
                        primary_shards.append(shard_metrics)

        metrics_store_put_doc.assert_has_calls([
            mock.call(doc={
                "name": "merges_total_time",
                "value": 509341,
                "unit": "ms",
                # [228652, 280689]
                "per-shard": [s["merges"]["total_time_in_millis"] for s in primary_shards]
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "merges_total_throttled_time",
                "value": 98925,
                "unit": "ms",
                # [40079, 58846]
                "per-shard": [s["merges"]["total_throttled_time_in_millis"] for s in primary_shards]
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "indexing_total_time",
                "value": 1065688,
                "unit": "ms",
                # [532026, 533662]
                "per-shard": [s["indexing"]["index_time_in_millis"] for s in primary_shards]
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "refresh_total_time",
                "value": 158465,
                "unit": "ms",
                # [77461, 81004]
                "per-shard": [s["refresh"]["total_time_in_millis"] for s in primary_shards]
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "flush_total_time",
                "value": 19082,
                "unit": "ms",
                # [9203, 9879]
                "per-shard": [s["flush"]["total_time_in_millis"] for s in primary_shards]
            }, level=metrics.MetaInfoScope.cluster),
        ])

        metrics_store_cluster_count.assert_has_calls([
            mock.call("segments_count", 5)
        ])
        metrics_store_cluster_value.assert_has_calls([
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
            # we don't have norms, so nothing should have been called
            mock.call("store_size_in_bytes", 2113867510, "byte"),
            mock.call("translog_size_in_bytes", 2647984713, "byte"),
        ], any_order=True)

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("esrally.metrics.EsMetricsStore.put_value_cluster_level")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_cluster_level")
    def test_index_stats_are_per_lap(self, metrics_store_cluster_count, metrics_store_cluster_value, metrics_store_put_doc):
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

        metrics_store_put_doc.assert_has_calls([
            # 1st lap
            mock.call(doc={
                "name": "merges_total_time",
                "value": 300,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "merges_total_throttled_time",
                "value": 120,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "indexing_total_time",
                "value": 2000,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "refresh_total_time",
                "value": 200,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "flush_total_time",
                "value": 100,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            # 2nd lap
            mock.call(doc={
                "name": "merges_total_time",
                "value": 900,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "merges_total_throttled_time",
                "value": 120,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "indexing_total_time",
                "value": 8000,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "refresh_total_time",
                "value": 500,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "flush_total_time",
                "value": 300,
                "unit": "ms",
                "per-shard": []
            }, level=metrics.MetaInfoScope.cluster),
        ])

        metrics_store_cluster_value.assert_has_calls([
            # 1st lap
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
            # we don't have norms or points, so nothing should have been called

            # 2nd lap
            mock.call("segments_memory_in_bytes", 2048, "byte"),
            mock.call("segments_doc_values_memory_in_bytes", 128, "byte"),
            mock.call("segments_stored_fields_memory_in_bytes", 1024, "byte"),
            mock.call("segments_terms_memory_in_bytes", 256, "byte"),
        ], any_order=True)


class MlBucketProcessingTimeTests(TestCase):
    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_error_on_retrieval_does_not_store_metrics(self, es, metrics_store_put_doc):
        es.search.side_effect = elasticsearch.TransportError("unit test error")
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        # cluster is not used by this device
        t.detach_from_cluster(cluster=None)

        self.assertEqual(0, metrics_store_put_doc.call_count)

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_empty_result_does_not_store_metrics(self, es, metrics_store_put_doc):
        es.search.return_value = {
            "aggregations": {
                "jobs": {
                    "buckets": []
                }
            }
        }
        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        # cluster is not used by this device
        t.detach_from_cluster(cluster=None)

        self.assertEqual(0, metrics_store_put_doc.call_count)

    @mock.patch("esrally.metrics.EsMetricsStore.put_doc")
    @mock.patch("elasticsearch.Elasticsearch")
    def test_result_is_stored(self, es, metrics_store_put_doc):
        es.search.return_value = {
            "aggregations": {
                "jobs": {
                    "buckets": [
                        {
                            "key": "benchmark_ml_job_1",
                            "doc_count": 4775,
                            "max_pt": {
                                "value": 36.0
                            },
                            "mean_pt": {
                                "value": 12.3
                            },
                            "median_pt": {
                                "values": {
                                    "50.0": 17.2
                                }
                            },
                            "min_pt": {
                                "value": 2.2
                            }
                        },
                        {
                            "key": "benchmark_ml_job_2",
                            "doc_count": 3333,
                            "max_pt": {
                                "value": 226.3
                            },
                            "mean_pt": {
                                "value": 78.3
                            },
                            "median_pt": {
                                "values": {
                                    "50.0": 37.4
                                }
                            },
                            "min_pt": {
                                "value": 32.2
                            }
                        }
                    ]
                }
            }
        }

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.MlBucketProcessingTime(es, metrics_store)
        t = telemetry.Telemetry(cfg, devices=[device])
        # cluster is not used by this device
        t.detach_from_cluster(cluster=None)

        metrics_store_put_doc.assert_has_calls([
            mock.call(doc={
                "name": "ml_processing_time",
                "job": "benchmark_ml_job_1",
                "min": 2.2,
                "mean": 12.3,
                "median": 17.2,
                "max": 36.0,
                "unit": "ms"
            }, level=metrics.MetaInfoScope.cluster),
            mock.call(doc={
                "name": "ml_processing_time",
                "job": "benchmark_ml_job_2",
                "min": 32.2,
                "mean": 78.3,
                "median": 37.4,
                "max": 226.3,
                "unit": "ms"
            }, level=metrics.MetaInfoScope.cluster)
        ])


class IndexSizeTests(TestCase):
    @mock.patch("esrally.utils.io.get_size")
    @mock.patch("esrally.metrics.EsMetricsStore.put_count_node_level")
    def test_stores_index_size_for_data_paths(self, metrics_store_node_count, get_size):
        get_size.side_effect = [2048, 16384]

        cfg = create_config()
        metrics_store = metrics.EsMetricsStore(cfg)
        device = telemetry.IndexSize(["/var/elasticsearch/data/1", "/var/elasticsearch/data/2"], metrics_store)
        t = telemetry.Telemetry(enabled_devices=[], devices=[device])
        node = cluster.Node(process=None, host_name="localhost", node_name="rally-node-0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)

        metrics_store_node_count.assert_has_calls([
            mock.call("rally-node-0", "final_index_size_bytes", 18432, "byte")
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
        node = cluster.Node(process=None, host_name="localhost", node_name="rally-node-0", telemetry=t)
        t.attach_to_node(node)
        t.on_benchmark_start()
        t.on_benchmark_stop()
        t.detach_from_node(node, running=True)
        t.detach_from_node(node, running=False)

        self.assertEqual(0, run_subprocess.call_count)
        self.assertEqual(0, metrics_store_cluster_count.call_count)
        self.assertEqual(0, get_size.call_count)
