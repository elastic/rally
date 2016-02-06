from unittest import TestCase
import unittest.mock as mock

from rally import config, metrics, telemetry
from rally.track import track


class MockTelemetryDevice(telemetry.TelemetryDevice):
    def __init__(self, cfg, metrics_store, mock_env):
        super().__init__(cfg, metrics_store)
        self.mock_env = mock_env

    def mandatory(self):
        return True

    def human_name(self):
        return "mock"

    def command(self):
        return "mock"

    def instrument_env(self, setup):
        return self.mock_env


class TelemetryTests(TestCase):
    def test_instrument_candidate_env(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "telemetry", "devices", "jfr")
        cfg.add(config.Scope.application, "system", "track.setup.root.dir", "track-setup-root")
        cfg.add(config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

        # we don't need one for this test
        metrics_store = None

        devices = [
            MockTelemetryDevice(cfg, metrics_store, {"ES_JAVA_OPTS": "-Xms256M"}),
            MockTelemetryDevice(cfg, metrics_store, {"ES_JAVA_OPTS": "-Xmx512M"}),
            MockTelemetryDevice(cfg, metrics_store, {"ES_NET_HOST": "127.0.0.1"})
        ]

        t = telemetry.Telemetry(cfg, metrics_store, devices)

        track_setup = track.TrackSetup(name="test-track", description="Test Track")
        opts = t.instrument_candidate_env(track_setup)

        self.assertTrue(opts)
        self.assertEqual(len(opts), 2)
        self.assertEqual("-Xms256M -Xmx512M", opts["ES_JAVA_OPTS"])
        self.assertEqual("127.0.0.1", opts["ES_NET_HOST"])


class MergePartsDeviceTests(TestCase):
    @mock.patch("rally.metrics.EsMetricsStore.put_count")
    @mock.patch("rally.metrics.EsMetricsStore.put_value")
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
        merge_parts_device.on_benchmark_stop()

        metrics_store_put_value.assert_not_called()
        metrics_store_put_count.assert_not_called()

    @mock.patch("rally.metrics.EsMetricsStore.put_count")
    @mock.patch("rally.metrics.EsMetricsStore.put_value")
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
        merge_parts_device.on_benchmark_stop()

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
