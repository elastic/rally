from unittest import TestCase
import unittest.mock as mock

import rally.telemetry
import rally.metrics
import rally.config
import rally.track.track


class TelemetryTests(TestCase):
    def test_instrument_candidate_env(self):
        config = rally.config.Config()
        config.add(rally.config.Scope.application, "telemetry", "devices", "jfr")
        config.add(rally.config.Scope.application, "system", "track.setup.root.dir", "track-setup-root")
        config.add(rally.config.Scope.application, "benchmarks", "metrics.log.dir", "telemetry")

        t = rally.telemetry.Telemetry(config, None)

        track_setup = rally.track.track.TrackSetup(name="test-track", description="Test Track")
        opts = t.instrument_candidate_env(track_setup)

        self.assertTrue(opts)


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
        config = self.create_config()
        metrics_store = rally.metrics.EsMetricsStore(config)
        merge_parts_device = rally.telemetry.MergeParts(config, metrics_store)
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
        metrics_store = rally.metrics.EsMetricsStore(config)
        merge_parts_device = rally.telemetry.MergeParts(config, metrics_store)
        merge_parts_device.on_benchmark_stop()

        metrics_store_put_value.assert_called_with("merge_parts_total_time_doc_values", 350, "ms")
        metrics_store_put_count.assert_called_with("merge_parts_total_docs_doc_values", 1850)

    def create_config(self):
        config = rally.config.Config()
        config.add(rally.config.Scope.application, "launcher", "candidate.log.dir", "/unittests/var/log/elasticsearch")
        config.add(rally.config.Scope.application, "system", "env.name", "unittest")
        config.add(rally.config.Scope.application, "reporting", "datastore.host", "localhost")
        config.add(rally.config.Scope.application, "reporting", "datastore.port", "0")
        config.add(rally.config.Scope.application, "reporting", "datastore.secure", False)
        config.add(rally.config.Scope.application, "reporting", "datastore.user", "")
        config.add(rally.config.Scope.application, "reporting", "datastore.password", "")
        return config
