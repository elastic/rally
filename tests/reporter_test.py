import collections
import datetime
from unittest import TestCase

from esrally import reporter, metrics, config, track


class StatsTests(TestCase):
    def test_calculate_simple_index_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")

        store = metrics.InMemoryMetricsStore(cfg=cfg)
        store.open(datetime.datetime.now(), "test", "unittest", "unittest_car")
        store.lap = 1

        store.put_value_cluster_level("throughput", 500, unit="docs/s", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("throughput", 2000, unit="docs/s", operation="index", operation_type=track.OperationType.Index)

        store.put_value_cluster_level("latency", 2800, unit="ms", operation="index", operation_type=track.OperationType.Index,
                                      sample_type=metrics.SampleType.Warmup)
        store.put_value_cluster_level("latency", 200, unit="ms", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("latency", 220, unit="ms", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("latency", 225, unit="ms", operation="index", operation_type=track.OperationType.Index)

        store.put_value_cluster_level("service_time", 250, unit="ms", operation="index", operation_type=track.OperationType.Index,
                                      sample_type=metrics.SampleType.Warmup)
        store.put_value_cluster_level("service_time", 190, unit="ms", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("service_time", 200, unit="ms", operation="index", operation_type=track.OperationType.Index)
        store.put_value_cluster_level("service_time", 215, unit="ms", operation="index", operation_type=track.OperationType.Index)

        index = track.Task(operation=track.Operation(name="index", operation_type=track.OperationType.Index, params=None))
        challenge = track.Challenge(name="unittest", description="", index_settings=None, schedule=[index])

        stats = reporter.Stats(store, challenge)

        del store

        self.assertEqual((500, 1000, 2000, "docs/s"), stats.op_metrics["index"]["throughput"])
        self.assertEqual(collections.OrderedDict([(50.0, 220), (100, 225)]), stats.op_metrics["index"]["latency"])
        self.assertEqual(collections.OrderedDict([(50.0, 200), (100, 215)]), stats.op_metrics["index"]["service_time"])


class ComparisonReporterTests(TestCase):
    def test_formats_table(self):
        cfg = config.Config()
        r = reporter.ComparisonReporter(cfg)

        formatted = r.format_as_table([])
        # 1 header line, 1 separation line + 0 data lines
        self.assertEqual(1 + 1 + 0, len(formatted.splitlines()))

        # ["Metric", "Operation", "Baseline", "Contender", "Diff", "Unit"]
        metrics_table = [
            ["Min Throughput", "index", "17300", "18000", "700", "ops/s"],
            ["Median Throughput", "index", "17500", "18500", "1000", "ops/s"],
            ["Max Throughput", "index", "17700", "19000", "1300", "ops/s"]
        ]

        formatted = r.format_as_table(metrics_table)
        # 1 header line, 1 separation line + 3 data lines
        self.assertEqual(1 + 1 + 3, len(formatted.splitlines()))
