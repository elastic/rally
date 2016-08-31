import collections
import datetime
from unittest import TestCase

from esrally import reporter, metrics, config, track


class ReporterTests(TestCase):
    def test_calculate_simple_index_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")

        store = metrics.InMemoryMetricsStore(config=cfg, clear=True)
        store.open(datetime.datetime.now(), "test", "unittest", "unittest_car")

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

        index = track.Task(operation=track.Operation(name="index", operation_type=track.OperationType.Index, granularity_unit="docs/s"))
        challenge = track.Challenge(name="unittest", description="", index_settings=None, schedule=[index])

        stats = reporter.Stats(store, challenge)

        self.assertEqual((500, 1000, 2000, "docs/s"), stats.op_metrics["index"]["throughput"])
        self.assertEqual(collections.OrderedDict([(50.0, 220), (100, 225)]), stats.op_metrics["index"]["latency"])
        self.assertEqual(collections.OrderedDict([(50.0, 200), (100, 215)]), stats.op_metrics["index"]["service_time"])
