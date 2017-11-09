import collections
import datetime
from unittest import TestCase

from esrally import reporter, metrics, config, track


class StatsCalculatorTests(TestCase):
    def test_calculate_simple_index_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "mechanic", "car.names", ["unittest_car"])
        cfg.add(config.Scope.application, "race", "laps", 1)
        cfg.add(config.Scope.application, "race", "user.tag", "")
        cfg.add(config.Scope.application, "race", "pipeline", "from-sources-skip-build")

        index = track.Task(operation=track.Operation(name="index", operation_type=track.OperationType.Index, params=None))
        challenge = track.Challenge(name="unittest", schedule=[index], default=True)
        t = track.Track("unittest", "unittest-track", challenges=[challenge])

        store = metrics.metrics_store(cfg, read_only=False, track=t, challenge=challenge)
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
                                      sample_type=metrics.SampleType.Warmup, meta_data={"success": False})
        store.put_value_cluster_level("service_time", 190, unit="ms", operation="index", operation_type=track.OperationType.Index,
                                      meta_data={"success": True})
        store.put_value_cluster_level("service_time", 200, unit="ms", operation="index", operation_type=track.OperationType.Index,
                                      meta_data={"success": False})
        store.put_value_cluster_level("service_time", 215, unit="ms", operation="index", operation_type=track.OperationType.Index,
                                      meta_data={"success": True})
        store.put_count_node_level("rally-node-0", "final_index_size_bytes", 2048, unit="bytes")
        store.put_count_node_level("rally-node-1", "final_index_size_bytes", 4096, unit="bytes")

        stats = reporter.calculate_results(store, metrics.create_race(cfg, t, challenge))

        del store

        opm = stats.metrics("index")
        self.assertEqual(collections.OrderedDict([("min", 500), ("median", 1000), ("max", 2000), ("unit", "docs/s")]), opm["throughput"])
        self.assertEqual(collections.OrderedDict([("50", 220), ("100", 225)]), opm["latency"])
        self.assertEqual(collections.OrderedDict([("50", 200), ("100", 215)]), opm["service_time"])
        self.assertAlmostEqual(0.3333333333333333, opm["error_rate"])

        self.assertEqual(6144, stats.index_size)


def select(l, name, operation=None):
    for item in l:
        if item["name"] == name and item.get("operation") == operation:
            return item
    return None


class StatsTests(TestCase):
    def test_as_flat_list(self):
        d = {
            "op_metrics": [
                {
                    "operation": "index",
                    "throughput": {
                        "min": 450,
                        "median": 450,
                        "max": 452,
                        "unit": "docs/s"
                    },
                    "latency": {
                        "50": 340,
                        "100": 376,
                    },
                    "service_time": {
                        "50": 341,
                        "100": 376
                    },
                    "error_rate": 0.0
                }
            ],
            "young_gc_time": 68,
            "old_gc_time": 0
        }

        s = reporter.Stats(d)
        metric_list = s.as_flat_list()
        self.assertEqual({
            "name": "throughput",
            "operation": "index",
            "value": {
                "min": 450,
                "median": 450,
                "max": 452,
                "unit": "docs/s"
            }
        }, select(metric_list, "throughput", "index"))

        self.assertEqual({
            "name": "service_time",
            "operation": "index",
            "value": {
                "50": 341,
                "100": 376
            }
        }, select(metric_list, "service_time", "index"))

        self.assertEqual({
            "name": "latency",
            "operation": "index",
            "value": {
                "50": 340,
                "100": 376
            }
        }, select(metric_list, "latency", "index"))

        self.assertEqual({
            "name": "error_rate",
            "operation": "index",
            "value": {
                "single": 0.0
            }
        }, select(metric_list, "error_rate", "index"))

        self.assertEqual({
            "name": "young_gc_time",
            "value": {
                "single": 68
            }
        }, select(metric_list, "young_gc_time"))

        self.assertEqual({
            "name": "old_gc_time",
            "value": {
                "single": 0
            }
        }, select(metric_list, "old_gc_time"))


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
