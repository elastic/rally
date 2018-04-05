import collections
import datetime
from unittest import TestCase

from esrally import reporter, metrics, config, track


class StatsCalculatorTests(TestCase):
    def test_calculate_simple_index_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "system", "trial.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "mechanic", "car.names", ["unittest_car"])
        cfg.add(config.Scope.application, "race", "laps", 1)
        cfg.add(config.Scope.application, "race", "user.tag", "")
        cfg.add(config.Scope.application, "race", "pipeline", "from-sources-skip-build")
        cfg.add(config.Scope.application, "track", "params", {})

        index = track.Task(name="index #1", operation=track.Operation(name="index", operation_type=track.OperationType.Bulk, params=None))
        challenge = track.Challenge(name="unittest", schedule=[index], default=True)
        t = track.Track("unittest", "unittest-track", challenges=[challenge])

        store = metrics.metrics_store(cfg, read_only=False, track=t, challenge=challenge)
        store.lap = 1

        store.put_value_cluster_level("throughput", 500, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 2000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)

        store.put_value_cluster_level("latency", 2800, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup)
        store.put_value_cluster_level("latency", 200, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("latency", 220, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("latency", 225, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)

        store.put_value_cluster_level("service_time", 250, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup, meta_data={"success": False})
        store.put_value_cluster_level("service_time", 190, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      meta_data={"success": True})
        store.put_value_cluster_level("service_time", 200, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      meta_data={"success": False})
        store.put_value_cluster_level("service_time", 215, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      meta_data={"success": True})
        store.put_count_node_level("rally-node-0", "final_index_size_bytes", 2048, unit="bytes")
        store.put_count_node_level("rally-node-1", "final_index_size_bytes", 4096, unit="bytes")

        stats = reporter.calculate_results(store, metrics.create_race(cfg, t, challenge))

        del store

        opm = stats.metrics("index #1")
        self.assertEqual(collections.OrderedDict([("min", 500), ("median", 1000), ("max", 2000), ("unit", "docs/s")]), opm["throughput"])
        self.assertEqual(collections.OrderedDict([("50_0", 220), ("100_0", 225)]), opm["latency"])
        self.assertEqual(collections.OrderedDict([("50_0", 200), ("100_0", 215)]), opm["service_time"])
        self.assertAlmostEqual(0.3333333333333333, opm["error_rate"])

        self.assertEqual(6144, stats.index_size)


def select(l, name, operation=None, node=None):
    for item in l:
        if item["name"] == name and item.get("operation") == operation and item.get("node") == node:
            return item
    return None


class StatsTests(TestCase):
    def test_as_flat_list(self):
        d = {
            "op_metrics": [
                {
                    "task": "index #1",
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
            "node_metrics": [
                {
                    "node": "rally-node-0",
                    "startup_time": 3.4
                },
                {
                    "node": "rally-node-1",
                    "startup_time": 4.2
                }
            ],
            "young_gc_time": 68,
            "old_gc_time": 0
        }

        s = reporter.Stats(d)
        metric_list = s.as_flat_list()
        self.assertEqual({
            "name": "throughput",
            "task": "index #1",
            "operation": "index",
            "value": {
                "min": 450,
                "median": 450,
                "max": 452,
                "unit": "docs/s"
            }
        }, select(metric_list, "throughput", operation="index"))

        self.assertEqual({
            "name": "service_time",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 341,
                "100": 376
            }
        }, select(metric_list, "service_time", operation="index"))

        self.assertEqual({
            "name": "latency",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 340,
                "100": 376
            }
        }, select(metric_list, "latency", operation="index"))

        self.assertEqual({
            "name": "error_rate",
            "task": "index #1",
            "operation": "index",
            "value": {
                "single": 0.0
            }
        }, select(metric_list, "error_rate", operation="index"))

        self.assertEqual({
            "node": "rally-node-0",
            "name": "startup_time",
            "value": {
                "single": 3.4
            }
        }, select(metric_list, "startup_time", node="rally-node-0"))

        self.assertEqual({
            "node": "rally-node-1",
            "name": "startup_time",
            "value": {
                "single": 4.2
            }
        }, select(metric_list, "startup_time", node="rally-node-1"))

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


class FormatterTests(TestCase):
    def setUp(self):
        self.empty_header = ["Header"]
        self.empty_data = []

        self.metrics_header = ["Metric", "Task", "Baseline", "Contender", "Diff", "Unit"]
        self.metrics_data = [
            ["Min Throughput", "index", "17300", "18000", "700", "ops/s"],
            ["Median Throughput", "index", "17500", "18500", "1000", "ops/s"],
            ["Max Throughput", "index", "17700", "19000", "1300", "ops/s"]
        ]

    def test_formats_as_markdown(self):
        formatted = reporter.format_as_markdown(self.empty_header, self.empty_data)
        # 1 header line, 1 separation line + 0 data lines
        self.assertEqual(1 + 1 + 0, len(formatted.splitlines()))

        formatted = reporter.format_as_markdown(self.metrics_header, self.metrics_data)
        # 1 header line, 1 separation line + 3 data lines
        self.assertEqual(1 + 1 + 3, len(formatted.splitlines()))

    def test_formats_as_csv(self):
        formatted = reporter.format_as_csv(self.empty_header, self.empty_data)
        # 1 header line, no separation line + 0 data lines
        self.assertEqual(1 + 0, len(formatted.splitlines()))

        formatted = reporter.format_as_csv(self.metrics_header, self.metrics_data)
        # 1 header line, no separation line + 3 data lines
        self.assertEqual(1 + 3, len(formatted.splitlines()))
