# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
        cfg.add(config.Scope.application, "mechanic", "car.params", {})
        cfg.add(config.Scope.application, "mechanic", "plugin.params", {})
        cfg.add(config.Scope.application, "race", "user.tag", "")
        cfg.add(config.Scope.application, "race", "pipeline", "from-sources-skip-build")
        cfg.add(config.Scope.application, "track", "params", {})

        index = track.Task(name="index #1", operation=track.Operation(name="index", operation_type=track.OperationType.Bulk, params=None))
        challenge = track.Challenge(name="unittest", schedule=[index], default=True)
        t = track.Track("unittest", "unittest-track", challenges=[challenge])

        store = metrics.metrics_store(cfg, read_only=False, track=t, challenge=challenge)

        store.put_value_cluster_level("throughput", 500, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
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
        store.put_value_cluster_level("service_time", 210, unit="ms", task="index #1", operation_type=track.OperationType.Bulk,
                                      meta_data={"success": True})
        store.put_doc(doc={
            "name": "ml_processing_time",
            "job": "benchmark_ml_job_1",
            "min": 2.2,
            "mean": 12.3,
            "median": 17.2,
            "max": 36.0,
            "unit": "ms"
        }, level=metrics.MetaInfoScope.cluster)
        store.put_count_node_level("rally-node-0", "final_index_size_bytes", 2048, unit="bytes")
        store.put_count_node_level("rally-node-1", "final_index_size_bytes", 4096, unit="bytes")

        stats = reporter.calculate_results(store, metrics.create_race(cfg, t, challenge))

        del store

        opm = stats.metrics("index #1")
        self.assertEqual(collections.OrderedDict(
            [("min", 500), ("mean", 1125), ("median", 1000), ("max", 2000), ("unit", "docs/s")]), opm["throughput"])
        self.assertEqual(collections.OrderedDict([("50_0", 220), ("100_0", 225), ("mean", 215)]), opm["latency"])
        self.assertEqual(collections.OrderedDict([("50_0", 200), ("100_0", 210), ("mean", 200)]), opm["service_time"])
        self.assertAlmostEqual(0.3333333333333333, opm["error_rate"])

        self.assertEqual(6144, stats.index_size)
        self.assertEqual(1, len(stats.ml_processing_time))
        self.assertEqual("benchmark_ml_job_1", stats.ml_processing_time[0]["job"])
        self.assertEqual(2.2, stats.ml_processing_time[0]["min"])
        self.assertEqual(12.3, stats.ml_processing_time[0]["mean"])
        self.assertEqual(17.2, stats.ml_processing_time[0]["median"])
        self.assertEqual(36.0, stats.ml_processing_time[0]["max"])
        self.assertEqual("ms", stats.ml_processing_time[0]["unit"])


def select(l, name, operation=None, job=None, node=None):
    for item in l:
        if item["name"] == name and item.get("operation") == operation and item.get("node") == node and item.get("job") == job:
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
                    "error_rate": 0.0,
                    "meta": {
                        "clients": 8,
                        "phase": "idx"
                    }
                },
                {
                    "task": "search #2",
                    "operation": "search",
                    "throughput": {
                        "min": 9,
                        "median": 10,
                        "max": 12,
                        "unit": "ops/s"
                    },
                    "latency": {
                        "50": 99,
                        "100": 111,
                    },
                    "service_time": {
                        "50": 98,
                        "100": 110
                    },
                    "error_rate": 0.1
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
            "ml_processing_time": [
                {
                    "job": "job_1",
                    "min": 3.3,
                    "mean": 5.2,
                    "median": 5.8,
                    "max": 12.34
                },
                {
                    "job": "job_2",
                    "min": 3.55,
                    "mean": 4.2,
                    "median": 4.9,
                    "max": 9.4
                },
            ],
            "young_gc_time": 68,
            "old_gc_time": 0,
            "merge_time": 3702,
            "merge_time_per_shard": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms"
            },
            "merge_count": 2,
            "refresh_time": 596,
            "refresh_time_per_shard": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms"
            },
            "refresh_count": 10,
            "flush_time": None,
            "flush_time_per_shard": {},
            "flush_count": 0
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
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "throughput", operation="index"))

        self.assertEqual({
            "name": "service_time",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 341,
                "100": 376
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "service_time", operation="index"))

        self.assertEqual({
            "name": "latency",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 340,
                "100": 376
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "latency", operation="index"))

        self.assertEqual({
            "name": "error_rate",
            "task": "index #1",
            "operation": "index",
            "value": {
                "single": 0.0
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "error_rate", operation="index"))

        self.assertEqual({
            "name": "throughput",
            "task": "search #2",
            "operation": "search",
            "value": {
                "min": 9,
                "median": 10,
                "max": 12,
                "unit": "ops/s"
            }
        }, select(metric_list, "throughput", operation="search"))

        self.assertEqual({
            "name": "service_time",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 98,
                "100": 110
            }
        }, select(metric_list, "service_time", operation="search"))

        self.assertEqual({
            "name": "latency",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 99,
                "100": 111
            }
        }, select(metric_list, "latency", operation="search"))

        self.assertEqual({
            "name": "error_rate",
            "task": "search #2",
            "operation": "search",
            "value": {
                "single": 0.1
            }
        }, select(metric_list, "error_rate", operation="search"))

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
            "name": "ml_processing_time",
            "job": "job_1",
            "value": {
                "min": 3.3,
                "mean": 5.2,
                "median": 5.8,
                "max": 12.34
            }
        }, select(metric_list, "ml_processing_time", job="job_1"))

        self.assertEqual({
            "name": "ml_processing_time",
            "job": "job_2",
            "value": {
                "min": 3.55,
                "mean": 4.2,
                "median": 4.9,
                "max": 9.4
            }
        }, select(metric_list, "ml_processing_time", job="job_2"))

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

        self.assertEqual({
            "name": "merge_time",
            "value": {
                "single": 3702
            }
        }, select(metric_list, "merge_time"))

        self.assertEqual({
            "name": "merge_time_per_shard",
            "value": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms"
            }
        }, select(metric_list, "merge_time_per_shard"))

        self.assertEqual({
            "name": "merge_count",
            "value": {
                "single": 2
            }
        }, select(metric_list, "merge_count"))

        self.assertEqual({
            "name": "refresh_time",
            "value": {
                "single": 596
            }
        }, select(metric_list, "refresh_time"))

        self.assertEqual({
            "name": "refresh_time_per_shard",
            "value": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms"
            }
        }, select(metric_list, "refresh_time_per_shard"))

        self.assertEqual({
            "name": "refresh_count",
            "value": {
                "single": 10
            }
        }, select(metric_list, "refresh_count"))

        self.assertIsNone(select(metric_list, "flush_time"))
        self.assertIsNone(select(metric_list, "flush_time_per_shard"))
        self.assertEqual({
            "name": "flush_count",
            "value": {
                "single": 0
            }
        }, select(metric_list, "flush_count"))


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
