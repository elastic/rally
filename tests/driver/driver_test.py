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

import asyncio
import collections
import io
import threading
import time
import unittest.mock as mock
from datetime import datetime
from unittest import TestCase

import elasticsearch

from esrally import metrics, track, exceptions, config
from esrally.driver import driver, runner, scheduler
from esrally.track import params
from tests import run_async, as_future


class DriverTestParamSource:
    def __init__(self, track=None, params=None, **kwargs):
        if params is None:
            params = {}
        self._indices = track.indices
        self._params = params
        self._current = 1
        self._total = params.get("size")
        self.infinite = self._total is None

    def partition(self, partition_index, total_partitions):
        return self

    @property
    def percent_completed(self):
        if self.infinite:
            return None
        return self._current / self._total

    def params(self):
        if not self.infinite and self._current > self._total:
            raise StopIteration()
        self._current += 1
        return self._params


class DriverTests(TestCase):
    class Holder:
        def __init__(self, all_hosts=None, all_client_options=None):
            self.all_hosts = all_hosts
            self.all_client_options = all_client_options
            self.uses_static_responses = False

    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        self.cfg = None
        self.track = None

    class StaticClientFactory:
        PATCHER = None

        def __init__(self, *args, **kwargs):
            DriverTests.StaticClientFactory.PATCHER = mock.patch("elasticsearch.Elasticsearch")
            self.es = DriverTests.StaticClientFactory.PATCHER.start()
            self.es.indices.stats.return_value = {"mocked": True}

        def create(self):
            return self.es

        @classmethod
        def close(cls):
            DriverTests.StaticClientFactory.PATCHER.stop()

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "system", "time.start", datetime(year=2017, month=8, day=20, hour=1, minute=0, second=0))
        self.cfg.add(config.Scope.application, "system", "race.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        self.cfg.add(config.Scope.application, "system", "available.cores", 8)
        self.cfg.add(config.Scope.application, "node", "root.dir", "/tmp")
        self.cfg.add(config.Scope.application, "track", "challenge.name", "default")
        self.cfg.add(config.Scope.application, "track", "params", {})
        self.cfg.add(config.Scope.application, "track", "test.mode.enabled", True)
        self.cfg.add(config.Scope.application, "telemetry", "devices", [])
        self.cfg.add(config.Scope.application, "telemetry", "params", {})
        self.cfg.add(config.Scope.application, "mechanic", "car.names", ["default"])
        self.cfg.add(config.Scope.application, "mechanic", "skip.rest.api.check", True)
        self.cfg.add(config.Scope.application, "client", "hosts",
                     DriverTests.Holder(all_hosts={"default": ["localhost:9200"]}))
        self.cfg.add(config.Scope.application, "client", "options", DriverTests.Holder(all_client_options={"default": {}}))
        self.cfg.add(config.Scope.application, "driver", "load_driver_hosts", ["localhost"])
        self.cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")

        default_challenge = track.Challenge("default", default=True, schedule=[
            track.Task(name="index", operation=track.Operation("index", operation_type=track.OperationType.Bulk), clients=4)
        ])
        another_challenge = track.Challenge("other", default=False)
        self.track = track.Track(name="unittest", description="unittest track", challenges=[another_challenge, default_challenge])

    def tearDown(self):
        DriverTests.StaticClientFactory.close()

    def create_test_driver_target(self):
        client = "client_marker"
        attrs = {
            "create_client.return_value": client
        }
        return mock.Mock(**attrs)

    @mock.patch("esrally.utils.net.resolve")
    def test_start_benchmark_and_prepare_track(self, resolve):
        # override load driver host
        self.cfg.add(config.Scope.applicationOverride, "driver", "load_driver_hosts", ["10.5.5.1", "10.5.5.2"])
        resolve.side_effect = ["10.5.5.1", "10.5.5.2"]

        target = self.create_test_driver_target()
        d = driver.Driver(target, self.cfg, es_client_factory_class=DriverTests.StaticClientFactory)
        d.prepare_benchmark(t=self.track)

        target.prepare_track.assert_called_once_with(["10.5.5.1", "10.5.5.2"], self.cfg, self.track)
        d.start_benchmark()

        target.create_client.assert_has_calls(calls=[
            mock.call("10.5.5.1"),
            mock.call("10.5.5.1"),
            mock.call("10.5.5.2"),
            mock.call("10.5.5.2"),
        ])

        # Did we start all load generators? There is no specific mock assert for this...
        self.assertEqual(4, target.start_worker.call_count)

    def test_assign_drivers_round_robin(self):
        target = self.create_test_driver_target()
        d = driver.Driver(target, self.cfg, es_client_factory_class=DriverTests.StaticClientFactory)

        d.prepare_benchmark(t=self.track)

        target.prepare_track.assert_called_once_with(["localhost"], self.cfg, self.track)

        d.start_benchmark()

        target.create_client.assert_has_calls(calls=[
            mock.call("localhost"),
            mock.call("localhost"),
            mock.call("localhost"),
            mock.call("localhost"),
        ])

        # Did we start all load generators? There is no specific mock assert for this...
        self.assertEqual(4, target.start_worker.call_count)

    def test_client_reaches_join_point_others_still_executing(self):
        target = self.create_test_driver_target()
        d = driver.Driver(target, self.cfg, es_client_factory_class=DriverTests.StaticClientFactory)

        d.prepare_benchmark(t=self.track)
        d.start_benchmark()

        self.assertEqual(0, len(d.workers_completed_current_step))

        d.joinpoint_reached(worker_id=0,
                            worker_local_timestamp=10,
                            task_allocations=[driver.ClientAllocation(client_id=0, task=driver.JoinPoint(id=0))])

        self.assertEqual(1, len(d.workers_completed_current_step))

        self.assertEqual(0, target.on_task_finished.call_count)
        self.assertEqual(0, target.drive_at.call_count)

    def test_client_reaches_join_point_which_completes_parent(self):
        target = self.create_test_driver_target()
        d = driver.Driver(target, self.cfg, es_client_factory_class=DriverTests.StaticClientFactory)

        d.prepare_benchmark(t=self.track)
        d.start_benchmark()

        self.assertEqual(0, len(d.workers_completed_current_step))

        d.joinpoint_reached(worker_id=0,
                            worker_local_timestamp=10,
                            task_allocations=[
                                driver.ClientAllocation(client_id=0,
                                                        task=driver.JoinPoint(id=0,
                                                                              clients_executing_completing_task=[0]))])

        self.assertEqual(-1, d.current_step)
        self.assertEqual(1, len(d.workers_completed_current_step))
        # notified all drivers that they should complete the current task ASAP
        self.assertEqual(4, target.complete_current_task.call_count)

        # awaiting responses of other clients
        d.joinpoint_reached(worker_id=1,
                            worker_local_timestamp=11,
                            task_allocations=[
                                driver.ClientAllocation(client_id=1,
                                                        task=driver.JoinPoint(id=0,
                                                                              clients_executing_completing_task=[0]))])

        self.assertEqual(-1, d.current_step)
        self.assertEqual(2, len(d.workers_completed_current_step))

        d.joinpoint_reached(worker_id=2,
                            worker_local_timestamp=12,
                            task_allocations=[
                                driver.ClientAllocation(client_id=2,
                                                        task=driver.JoinPoint(id=0,
                                                                              clients_executing_completing_task=[0]))])
        self.assertEqual(-1, d.current_step)
        self.assertEqual(3, len(d.workers_completed_current_step))

        d.joinpoint_reached(worker_id=3,
                            worker_local_timestamp=13,
                            task_allocations=[
                                driver.ClientAllocation(client_id=3,
                                                        task=driver.JoinPoint(id=0,
                                                                              clients_executing_completing_task=[0]))])

        # by now the previous step should be considered completed and we are at the next one
        self.assertEqual(0, d.current_step)
        self.assertEqual(0, len(d.workers_completed_current_step))

        # this requires at least Python 3.6
        # target.on_task_finished.assert_called_once()
        self.assertEqual(1, target.on_task_finished.call_count)
        self.assertEqual(4, target.drive_at.call_count)


def op(name, operation_type):
    return track.Operation(name, operation_type, param_source="driver-test-param-source")


class SamplePostprocessorTests(TestCase):
    def throughput(self, absolute_time, relative_time, value):
        return mock.call(name="throughput",
                         value=value,
                         unit="docs/s",
                         task="index",
                         operation="index-op",
                         operation_type="bulk",
                         sample_type=metrics.SampleType.Normal,
                         absolute_time=absolute_time,
                         relative_time=relative_time,
                         meta_data={})

    def service_time(self, absolute_time, relative_time, value):
        return self.request_metric(absolute_time, relative_time, "service_time", value)

    def processing_time(self, absolute_time, relative_time, value):
        return self.request_metric(absolute_time, relative_time, "processing_time", value)

    def latency(self, absolute_time, relative_time, value):
        return self.request_metric(absolute_time, relative_time, "latency", value)

    def request_metric(self, absolute_time, relative_time, name, value):
        return mock.call(name=name,
                         value=value,
                         unit="ms",
                         task="index",
                         operation="index-op",
                         operation_type="bulk",
                         sample_type=metrics.SampleType.Normal,
                         absolute_time=absolute_time,
                         relative_time=relative_time,
                         meta_data={})

    @mock.patch("esrally.metrics.MetricsStore")
    def test_all_samples(self, metrics_store):
        post_process = driver.SamplePostprocessor(metrics_store,
                                                  downsample_factor=1,
                                                  track_meta_data={},
                                                  challenge_meta_data={})

        task = track.Task("index", track.Operation("index-op", "bulk", param_source="driver-test-param-source"))
        samples = [
            driver.Sample(0, 38598, 24, 0, task, metrics.SampleType.Normal, None, 0.01, 0.007, 0.009, None, 5000, "docs", 1, 1 / 2),
            driver.Sample(0, 38599, 25, 0, task, metrics.SampleType.Normal, None, 0.01, 0.007, 0.009, None, 5000, "docs", 2, 2 / 2),
        ]

        post_process(samples)

        calls = [
            self.latency(38598, 24, 10.0), self.service_time(38598, 24, 7.0), self.processing_time(38598, 24, 9.0),
            self.latency(38599, 25, 10.0), self.service_time(38599, 25, 7.0), self.processing_time(38599, 25, 9.0),
            self.throughput(38598, 24, 5000),
            self.throughput(38599, 25, 5000),
        ]
        metrics_store.put_value_cluster_level.assert_has_calls(calls)

    @mock.patch("esrally.metrics.MetricsStore")
    def test_downsamples(self, metrics_store):
        post_process = driver.SamplePostprocessor(metrics_store,
                                                  downsample_factor=2,
                                                  track_meta_data={},
                                                  challenge_meta_data={})

        task = track.Task("index", track.Operation("index-op", "bulk", param_source="driver-test-param-source"))

        samples = [
            driver.Sample(0, 38598, 24, 0, task, metrics.SampleType.Normal, None, 0.01, 0.007, 0.009, None, 5000, "docs", 1, 1 / 2),
            driver.Sample(0, 38599, 25, 0, task, metrics.SampleType.Normal, None, 0.01, 0.007, 0.009, None, 5000, "docs", 2, 2 / 2),
        ]

        post_process(samples)

        calls = [
            # only the first out of two request samples is included, throughput metrics are still complete
            self.latency(38598, 24, 10.0), self.service_time(38598, 24, 7.0), self.processing_time(38598, 24, 9.0),
            self.throughput(38598, 24, 5000),
            self.throughput(38599, 25, 5000),
        ]
        metrics_store.put_value_cluster_level.assert_has_calls(calls)

    @mock.patch("esrally.metrics.MetricsStore")
    def test_dependent_samples(self, metrics_store):
        post_process = driver.SamplePostprocessor(metrics_store,
                                                  downsample_factor=1,
                                                  track_meta_data={},
                                                  challenge_meta_data={})

        task = track.Task("index", track.Operation("index-op", "bulk", param_source="driver-test-param-source"))
        samples = [
            driver.Sample(0, 38598, 24, 0, task, metrics.SampleType.Normal, None, 0.01, 0.007, 0.009, None, 5000, "docs", 1, 1 / 2,
                          dependent_timing=[
                              {
                                  "absolute_time": 38601,
                                  "request_start": 25,
                                  "service_time": 0.05,
                                  "operation": "index-op",
                                  "operation-type": "bulk"
                              },
                              {
                                  "absolute_time": 38602,
                                  "request_start": 26,
                                  "service_time": 0.08,
                                  "operation": "index-op",
                                  "operation-type": "bulk"
                              }
                          ]),
        ]

        post_process(samples)

        calls = [
            self.latency(38598, 24, 10.0), self.service_time(38598, 24, 7.0), self.processing_time(38598, 24, 9.0),
            # dependent timings
            self.service_time(38601, 25, 50.0),
            self.service_time(38602, 26, 80.0),
            self.throughput(38598, 24, 5000),
        ]
        metrics_store.put_value_cluster_level.assert_has_calls(calls)


class WorkerAssignmentTests(TestCase):
    def test_single_host_assignment_clients_matches_cores(self):
        host_configs = [{
            "host": "localhost",
            "cores": 4
        }]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=4)

        self.assertEqual([
            {
                "host": "localhost",
                "workers": [
                    [0],
                    [1],
                    [2],
                    [3]
                ]
            }
        ], assignments)

    def test_single_host_assignment_more_clients_than_cores(self):
        host_configs = [{
            "host": "localhost",
            "cores": 4
        }]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=6)

        self.assertEqual([
            {
                "host": "localhost",
                "workers": [
                    [0, 1],
                    [2, 3],
                    [4],
                    [5]
                ]
            }
        ], assignments)

    def test_single_host_assignment_less_clients_than_cores(self):
        host_configs = [{
            "host": "localhost",
            "cores": 4
        }]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=2)

        self.assertEqual([
            {
                "host": "localhost",
                "workers": [
                    [0],
                    [1],
                    [],
                    []
                ]
            }
        ], assignments)

    def test_multiple_host_assignment_more_clients_than_cores(self):
        host_configs = [
            {
                "host": "host-a",
                "cores": 4
            },
            {
                "host": "host-b",
                "cores": 4
            }
        ]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=16)

        self.assertEqual([
            {
                "host": "host-a",
                "workers": [
                    [0, 1],
                    [2, 3],
                    [4, 5],
                    [6, 7]
                ]
            },
            {
                "host": "host-b",
                "workers": [
                    [8, 9],
                    [10, 11],
                    [12, 13],
                    [14, 15]
                ]
            }
        ], assignments)

    def test_multiple_host_assignment_less_clients_than_cores(self):
        host_configs = [
            {
                "host": "host-a",
                "cores": 4
            },
            {
                "host": "host-b",
                "cores": 4
            }
        ]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=4)

        self.assertEqual([
            {
                "host": "host-a",
                "workers": [
                    [0],
                    [1],
                    [],
                    []
                ]
            },
            {
                "host": "host-b",
                "workers": [
                    [2],
                    [3],
                    [],
                    []
                ]
            }
        ], assignments)

    def test_uneven_assignment_across_hosts(self):
        host_configs = [
            {
                "host": "host-a",
                "cores": 4
            },
            {
                "host": "host-b",
                "cores": 4
            },
            {
                "host": "host-c",
                "cores": 4
            }
        ]

        assignments = driver.calculate_worker_assignments(host_configs, client_count=17)

        self.assertEqual([
            {
                "host": "host-a",
                "workers": [
                    [0, 1],
                    [2, 3],
                    [4],
                    [5]
                ]
            },
            {
                "host": "host-b",
                "workers": [
                    [6, 7],
                    [8, 9],
                    [10],
                    [11]
                ]
            },
            {
                "host": "host-c",
                "workers": [
                    [12, 13],
                    [14],
                    [15],
                    [16]
                ]
            }
        ], assignments)


class AllocatorTests(TestCase):
    def setUp(self):
        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)

    def ta(self, task, client_index_in_task, global_client_index=None, total_clients=None):
        return driver.TaskAllocation(task,
                                     client_index_in_task,
                                     client_index_in_task if global_client_index is None else global_client_index,
                                     task.clients if total_clients is None else total_clients)

    def test_allocates_one_task(self):
        task = track.Task("index", op("index", track.OperationType.Bulk))

        allocator = driver.Allocator([task])

        self.assertEqual(1, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{task}], allocator.tasks_per_joinpoint)

    def test_allocates_two_serial_tasks(self):
        task = track.Task("index", op("index", track.OperationType.Bulk))

        allocator = driver.Allocator([task, task])

        self.assertEqual(1, allocator.clients)
        # we have two operations and three join points
        self.assertEqual(5, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.join_points))
        self.assertEqual([{task}, {task}], allocator.tasks_per_joinpoint)

    def test_allocates_two_parallel_tasks(self):
        task = track.Task("index", op("index", track.OperationType.Bulk))

        allocator = driver.Allocator([track.Parallel([task, task])])

        self.assertEqual(2, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.allocations[1]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{task}], allocator.tasks_per_joinpoint)
        for join_point in allocator.join_points:
            self.assertFalse(join_point.preceding_task_completes_parent)
            self.assertEqual(0, join_point.num_clients_executing_completing_task)

    def test_a_task_completes_the_parallel_structure(self):
        taskA = track.Task("index-completing", op("index", track.OperationType.Bulk), completes_parent=True)
        taskB = track.Task("index-non-completing", op("index", track.OperationType.Bulk))

        allocator = driver.Allocator([track.Parallel([taskA, taskB])])

        self.assertEqual(2, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.allocations[1]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{taskA, taskB}], allocator.tasks_per_joinpoint)
        final_join_point = allocator.join_points[1]
        self.assertTrue(final_join_point.preceding_task_completes_parent)
        self.assertEqual(1, final_join_point.num_clients_executing_completing_task)
        self.assertEqual([0], final_join_point.clients_executing_completing_task)

    def test_allocates_mixed_tasks(self):
        index = track.Task("index", op("index", track.OperationType.Bulk))
        stats = track.Task("stats", op("stats", track.OperationType.IndexStats))
        search = track.Task("search", op("search", track.OperationType.Search))

        allocator = driver.Allocator([index,
                                      track.Parallel([index, stats, stats]),
                                      index,
                                      index,
                                      track.Parallel([search, search, search])])

        self.assertEqual(3, allocator.clients)

        # 1 join point, 1 op, 1 jp, 1 (parallel) op, 1 jp, 1 op, 1 jp, 1 op, 1 jp, 1 (parallel) op, 1 jp
        self.assertEqual(11, len(allocator.allocations[0]))
        self.assertEqual(11, len(allocator.allocations[1]))
        self.assertEqual(11, len(allocator.allocations[2]))
        self.assertEqual(6, len(allocator.join_points))
        self.assertEqual([{index}, {index, stats}, {index}, {index}, {search}], allocator.tasks_per_joinpoint)
        for join_point in allocator.join_points:
            self.assertFalse(join_point.preceding_task_completes_parent)
            self.assertEqual(0, join_point.num_clients_executing_completing_task)

    # TODO (follow-up PR): We should probably forbid this
    def test_allocates_more_tasks_than_clients(self):
        index_a = track.Task("index-a", op("index-a", track.OperationType.Bulk))
        index_b = track.Task("index-b", op("index-b", track.OperationType.Bulk), completes_parent=True)
        index_c = track.Task("index-c", op("index-c", track.OperationType.Bulk))
        index_d = track.Task("index-d", op("index-d", track.OperationType.Bulk))
        index_e = track.Task("index-e", op("index-e", track.OperationType.Bulk))

        allocator = driver.Allocator([track.Parallel(tasks=[index_a, index_b, index_c, index_d, index_e], clients=2)])

        self.assertEqual(2, allocator.clients)

        allocations = allocator.allocations

        # 2 clients
        self.assertEqual(2, len(allocations))
        # join_point, index_a, index_c, index_e, join_point
        self.assertEqual(5, len(allocations[0]))
        # we really have no chance to extract the join point so we just take what is there...
        self.assertEqual([
            allocations[0][0],
            self.ta(index_a, client_index_in_task=0, global_client_index=0, total_clients=2),
            self.ta(index_c, client_index_in_task=0, global_client_index=2, total_clients=2),
            self.ta(index_e, client_index_in_task=0, global_client_index=4, total_clients=2),
            allocations[0][4]
        ], allocations[0])
        # join_point, index_a, index_c, None, join_point
        self.assertEqual(5, len(allocator.allocations[1]))
        self.assertEqual([
            allocations[1][0],
            self.ta(index_b, client_index_in_task=0, global_client_index=1, total_clients=2),
            self.ta(index_d, client_index_in_task=0, global_client_index=3, total_clients=2),
            None,
            allocations[1][4]
        ], allocations[1])

        self.assertEqual([{index_a, index_b, index_c, index_d, index_e}], allocator.tasks_per_joinpoint)
        self.assertEqual(2, len(allocator.join_points))
        final_join_point = allocator.join_points[1]
        self.assertTrue(final_join_point.preceding_task_completes_parent)
        self.assertEqual(1, final_join_point.num_clients_executing_completing_task)
        self.assertEqual([1], final_join_point.clients_executing_completing_task)

    # TODO (follow-up PR): We should probably forbid this
    def test_considers_number_of_clients_per_subtask(self):
        index_a = track.Task("index-a", op("index-a", track.OperationType.Bulk))
        index_b = track.Task("index-b", op("index-b", track.OperationType.Bulk))
        index_c = track.Task("index-c", op("index-c", track.OperationType.Bulk), clients=2, completes_parent=True)

        allocator = driver.Allocator([track.Parallel(tasks=[index_a, index_b, index_c], clients=3)])

        self.assertEqual(3, allocator.clients)

        allocations = allocator.allocations

        # 3 clients
        self.assertEqual(3, len(allocations))

        # tasks that client 0 will execute:
        # join_point, index_a, index_c, join_point
        self.assertEqual(4, len(allocations[0]))
        # we really have no chance to extract the join point so we just take what is there...
        self.assertEqual([
            allocations[0][0],
            self.ta(index_a, client_index_in_task=0, global_client_index=0, total_clients=3),
            self.ta(index_c, client_index_in_task=1, global_client_index=3, total_clients=3),
            allocations[0][3]
        ], allocations[0])

        # task that client 1 will execute:
        # join_point, index_b, None, join_point
        self.assertEqual(4, len(allocator.allocations[1]))
        self.assertEqual([
            allocations[1][0],
            self.ta(index_b, client_index_in_task=0, global_client_index=1, total_clients=3),
            None,
            allocations[1][3]
        ], allocations[1])

        # tasks that client 2 will execute:
        self.assertEqual(4, len(allocator.allocations[2]))
        self.assertEqual([
            allocations[2][0],
            self.ta(index_c, client_index_in_task=0, global_client_index=2, total_clients=3),
            None,
            allocations[2][3]
        ], allocations[2])

        self.assertEqual([{index_a, index_b, index_c}], allocator.tasks_per_joinpoint)

        self.assertEqual(2, len(allocator.join_points))
        final_join_point = allocator.join_points[1]
        self.assertTrue(final_join_point.preceding_task_completes_parent)
        # task index_c has two clients, hence we have to wait for two clients to finish
        self.assertEqual(2, final_join_point.num_clients_executing_completing_task)
        self.assertEqual([2, 0], final_join_point.clients_executing_completing_task)


class MetricsAggregationTests(TestCase):
    def setUp(self):
        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)

    def test_different_sample_types(self):
        op = track.Operation("index", track.OperationType.Bulk, param_source="driver-test-param-source")

        samples = [
            driver.Sample(0, 1470838595, 21, 0, op, metrics.SampleType.Warmup, None, -1, -1, -1, None, 3000, "docs", 1, 1),
            driver.Sample(0, 1470838595.5, 21.5, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 2500, "docs", 1, 1),
        ]

        aggregated = self.calculate_global_throughput(samples)

        self.assertIn(op, aggregated)
        self.assertEqual(1, len(aggregated))

        throughput = aggregated[op]
        self.assertEqual(2, len(throughput))
        self.assertEqual((1470838595, 21, metrics.SampleType.Warmup, 3000, "docs/s"), throughput[0])
        self.assertEqual((1470838595.5, 21.5, metrics.SampleType.Normal, 3666.6666666666665, "docs/s"), throughput[1])

    def test_single_metrics_aggregation(self):
        op = track.Operation("index", track.OperationType.Bulk, param_source="driver-test-param-source")

        samples = [
            driver.Sample(0, 38595, 21, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 1, 1 / 9),
            driver.Sample(0, 38596, 22, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 2, 2 / 9),
            driver.Sample(0, 38597, 23, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 3, 3 / 9),
            driver.Sample(0, 38598, 24, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 4, 4 / 9),
            driver.Sample(0, 38599, 25, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 5, 5 / 9),
            driver.Sample(0, 38600, 26, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 6, 6 / 9),
            driver.Sample(1, 38598.5, 24.5, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 4.5, 7 / 9),
            driver.Sample(1, 38599.5, 25.5, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 5.5, 8 / 9),
            driver.Sample(1, 38600.5, 26.5, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, None, 5000, "docs", 6.5, 9 / 9)
        ]

        aggregated = self.calculate_global_throughput(samples)

        self.assertIn(op, aggregated)
        self.assertEqual(1, len(aggregated))

        throughput = aggregated[op]
        self.assertEqual(6, len(throughput))
        self.assertEqual((38595, 21, metrics.SampleType.Normal, 5000, "docs/s"), throughput[0])
        self.assertEqual((38596, 22, metrics.SampleType.Normal, 5000, "docs/s"), throughput[1])
        self.assertEqual((38597, 23, metrics.SampleType.Normal, 5000, "docs/s"), throughput[2])
        self.assertEqual((38598, 24, metrics.SampleType.Normal, 5000, "docs/s"), throughput[3])
        self.assertEqual((38599, 25, metrics.SampleType.Normal, 6000, "docs/s"), throughput[4])
        self.assertEqual((38600, 26, metrics.SampleType.Normal, 6666.666666666667, "docs/s"), throughput[5])
        # self.assertEqual((1470838600.5, 26.5, metrics.SampleType.Normal, 10000), throughput[6])

    def test_use_provided_throughput(self):
        op = track.Operation("index-recovery", track.OperationType.WaitForRecovery,
                             param_source="driver-test-param-source")

        samples = [
            driver.Sample(0, 38595, 21, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, 8000, 5000, "byte", 1, 1 / 3),
            driver.Sample(0, 38596, 22, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, 8000, 5000, "byte", 2, 2 / 3),
            driver.Sample(0, 38597, 23, 0, op, metrics.SampleType.Normal, None, -1, -1, -1, 8000, 5000, "byte", 3, 3 / 3),
        ]

        aggregated = self.calculate_global_throughput(samples)

        self.assertIn(op, aggregated)
        self.assertEqual(1, len(aggregated))

        throughput = aggregated[op]
        self.assertEqual(3, len(throughput))
        self.assertEqual((38595, 21, metrics.SampleType.Normal, 8000, "byte/s"), throughput[0])
        self.assertEqual((38596, 22, metrics.SampleType.Normal, 8000, "byte/s"), throughput[1])
        self.assertEqual((38597, 23, metrics.SampleType.Normal, 8000, "byte/s"), throughput[2])

    def calculate_global_throughput(self, samples):
        return driver.ThroughputCalculator().calculate(samples)


class SchedulerTests(TestCase):
    class RunnerWithProgress:
        def __init__(self, complete_after=3):
            self.completed = False
            self.percent_completed = 0.0
            self.calls = 0
            self.complete_after = complete_after

        async def __call__(self, *args, **kwargs):
            self.calls += 1
            if not self.completed:
                self.percent_completed = self.calls / self.complete_after
                self.completed = self.calls == self.complete_after
            else:
                self.percent_completed = 1.0

    class CustomComplexScheduler:
        def __init__(self, task):
            self.task = task
            # will be injected by Rally
            self.parameter_source = None

        def before_request(self, now):
            pass

        def after_request(self, now, weight, unit, meta_data):
            pass

        def next(self, current):
            return current

    async def assert_schedule(self, expected_schedule, schedule_handle, infinite_schedule=False):
        idx = 0
        schedule_handle.start()
        async for invocation_time, sample_type, progress_percent, runner, params in schedule_handle():
            schedule_handle.before_request(now=idx)
            exp_invocation_time, exp_sample_type, exp_progress_percent, exp_params = expected_schedule[idx]
            self.assertAlmostEqual(exp_invocation_time, invocation_time, msg="Invocation time for sample at index %d does not match" % idx)
            self.assertEqual(exp_sample_type, sample_type, "Sample type for sample at index %d does not match" % idx)
            self.assertEqual(exp_progress_percent, progress_percent, "Current progress for sample at index %d does not match" % idx)
            self.assertIsNotNone(runner, "runner must be defined")
            self.assertEqual(exp_params, params, "Parameters do not match")
            idx += 1
            # for infinite schedules we only check the first few elements
            if infinite_schedule and idx == len(expected_schedule):
                break
            # simulate that the request is done - we only support throttling based on request count (ops).
            schedule_handle.after_request(now=idx, weight=1, unit="ops", request_meta_data=None)
        if not infinite_schedule:
            self.assertEqual(len(expected_schedule), idx, msg="Number of elements in the schedules do not match")

    def setUp(self):
        self.test_track = track.Track(name="unittest")
        self.runner_with_progress = SchedulerTests.RunnerWithProgress()
        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        runner.register_default_runners()
        runner.register_runner("driver-test-runner-with-completion", self.runner_with_progress, async_runner=True)
        scheduler.register_scheduler("custom-complex-scheduler", SchedulerTests.CustomComplexScheduler)

    def tearDown(self):
        runner.remove_runner("driver-test-runner-with-completion")
        scheduler.remove_scheduler("custom-complex-scheduler")

    def test_injects_parameter_source_into_scheduler(self):
        task = track.Task(name="search",
                          schedule="custom-complex-scheduler",
                          operation=track.Operation(
                              name="search",
                              operation_type=track.OperationType.Search.to_hyphenated_string(),
                              param_source="driver-test-param-source"
                          ),
                          clients=4,
                          params={
                              "target-throughput": "5000 ops/s"
                          })

        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        param_source = track.operation_parameters(self.test_track, task)
        schedule = driver.schedule_for(task_allocation, param_source)

        self.assertIsNotNone(schedule.sched.parameter_source, "Parameter source has not been injected into scheduler")
        self.assertEqual(param_source, schedule.sched.parameter_source)

    @run_async
    async def test_search_task_one_client(self):
        task = track.Task("search", track.Operation("search", track.OperationType.Search.to_hyphenated_string(),
                                                    param_source="driver-test-param-source"),
                          warmup_iterations=3, iterations=5, clients=1, params={"target-throughput": 10, "clients": 1})
        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 1 / 8, {}),
            (0.1, metrics.SampleType.Warmup, 2 / 8, {}),
            (0.2, metrics.SampleType.Warmup, 3 / 8, {}),
            (0.3, metrics.SampleType.Normal, 4 / 8, {}),
            (0.4, metrics.SampleType.Normal, 5 / 8, {}),
            (0.5, metrics.SampleType.Normal, 6 / 8, {}),
            (0.6, metrics.SampleType.Normal, 7 / 8, {}),
            (0.7, metrics.SampleType.Normal, 8 / 8, {}),
        ]
        await self.assert_schedule(expected_schedule, schedule)

    @run_async
    async def test_search_task_two_clients(self):
        task = track.Task("search", track.Operation("search", track.OperationType.Search.to_hyphenated_string(),
                                                    param_source="driver-test-param-source"),
                          warmup_iterations=1, iterations=5, clients=2, params={"target-throughput": 10, "clients": 2})
        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 1 / 6, {}),
            (0.2, metrics.SampleType.Normal, 2 / 6, {}),
            (0.4, metrics.SampleType.Normal, 3 / 6, {}),
            (0.6, metrics.SampleType.Normal, 4 / 6, {}),
            (0.8, metrics.SampleType.Normal, 5 / 6, {}),
            (1.0, metrics.SampleType.Normal, 6 / 6, {}),
        ]
        await self.assert_schedule(expected_schedule, schedule)

    @run_async
    async def test_schedule_param_source_determines_iterations_no_warmup(self):
        # we neither define any time-period nor any iteration count on the task.
        task = track.Task("bulk-index", track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 3},
                                                        param_source="driver-test-param-source"),
                          clients=4, params={"target-throughput": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 1 / 3, {"body": ["a"], "size": 3}),
            (1.0, metrics.SampleType.Normal, 2 / 3, {"body": ["a"], "size": 3}),
            (2.0, metrics.SampleType.Normal, 3 / 3, {"body": ["a"], "size": 3}),
        ], schedule)

    @run_async
    async def test_schedule_param_source_determines_iterations_including_warmup(self):
        task = track.Task("bulk-index", track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 5},
                                                        param_source="driver-test-param-source"),
                          warmup_iterations=2, clients=4, params={"target-throughput": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Warmup, 1 / 5, {"body": ["a"], "size": 5}),
            (1.0, metrics.SampleType.Warmup, 2 / 5, {"body": ["a"], "size": 5}),
            (2.0, metrics.SampleType.Normal, 3 / 5, {"body": ["a"], "size": 5}),
            (3.0, metrics.SampleType.Normal, 4 / 5, {"body": ["a"], "size": 5}),
            (4.0, metrics.SampleType.Normal, 5 / 5, {"body": ["a"], "size": 5}),
        ], schedule)

    @run_async
    async def test_schedule_defaults_to_iteration_based(self):
        # no time-period and no iterations specified on the task. Also, the parameter source does not define a size.
        task = track.Task("bulk-index", track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"]},
                                                        param_source="driver-test-param-source"),
                          clients=1, params={"target-throughput": 4, "clients": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 1 / 1, {"body": ["a"]}),
        ], schedule)

    @run_async
    async def test_schedule_for_warmup_time_based(self):
        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 11},
                                                        param_source="driver-test-param-source"),
                          warmup_time_period=0, clients=4, params={"target-throughput": 4, "clients": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 1 / 11, {"body": ["a"], "size": 11}),
            (1.0, metrics.SampleType.Normal, 2 / 11, {"body": ["a"], "size": 11}),
            (2.0, metrics.SampleType.Normal, 3 / 11, {"body": ["a"], "size": 11}),
            (3.0, metrics.SampleType.Normal, 4 / 11, {"body": ["a"], "size": 11}),
            (4.0, metrics.SampleType.Normal, 5 / 11, {"body": ["a"], "size": 11}),
            (5.0, metrics.SampleType.Normal, 6 / 11, {"body": ["a"], "size": 11}),
            (6.0, metrics.SampleType.Normal, 7 / 11, {"body": ["a"], "size": 11}),
            (7.0, metrics.SampleType.Normal, 8 / 11, {"body": ["a"], "size": 11}),
            (8.0, metrics.SampleType.Normal, 9 / 11, {"body": ["a"], "size": 11}),
            (9.0, metrics.SampleType.Normal, 10 / 11, {"body": ["a"], "size": 11}),
            (10.0, metrics.SampleType.Normal, 11 / 11, {"body": ["a"], "size": 11}),
        ], schedule)

    @run_async
    async def test_infinite_schedule_without_progress_indication(self):
        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"]},
                                                        param_source="driver-test-param-source"),
                          warmup_time_period=0, clients=4, params={"target-throughput": 4, "clients": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (1.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (2.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (3.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (4.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
        ], schedule, infinite_schedule=True)

    @run_async
    async def test_finite_schedule_with_progress_indication(self):
        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 5},
                                                        param_source="driver-test-param-source"),
                          warmup_time_period=0, clients=4, params={"target-throughput": 4, "clients": 4})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 1 / 5, {"body": ["a"], "size": 5}),
            (1.0, metrics.SampleType.Normal, 2 / 5, {"body": ["a"], "size": 5}),
            (2.0, metrics.SampleType.Normal, 3 / 5, {"body": ["a"], "size": 5}),
            (3.0, metrics.SampleType.Normal, 4 / 5, {"body": ["a"], "size": 5}),
            (4.0, metrics.SampleType.Normal, 5 / 5, {"body": ["a"], "size": 5}),
        ], schedule, infinite_schedule=False)

    @run_async
    async def test_schedule_with_progress_determined_by_runner(self):
        task = track.Task("time-based", track.Operation("time-based", "driver-test-runner-with-completion",
                                                        params={"body": ["a"]},
                                                        param_source="driver-test-param-source"),
                          clients=1,
                          params={"target-throughput": 1, "clients": 1})

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        await self.assert_schedule([
            (0.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (1.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (2.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (3.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
            (4.0, metrics.SampleType.Normal, None, {"body": ["a"]}),
        ], schedule, infinite_schedule=True)

    @run_async
    async def test_schedule_for_time_based(self):
        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 11},
                                                        param_source="driver-test-param-source"),
                          ramp_up_time_period=0.1,
                          warmup_time_period=0.1,
                          time_period=0.1,
                          clients=1)

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule_handle = driver.schedule_for(task_allocation, param_source)
        schedule_handle.start()
        # first client does not wait
        self.assertEqual(0.0, schedule_handle.ramp_up_wait_time)
        schedule = schedule_handle()

        last_progress = -1

        async for invocation_time, sample_type, progress_percent, runner, params in schedule:
            # we're not throughput throttled
            self.assertEqual(0, invocation_time)
            if progress_percent <= 0.5:
                self.assertEqual(metrics.SampleType.Warmup, sample_type)
            else:
                self.assertEqual(metrics.SampleType.Normal, sample_type)
            self.assertTrue(last_progress < progress_percent)
            last_progress = progress_percent
            self.assertTrue(round(progress_percent, 2) >= 0.0, "progress should be >= 0.0 but was [%f]" % progress_percent)
            self.assertTrue(round(progress_percent, 2) <= 1.0, "progress should be <= 1.0 but was [%f]" % progress_percent)
            self.assertIsNotNone(runner, "runner must be defined")
            self.assertEqual({"body": ["a"], "size": 11}, params)

    @run_async
    async def test_schedule_for_time_based_with_multiple_clients(self):
        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={"body": ["a"], "size": 11},
                                                        param_source="driver-test-param-source"),
                          ramp_up_time_period=0.1,
                          warmup_time_period=0.1,
                          time_period=0.1,
                          clients=4)

        param_source = track.operation_parameters(self.test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                # assume this task is embedded in a parallel structure
                                                # with 8 clients in total
                                                client_index_in_task=0,
                                                global_client_index=4,
                                                total_clients=8)
        schedule_handle = driver.schedule_for(task_allocation, param_source)
        schedule_handle.start()
        # client number 4 out of 8 -> 0.1 * (4 / 8) = 0.05
        self.assertEqual(0.05, schedule_handle.ramp_up_wait_time)
        schedule = schedule_handle()

        last_progress = -1

        async for invocation_time, sample_type, progress_percent, runner, params in schedule:
            # we're not throughput throttled
            self.assertEqual(0, invocation_time)
            if progress_percent <= 0.5:
                self.assertEqual(metrics.SampleType.Warmup, sample_type)
            else:
                self.assertEqual(metrics.SampleType.Normal, sample_type)
            self.assertTrue(last_progress < progress_percent)
            last_progress = progress_percent
            self.assertTrue(round(progress_percent, 2) >= 0.0, "progress should be >= 0.0 but was [%f]" % progress_percent)
            self.assertTrue(round(progress_percent, 2) <= 1.0, "progress should be <= 1.0 but was [%f]" % progress_percent)
            self.assertIsNotNone(runner, "runner must be defined")
            self.assertEqual({"body": ["a"], "size": 11}, params)



class AsyncExecutorTests(TestCase):
    class NoopContextManager:
        def __init__(self, mock):
            self.mock = mock

        async def __aenter__(self):
            return self

        async def __call__(self, *args):
            return await self.mock(*args)

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

        def __str__(self):
            return str(self.mock)

    class StaticRequestTiming:
        def __init__(self, task_start):
            self.task_start = task_start
            self.current_request_start = self.task_start

        async def __aenter__(self):
            # pretend time advances on each request
            self.current_request_start += 5
            return self

        @property
        def request_start(self):
            return self.current_request_start

        @property
        def request_end(self):
            return self.current_request_start + 0.05

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    class RunnerWithProgress:
        def __init__(self, iterations=5):
            self.iterations_left = iterations
            self.iterations = iterations

        @property
        def completed(self):
            return self.iterations_left <= 0

        @property
        def percent_completed(self):
            return (self.iterations - self.iterations_left) / self.iterations

        async def __call__(self, es, params):
            self.iterations_left -= 1

    class RunnerOverridingThroughput:
        async def __call__(self, es, params):
            return {
                "weight": 1,
                "unit": "ops",
                "throughput": 1.23
            }

    def __init__(self, methodName):
        super().__init__(methodName)
        self.runner_with_progress = None

    @staticmethod
    def context_managed(mock):
        return AsyncExecutorTests.NoopContextManager(mock)

    def setUp(self):
        runner.register_default_runners()
        self.runner_with_progress = AsyncExecutorTests.RunnerWithProgress()
        self.runner_overriding_throughput = AsyncExecutorTests.RunnerOverridingThroughput()
        runner.register_runner("unit-test-recovery", self.runner_with_progress, async_runner=True)
        runner.register_runner("override-throughput", self.runner_overriding_throughput, async_runner=True)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_execute_schedule_in_throughput_mode(self, es):
        task_start = time.perf_counter()
        es.new_request_context.return_value = AsyncExecutorTests.StaticRequestTiming(task_start=task_start)

        es.bulk.return_value = as_future(io.StringIO('{"errors": false, "took": 8}'))

        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        test_track = track.Track(name="unittest", description="unittest track",
                                 indices=None,
                                 challenges=None)

        task = track.Task("time-based", track.Operation("time-based", track.OperationType.Bulk.to_hyphenated_string(),
                                                        params={
                                                            "body": ["action_metadata_line", "index_line"],
                                                            "action-metadata-present": True,
                                                            "bulk-size": 1,
                                                            "unit": "docs",
                                                            # we need this because DriverTestParamSource does not know
                                                            # that we only have one bulk and hence size() returns
                                                            # incorrect results
                                                            "size": 1
                                                        },
                                                        param_source="driver-test-param-source"),
                          warmup_time_period=0, clients=4)
        param_source = track.operation_parameters(test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        sampler = driver.Sampler(start_timestamp=task_start)
        cancel = threading.Event()
        complete = threading.Event()

        execute_schedule = driver.AsyncExecutor(client_id=2,
                                                task=task,
                                                schedule=schedule,
                                                es={
                                                    "default": es
                                                },
                                                sampler=sampler,
                                                cancel=cancel,
                                                complete=complete,
                                                on_error="continue")
        await execute_schedule()

        samples = sampler.samples

        self.assertTrue(len(samples) > 0)
        self.assertFalse(complete.is_set(), "Executor should not auto-complete a normal task")
        previous_absolute_time = -1.0
        previous_relative_time = -1.0
        for sample in samples:
            self.assertEqual(2, sample.client_id)
            self.assertEqual(task, sample.task)
            self.assertLess(previous_absolute_time, sample.absolute_time)
            previous_absolute_time = sample.absolute_time
            self.assertLess(previous_relative_time, sample.relative_time)
            previous_relative_time = sample.relative_time
            # we don't have any warmup time period
            self.assertEqual(metrics.SampleType.Normal, sample.sample_type)
            # latency equals service time in throughput mode
            self.assertEqual(sample.latency, sample.service_time)
            self.assertEqual(1, sample.total_ops)
            self.assertEqual("docs", sample.total_ops_unit)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_execute_schedule_with_progress_determined_by_runner(self, es):
        task_start = time.perf_counter()
        es.new_request_context.return_value = AsyncExecutorTests.StaticRequestTiming(task_start=task_start)

        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        test_track = track.Track(name="unittest", description="unittest track",
                                 indices=None,
                                 challenges=None)

        task = track.Task("time-based", track.Operation("time-based", operation_type="unit-test-recovery", params={
            "indices-to-restore": "*",
            # The runner will determine progress
            "size": None
        }, param_source="driver-test-param-source"), warmup_time_period=0, clients=4)
        param_source = track.operation_parameters(test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        sampler = driver.Sampler(start_timestamp=task_start)
        cancel = threading.Event()
        complete = threading.Event()

        execute_schedule = driver.AsyncExecutor(client_id=2,
                                                task=task,
                                                schedule=schedule,
                                                es={
                                                    "default": es
                                                },
                                                sampler=sampler,
                                                cancel=cancel,
                                                complete=complete,
                                                on_error="continue")
        await execute_schedule()

        samples = sampler.samples

        self.assertEqual(5, len(samples))
        self.assertTrue(self.runner_with_progress.completed)
        self.assertEqual(1.0, self.runner_with_progress.percent_completed)
        self.assertFalse(complete.is_set(), "Executor should not auto-complete a normal task")
        previous_absolute_time = -1.0
        previous_relative_time = -1.0
        for sample in samples:
            self.assertEqual(2, sample.client_id)
            self.assertEqual(task, sample.task)
            self.assertLess(previous_absolute_time, sample.absolute_time)
            previous_absolute_time = sample.absolute_time
            self.assertLess(previous_relative_time, sample.relative_time)
            previous_relative_time = sample.relative_time
            # we don't have any warmup time period
            self.assertEqual(metrics.SampleType.Normal, sample.sample_type)
            # throughput is not overridden and will be calculated later
            self.assertIsNone(sample.throughput)
            # latency equals service time in throughput mode
            self.assertEqual(sample.latency, sample.service_time)
            self.assertEqual(1, sample.total_ops)
            self.assertEqual("ops", sample.total_ops_unit)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_execute_schedule_runner_overrides_times(self, es):
        task_start = time.perf_counter()
        es.new_request_context.return_value = AsyncExecutorTests.StaticRequestTiming(task_start=task_start)

        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        test_track = track.Track(name="unittest", description="unittest track",
                                 indices=None,
                                 challenges=None)

        task = track.Task("override-throughput", track.Operation("override-throughput",
                                                                 operation_type="override-throughput", params={
                # we need this because DriverTestParamSource does not know that we only have one iteration and hence
                # size() returns incorrect results
                "size": 1
            },
                                                                 param_source="driver-test-param-source"),
                          warmup_iterations=0, iterations=1, clients=1)
        param_source = track.operation_parameters(test_track, task)
        task_allocation = driver.TaskAllocation(task=task,
                                                client_index_in_task=0,
                                                global_client_index=0,
                                                total_clients=task.clients)
        schedule = driver.schedule_for(task_allocation, param_source)

        sampler = driver.Sampler(start_timestamp=task_start)
        cancel = threading.Event()
        complete = threading.Event()

        execute_schedule = driver.AsyncExecutor(client_id=0,
                                                task=task,
                                                schedule=schedule,
                                                es={
                                                    "default": es
                                                },
                                                sampler=sampler,
                                                cancel=cancel,
                                                complete=complete,
                                                on_error="continue")
        await execute_schedule()

        samples = sampler.samples

        self.assertFalse(complete.is_set(), "Executor should not auto-complete a normal task")
        self.assertEqual(1, len(samples))
        sample = samples[0]
        self.assertEqual(0, sample.client_id)
        self.assertEqual(task, sample.task)
        # we don't have any warmup samples
        self.assertEqual(metrics.SampleType.Normal, sample.sample_type)
        self.assertEqual(sample.latency, sample.service_time)
        self.assertEqual(1, sample.total_ops)
        self.assertEqual("ops", sample.total_ops_unit)
        self.assertEqual(1.23, sample.throughput)
        self.assertIsNotNone(sample.service_time)
        self.assertIsNotNone(sample.time_period)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_execute_schedule_throughput_throttled(self, es):
        def perform_request(*args, **kwargs):
            return as_future()

        es.init_request_context.return_value = {
            "request_start": 0,
            "request_end": 10
        }
        # as this method is called several times we need to return a fresh instance every time as the previous
        # one has been "consumed".
        es.transport.perform_request.side_effect = perform_request

        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        test_track = track.Track(name="unittest", description="unittest track",
                                 indices=None,
                                 challenges=None)

        # in one second (0.5 warmup + 0.5 measurement) we should get 1000 [ops/s] / 4 [clients] = 250 samples
        for target_throughput, bounds in {10: [2, 4], 100: [24, 26], 1000: [235, 255]}.items():
            task = track.Task("time-based", track.Operation("time-based",
                                                            track.OperationType.Search.to_hyphenated_string(),
                                                            params={
                                                                "index": "_all",
                                                                "type": None,
                                                                "body": {"query": {"match_all": {}}},
                                                                "request-params": {},
                                                                "cache": False,
                                                                "response-compression-enabled": True
                                                            },
                                                            param_source="driver-test-param-source"),
                              warmup_time_period=0.5, time_period=0.5, clients=4,
                              params={"target-throughput": target_throughput, "clients": 4},
                              completes_parent=True)
            sampler = driver.Sampler(start_timestamp=0)

            cancel = threading.Event()
            complete = threading.Event()

            param_source = track.operation_parameters(test_track, task)
            task_allocation = driver.TaskAllocation(task=task,
                                                    client_index_in_task=0,
                                                    global_client_index=0,
                                                    total_clients=task.clients)
            schedule = driver.schedule_for(task_allocation, param_source)
            execute_schedule = driver.AsyncExecutor(client_id=0,
                                                    task=task,
                                                    schedule=schedule,
                                                    es={
                                                        "default": es
                                                    },
                                                    sampler=sampler,
                                                    cancel=cancel,
                                                    complete=complete,
                                                    on_error="continue")
            await execute_schedule()

            samples = sampler.samples

            sample_size = len(samples)
            lower_bound = bounds[0]
            upper_bound = bounds[1]
            self.assertTrue(lower_bound <= sample_size <= upper_bound,
                            msg="Expected sample size to be between %d and %d but was %d" % (lower_bound, upper_bound, sample_size))
            self.assertTrue(complete.is_set(), "Executor should auto-complete a task that terminates its parent")

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_cancel_execute_schedule(self, es):
        es.init_request_context.return_value = {
            "request_start": 0,
            "request_end": 10
        }
        es.bulk.return_value = as_future(io.StringIO('{"errors": false, "took": 8}'))

        params.register_param_source_for_name("driver-test-param-source", DriverTestParamSource)
        test_track = track.Track(name="unittest", description="unittest track",
                                 indices=None,
                                 challenges=None)

        # in one second (0.5 warmup + 0.5 measurement) we should get 1000 [ops/s] / 4 [clients] = 250 samples
        for target_throughput in [10, 100, 1000]:
            task = track.Task("time-based", track.Operation("time-based",
                                                            track.OperationType.Bulk.to_hyphenated_string(),
                                                            params={
                                                                "body": ["action_metadata_line", "index_line"],
                                                                "action-metadata-present": True,
                                                                "bulk-size": 1
                                                            },
                                                            param_source="driver-test-param-source"),
                              warmup_time_period=0.5, time_period=0.5, clients=4,
                              params={"target-throughput": target_throughput, "clients": 4})

            param_source = track.operation_parameters(test_track, task)
            task_allocation = driver.TaskAllocation(task=task,
                                                    client_index_in_task=0,
                                                    global_client_index=0,
                                                    total_clients=task.clients)
            schedule = driver.schedule_for(task_allocation, param_source)
            sampler = driver.Sampler(start_timestamp=0)

            cancel = threading.Event()
            complete = threading.Event()
            execute_schedule = driver.AsyncExecutor(client_id=0,
                                                    task=task,
                                                    schedule=schedule,
                                                    es={
                                                        "default": es
                                                    },
                                                    sampler=sampler,
                                                    cancel=cancel,
                                                    complete=complete,
                                                    on_error="continue")

            cancel.set()
            await execute_schedule()

            samples = sampler.samples

            sample_size = len(samples)
            self.assertEqual(0, sample_size)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_execute_schedule_aborts_on_error(self, es):
        class ExpectedUnitTestException(Exception):

            def __str__(self):
                return "expected unit test exception"

        def run(*args, **kwargs):
            raise ExpectedUnitTestException()

        class ScheduleHandle:
            def __init__(self):
                self.ramp_up_wait_time = 0

            def before_request(self, now):
                pass

            def after_request(self, now, weight, unit, meta_data):
                pass

            def start(self):
                pass

            async def __call__(self):
                invocations = [(0, metrics.SampleType.Warmup, 0, AsyncExecutorTests.context_managed(run), None)]
                for invocation in invocations:
                    yield invocation

        task = track.Task("no-op", track.Operation("no-op", track.OperationType.Bulk.to_hyphenated_string(),
                                                   params={},
                                                   param_source="driver-test-param-source"),
                          warmup_time_period=0.5, time_period=0.5, clients=4,
                          params={"clients": 4})

        sampler = driver.Sampler(start_timestamp=0)
        cancel = threading.Event()
        complete = threading.Event()
        execute_schedule = driver.AsyncExecutor(client_id=2,
                                                task=task,
                                                schedule=ScheduleHandle(),
                                                es={
                                                    "default": es
                                                },
                                                sampler=sampler,
                                                cancel=cancel,
                                                complete=complete,
                                                on_error="continue")

        with self.assertRaisesRegex(exceptions.RallyError, r"Cannot run task \[no-op\]: expected unit test exception"):
            await execute_schedule()

        self.assertEqual(0, es.call_count)

    @run_async
    async def test_execute_single_no_return_value(self):
        es = None
        params = None
        runner = mock.Mock()
        runner.return_value = as_future()

        ops, unit, request_meta_data = await driver.execute_single(self.context_managed(runner), es, params, on_error="continue")

        self.assertEqual(1, ops)
        self.assertEqual("ops", unit)
        self.assertEqual({"success": True}, request_meta_data)

    @run_async
    async def test_execute_single_tuple(self):
        es = None
        params = None
        runner = mock.Mock()
        runner.return_value = as_future(result=(500, "MB"))

        ops, unit, request_meta_data = await driver.execute_single(self.context_managed(runner), es, params, on_error="continue")

        self.assertEqual(500, ops)
        self.assertEqual("MB", unit)
        self.assertEqual({"success": True}, request_meta_data)

    @run_async
    async def test_execute_single_dict(self):
        es = None
        params = None
        runner = mock.Mock()
        runner.return_value = as_future({
            "weight": 50,
            "unit": "docs",
            "some-custom-meta-data": "valid",
            "http-status": 200
        })

        ops, unit, request_meta_data = await driver.execute_single(self.context_managed(runner), es, params, on_error="continue")

        self.assertEqual(50, ops)
        self.assertEqual("docs", unit)
        self.assertEqual({
            "some-custom-meta-data": "valid",
            "http-status": 200,
            "success": True
        }, request_meta_data)

    @run_async
    async def test_execute_single_with_connection_error_always_aborts(self):
        for on_error in ["abort", "continue"]:
            with self.subTest():
                es = None
                params = None
                # ES client uses pseudo-status "N/A" in this case...
                runner = mock.Mock(side_effect=as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host", None)))

                with self.assertRaises(exceptions.RallyAssertionError) as ctx:
                    await driver.execute_single(self.context_managed(runner), es, params, on_error=on_error)
                self.assertEqual(
                    "Request returned an error. Error type: transport, Description: no route to host",
                    ctx.exception.args[0])

    @run_async
    async def test_execute_single_with_http_400_aborts_when_specified(self):
        es = None
        params = None
        runner = mock.Mock(side_effect=
                           as_future(exception=elasticsearch.NotFoundError(404, "not found", "the requested document could not be found")))

        with self.assertRaises(exceptions.RallyAssertionError) as ctx:
            await driver.execute_single(self.context_managed(runner), es, params, on_error="abort")
        self.assertEqual(
            "Request returned an error. Error type: transport, Description: not found (the requested document could not be found)",
            ctx.exception.args[0])


    @run_async
    async def test_execute_single_with_http_400(self):
        es = None
        params = None
        runner = mock.Mock(side_effect=
                           as_future(exception=elasticsearch.NotFoundError(404, "not found", "the requested document could not be found")))

        ops, unit, request_meta_data = await driver.execute_single(
            self.context_managed(runner), es, params, on_error="continue")

        self.assertEqual(0, ops)
        self.assertEqual("ops", unit)
        self.assertEqual({
            "http-status": 404,
            "error-type": "transport",
            "error-description": "not found (the requested document could not be found)",
            "success": False
        }, request_meta_data)

    @run_async
    async def test_execute_single_with_http_413(self):
        es = None
        params = None
        runner = mock.Mock(side_effect=
                           as_future(exception=elasticsearch.NotFoundError(413, b"", b"")))

        ops, unit, request_meta_data = await driver.execute_single(
            self.context_managed(runner), es, params, on_error="continue")

        self.assertEqual(0, ops)
        self.assertEqual("ops", unit)
        self.assertEqual({
            "http-status": 413,
            "error-type": "transport",
            "error-description": "",
            "success": False
        }, request_meta_data)

    @run_async
    async def test_execute_single_with_key_error(self):
        class FailingRunner:
            async def __call__(self, *args):
                raise KeyError("bulk-size missing")

            def __str__(self):
                return "failing_mock_runner"

        es = None
        params = collections.OrderedDict()
        # simulating an error; this should be "bulk-size"
        params["bulk"] = 5000
        params["mode"] = "append"
        runner = FailingRunner()

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            await driver.execute_single(self.context_managed(runner), es, params, on_error="continue")
        self.assertEqual(
            "Cannot execute [failing_mock_runner]. Provided parameters are: ['bulk', 'mode']. Error: ['bulk-size missing'].",
            ctx.exception.args[0])


class AsyncProfilerTests(TestCase):
    @run_async
    async def test_profiler_is_a_transparent_wrapper(self):
        async def f(x):
            await asyncio.sleep(x)
            return x * 2

        profiler = driver.AsyncProfiler(f)
        start = time.perf_counter()
        # this should take roughly 1 second and should return something
        return_value = await profiler(1)
        end = time.perf_counter()
        self.assertEqual(2, return_value)
        duration = end - start
        self.assertTrue(0.9 <= duration <= 1.2, "Should sleep for roughly 1 second but took [%.2f] seconds." % duration)
