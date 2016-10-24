from unittest import TestCase

from esrally import metrics
from esrally.driver import driver
from esrally.track import track


class DriverTestParamSource:
    def __init__(self, indices=None, params=None, variation_count=1):
        if params is None:
            params = {}
        self.indices = indices
        self._params = params
        self._variation_count = variation_count

    def partition(self, partition_index, total_partitions):
        return self

    def variation_count(self):
        return self._variation_count

    def params(self):
        return self._params


class ScheduleTestCase(TestCase):
    def assert_schedule(self, expected_schedule, schedule):
        idx = 0
        for invocation_time, sample_type_calculator, current_it, total_it, runner, params in schedule:
            exp_invocation_time, exp_sample_type, exp_current_it, exp_total_it, exp_runner, exp_params = expected_schedule[idx]
            self.assertAlmostEqual(exp_invocation_time, invocation_time, msg="Expected invocation time does not match")
            self.assertEqual(exp_sample_type, sample_type_calculator(0), "Sample type does not match")
            self.assertEqual(exp_current_it, current_it, "Current iteration does not match")
            self.assertEqual(exp_total_it, total_it, "Number of iterations does not match")
            self.assertIsNotNone(runner, "runner must be defined")
            self.assertEqual(exp_params, params, "Parameters do not match")
            idx += 1


class AllocatorTests(TestCase):
    def test_allocates_one_task(self):
        op = track.Operation("index", track.OperationType.Index, DriverTestParamSource())
        task = track.Task(op)

        allocator = driver.Allocator([task])

        self.assertEqual(1, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{op}], allocator.operations_per_joinpoint)

    def test_allocates_two_serial_tasks(self):
        op = track.Operation("index", track.OperationType.Index, DriverTestParamSource())
        task = track.Task(op)

        allocator = driver.Allocator([task, task])

        self.assertEqual(1, allocator.clients)
        # we have two operations and three join points
        self.assertEqual(5, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.join_points))
        self.assertEqual([{op}, {op}], allocator.operations_per_joinpoint)

    def test_allocates_two_parallel_tasks(self):
        op = track.Operation("index", track.OperationType.Index, DriverTestParamSource())
        task = track.Task(op)

        allocator = driver.Allocator([track.Parallel([task, task])])

        self.assertEqual(2, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.allocations[1]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{op}], allocator.operations_per_joinpoint)

    def test_allocates_mixed_tasks(self):
        op1 = track.Operation("index", track.OperationType.Index, DriverTestParamSource())
        op2 = track.Operation("stats", track.OperationType.IndicesStats, DriverTestParamSource())
        op3 = track.Operation("search", track.OperationType.Search, DriverTestParamSource())

        index = track.Task(op1)
        stats = track.Task(op2)
        search = track.Task(op3)

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
        self.assertEqual([{op1}, {op1, op2}, {op1}, {op1}, {op3}], allocator.operations_per_joinpoint)

    def test_allocates_more_tasks_than_clients(self):
        op1 = track.Operation("index-a", track.OperationType.Index, DriverTestParamSource())
        op2 = track.Operation("index-b", track.OperationType.Index, DriverTestParamSource())
        op3 = track.Operation("index-c", track.OperationType.Index, DriverTestParamSource())
        op4 = track.Operation("index-d", track.OperationType.Index, DriverTestParamSource())
        op5 = track.Operation("index-e", track.OperationType.Index, DriverTestParamSource())

        index_a = track.Task(op1)
        index_b = track.Task(op2)
        index_c = track.Task(op3)
        index_d = track.Task(op4)
        index_e = track.Task(op5)

        allocator = driver.Allocator([track.Parallel(tasks=[index_a, index_b, index_c, index_d, index_e], clients=2)])

        self.assertEqual(2, allocator.clients)

        allocations = allocator.allocations

        self.assertEqual(2, len(allocations))
        # join_point, index_a, index_c, index_e, join_point
        self.assertEqual(5, len(allocations[0]))
        # we really have no chance to extract the join point so we just take what is there...
        self.assertEqual([allocations[0][0], index_a, index_c, index_e, allocations[0][4]], allocations[0])
        # join_point, index_a, index_c, None, join_point
        self.assertEqual(5, len(allocator.allocations[1]))
        self.assertEqual([allocations[1][0], index_b, index_d, None, allocations[1][4]], allocations[1])

        self.assertEqual([{op1, op2, op3, op4, op5}], allocator.operations_per_joinpoint)

    def test_considers_number_of_clients_per_subtask(self):
        op1 = track.Operation("index-a", track.OperationType.Index, DriverTestParamSource())
        op2 = track.Operation("index-b", track.OperationType.Index, DriverTestParamSource())
        op3 = track.Operation("index-c", track.OperationType.Index, DriverTestParamSource())

        index_a = track.Task(op1)
        index_b = track.Task(op2)
        index_c = track.Task(op3, clients=2)

        allocator = driver.Allocator([track.Parallel(tasks=[index_a, index_b, index_c], clients=3)])

        self.assertEqual(3, allocator.clients)

        allocations = allocator.allocations

        self.assertEqual(3, len(allocations))
        # join_point, index_a, index_c, join_point
        self.assertEqual(4, len(allocations[0]))
        # we really have no chance to extract the join point so we just take what is there...
        self.assertEqual([allocations[0][0], index_a, index_c, allocations[0][3]], allocations[0])
        # join_point, index_b, None, join_point
        self.assertEqual(4, len(allocator.allocations[1]))
        self.assertEqual([allocations[1][0], index_b, None, allocations[1][3]], allocations[1])

        self.assertEqual(4, len(allocator.allocations[2]))
        self.assertEqual([allocations[2][0], index_c, None, allocations[2][3]], allocations[2])

        self.assertEqual([{op1, op2, op3}], allocator.operations_per_joinpoint)


class MetricsAggregationTests(TestCase):
    def test_single_metrics_aggregation(self):
        op = track.Operation("index", track.OperationType.Index, DriverTestParamSource())

        samples = [
            driver.Sample(0, 1470838595, 21, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 1, 1, 9),
            driver.Sample(0, 1470838596, 22, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 2, 1, 9),
            driver.Sample(0, 1470838597, 23, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 3, 1, 9),
            driver.Sample(0, 1470838598, 24, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 4, 1, 9),
            driver.Sample(0, 1470838599, 25, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 5, 1, 9),
            driver.Sample(0, 1470838600, 26, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 6, 1, 9),
            driver.Sample(1, 1470838598.5, 24.5, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 4.5, 1, 9),
            driver.Sample(1, 1470838599.5, 25.5, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 5.5, 1, 9),
            driver.Sample(1, 1470838600.5, 26.5, op, metrics.SampleType.Normal, -1, -1, 5000, "docs", 6.5, 1, 9)
        ]

        aggregated = driver.calculate_global_throughput(samples)

        self.assertIn(op, aggregated)
        self.assertEqual(1, len(aggregated))

        throughput = aggregated[op]
        self.assertEqual(6, len(throughput))
        self.assertEqual((1470838595, 21, metrics.SampleType.Normal, 5000, "docs/s"), throughput[0])
        self.assertEqual((1470838596, 22, metrics.SampleType.Normal, 5000, "docs/s"), throughput[1])
        self.assertEqual((1470838597, 23, metrics.SampleType.Normal, 5000, "docs/s"), throughput[2])
        self.assertEqual((1470838598, 24, metrics.SampleType.Normal, 5000, "docs/s"), throughput[3])
        self.assertEqual((1470838599, 25, metrics.SampleType.Normal, 6000, "docs/s"), throughput[4])
        self.assertEqual((1470838600, 26, metrics.SampleType.Normal, 6666.666666666667, "docs/s"), throughput[5])
        # self.assertEqual((1470838600.5, 26.5, metrics.SampleType.Normal, 10000), throughput[6])


class SchedulerTests(ScheduleTestCase):
    def test_search_task_one_client(self):
        task = track.Task(track.Operation("search", track.OperationType.Search, params=DriverTestParamSource()),
                          warmup_iterations=3, iterations=5, clients=1, target_throughput=10)
        schedule = driver.schedule_for(task, 0)

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 0, 8, None, {}),
            (0.1, metrics.SampleType.Warmup, 1, 8, None, {}),
            (0.2, metrics.SampleType.Warmup, 2, 8, None, {}),
            (0, metrics.SampleType.Normal, 0, 8, None, {}),
            (0.1, metrics.SampleType.Normal, 1, 8, None, {}),
            (0.2, metrics.SampleType.Normal, 2, 8, None, {}),
            (0.3, metrics.SampleType.Normal, 3, 8, None, {}),
            (0.4, metrics.SampleType.Normal, 4, 8, None, {}),
        ]
        self.assert_schedule(expected_schedule, schedule)

    def test_search_task_two_clients(self):
        task = track.Task(track.Operation("search", track.OperationType.Search, params=DriverTestParamSource()),
                          warmup_iterations=2, iterations=10, clients=2, target_throughput=10)
        schedule = driver.schedule_for(task, 0)

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 0, 6, None, {}),
            (0, metrics.SampleType.Normal, 0, 6, None, {}),
            (0.2, metrics.SampleType.Normal, 1, 6, None, {}),
            (0.4, metrics.SampleType.Normal, 2, 6, None, {}),
            (0.6, metrics.SampleType.Normal, 3, 6, None, {}),
            (0.8, metrics.SampleType.Normal, 4, 6, None, {}),
        ]
        self.assert_schedule(expected_schedule, schedule)

    def test_schedule_for_warmup_time_based(self):
        task = track.Task(track.Operation("time-based", track.OperationType.Index,
                                          DriverTestParamSource(params={"body": ["a"]}, variation_count=11)),
                          warmup_time_period=0, clients=4, target_throughput=4)

        invocations = driver.schedule_for(task, 0)

        self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 0, 11, "runner", {"body": ["a"]}),
            (1.0, metrics.SampleType.Normal, 1, 11, "runner", {"body": ["a"]}),
            (2.0, metrics.SampleType.Normal, 2, 11, "runner", {"body": ["a"]}),
            (3.0, metrics.SampleType.Normal, 3, 11, "runner", {"body": ["a"]}),
            (4.0, metrics.SampleType.Normal, 4, 11, "runner", {"body": ["a"]}),
            (5.0, metrics.SampleType.Normal, 5, 11, "runner", {"body": ["a"]}),
            (6.0, metrics.SampleType.Normal, 6, 11, "runner", {"body": ["a"]}),
            (7.0, metrics.SampleType.Normal, 7, 11, "runner", {"body": ["a"]}),
            (8.0, metrics.SampleType.Normal, 8, 11, "runner", {"body": ["a"]}),
            (9.0, metrics.SampleType.Normal, 9, 11, "runner", {"body": ["a"]}),
            (10.0, metrics.SampleType.Normal, 10, 11, "runner", {"body": ["a"]}),
        ], list(invocations))
