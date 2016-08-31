from unittest import TestCase

from esrally import driver, track, metrics


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
        op = track.Operation("index", track.OperationType.Index)
        task = track.Task(op)

        allocator = driver.Allocator([task])

        self.assertEqual(1, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{op}], allocator.operations_per_joinpoint)

    def test_allocates_two_serial_tasks(self):
        op = track.Operation("index", track.OperationType.Index)
        task = track.Task(op)

        allocator = driver.Allocator([task, task])

        self.assertEqual(1, allocator.clients)
        # we have two operations and three join points
        self.assertEqual(5, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.join_points))
        self.assertEqual([{op}, {op}], allocator.operations_per_joinpoint)

    def test_allocates_two_parallel_tasks(self):
        op = track.Operation("index", track.OperationType.Index)
        task = track.Task(op)

        allocator = driver.Allocator([track.Parallel([task, task])])

        self.assertEqual(2, allocator.clients)
        self.assertEqual(3, len(allocator.allocations[0]))
        self.assertEqual(3, len(allocator.allocations[1]))
        self.assertEqual(2, len(allocator.join_points))
        self.assertEqual([{op}], allocator.operations_per_joinpoint)

    def test_allocates_mixed_tasks(self):
        op1 = track.Operation("index", track.OperationType.Index)
        op2 = track.Operation("stats", track.OperationType.IndicesStats)
        op3 = track.Operation("search", track.OperationType.Search)

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
        op1 = track.Operation("index-a", track.OperationType.Index)
        op2 = track.Operation("index-b", track.OperationType.Index)
        op3 = track.Operation("index-c", track.OperationType.Index)
        op4 = track.Operation("index-d", track.OperationType.Index)
        op5 = track.Operation("index-e", track.OperationType.Index)

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
        op1 = track.Operation("index-a", track.OperationType.Index)
        op2 = track.Operation("index-b", track.OperationType.Index)
        op3 = track.Operation("index-c", track.OperationType.Index)

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
        op = track.Operation("index", track.OperationType.Index)

        samples = [
            driver.Sample(0, 1470838595, 21, op, metrics.SampleType.Normal, -1, -1, 5000, 1),
            driver.Sample(0, 1470838596, 22, op, metrics.SampleType.Normal, -1, -1, 5000, 2),
            driver.Sample(0, 1470838597, 23, op, metrics.SampleType.Normal, -1, -1, 5000, 3),
            driver.Sample(0, 1470838598, 24, op, metrics.SampleType.Normal, -1, -1, 5000, 4),
            driver.Sample(0, 1470838599, 25, op, metrics.SampleType.Normal, -1, -1, 5000, 5),
            driver.Sample(0, 1470838600, 26, op, metrics.SampleType.Normal, -1, -1, 5000, 6),
            driver.Sample(1, 1470838598.5, 24.5, op, metrics.SampleType.Normal, -1, -1, 5000, 4.5),
            driver.Sample(1, 1470838599.5, 25.5, op, metrics.SampleType.Normal, -1, -1, 5000, 5.5),
            driver.Sample(1, 1470838600.5, 26.5, op, metrics.SampleType.Normal, -1, -1, 5000, 6.5)
        ]

        aggregated = driver.calculate_global_throughput(samples, bucket_interval_secs=1)

        self.assertIn(op, aggregated)
        self.assertEqual(1, len(aggregated))

        throughput = aggregated[op]
        print(throughput)
        self.assertEqual(7, len(throughput))
        self.assertEqual((1470838595, 21, metrics.SampleType.Normal, 5000), throughput[0])
        self.assertEqual((1470838596, 22, metrics.SampleType.Normal, 5000), throughput[1])
        self.assertEqual((1470838597, 23, metrics.SampleType.Normal, 5000), throughput[2])
        self.assertEqual((1470838598, 24, metrics.SampleType.Normal, 5000), throughput[3])
        self.assertEqual((1470838599, 25, metrics.SampleType.Normal, 10000), throughput[4])
        self.assertEqual((1470838600, 26, metrics.SampleType.Normal, 10000), throughput[5])
        self.assertEqual((1470838600.5, 26.5, metrics.SampleType.Normal, 10000), throughput[6])


class SchedulerTests(ScheduleTestCase):
    def test_search_task_one_client(self):
        task = track.Task(track.Operation("search", track.OperationType.Search),
                          warmup_iterations=3, iterations=5, clients=1, target_throughput=10)
        schedule = driver.schedule_for(task, 0, [])

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 0, 3, None, {}),
            (0.1, metrics.SampleType.Warmup, 1, 3, None, {}),
            (0.2, metrics.SampleType.Warmup, 2, 3, None, {}),
            (0, metrics.SampleType.Normal, 0, 5, None, {}),
            (0.1, metrics.SampleType.Normal, 1, 5, None, {}),
            (0.2, metrics.SampleType.Normal, 2, 5, None, {}),
            (0.3, metrics.SampleType.Normal, 3, 5, None, {}),
            (0.4, metrics.SampleType.Normal, 4, 5, None, {}),
        ]
        self.assert_schedule(expected_schedule, schedule)

    def test_search_task_two_clients(self):
        task = track.Task(track.Operation("search", track.OperationType.Search),
                          warmup_iterations=2, iterations=10, clients=2, target_throughput=10)
        schedule = driver.schedule_for(task, 0, [])

        expected_schedule = [
            (0, metrics.SampleType.Warmup, 0, 1, None, {}),
            (0, metrics.SampleType.Normal, 0, 5, None, {}),
            (0.2, metrics.SampleType.Normal, 1, 5, None, {}),
            (0.4, metrics.SampleType.Normal, 2, 5, None, {}),
            (0.6, metrics.SampleType.Normal, 3, 5, None, {}),
            (0.8, metrics.SampleType.Normal, 4, 5, None, {}),
        ]
        self.assert_schedule(expected_schedule, schedule)


class StringAsFileSource:
    @staticmethod
    def open(contents, mode):
        return StringAsFileSource(contents, mode)

    def __init__(self, contents, mode):
        self.contents = contents
        self.current_index = 0

    def readline(self):
        if self.current_index >= len(self.contents):
            return ""
        line = self.contents[self.current_index]
        self.current_index += 1
        return line

    def close(self):
        self.contents = None


class IndexDataReaderTests(TestCase):
    def test_read_bulk_larger_than_number_of_docs(self):
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}'
        ]
        bulk_size = 50

        reader = driver.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
                                        bulk_size=bulk_size, file_source=StringAsFileSource)
        with reader:
            for bulk in reader:
                self.assertEqual(len(data) * 2, len(bulk))

    def test_read_bulk_with_offset(self):
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}'
        ]
        bulk_size = 50

        reader = driver.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
                                        bulk_size=bulk_size, offset=3, file_source=StringAsFileSource)
        with reader:
            for bulk in reader:
                self.assertEqual((len(data) - 3) * 2, len(bulk))

    def test_read_bulk_smaller_than_number_of_docs(self):
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}',
            '{"key": "value6"}',
            '{"key": "value7"}',
        ]
        bulk_size = 3

        reader = driver.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
                                        bulk_size=bulk_size, file_source=StringAsFileSource)

        # always double the amount as one line contains the data and one line contains the index command
        expected_bulk_lengths = [6, 6, 2]
        with reader:
            bulk_index = 0
            for bulk in reader:
                self.assertEqual(expected_bulk_lengths[bulk_index], len(bulk))
                bulk_index += 1


class TestIndexReader:
    def __init__(self, data):
        self.enter_count = 0
        self.exit_count = 0
        self.data = data

    def __enter__(self):
        self.enter_count += 1
        return self

    def __iter__(self):
        return iter(self.data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit_count += 1
        return False


class TestIndex:
    def __init__(self, name, types):
        self.name = name
        self.types = types


class TestType:
    def __init__(self, number_of_documents):
        self.number_of_documents = number_of_documents


class InvocationGeneratorTests(ScheduleTestCase):
    def test_iterator_chaining_respects_context_manager(self):
        i0 = TestIndexReader([1, 2, 3])
        i1 = TestIndexReader([4, 5, 6])

        self.assertEqual([1, 2, 3, 4, 5, 6], list(driver.chain(i0, i1)))
        self.assertEqual(1, i0.enter_count)
        self.assertEqual(1, i0.exit_count)
        self.assertEqual(1, i1.enter_count)
        self.assertEqual(1, i1.exit_count)

    def test_iteration_count_based(self):
        invocations = driver.iteration_count_based(None, 2, 3, "runner", ["sample-param"])
        self.assert_schedule([
            (0, metrics.SampleType.Warmup, 0, 2, None, ["sample-param"]),
            (0, metrics.SampleType.Warmup, 1, 2, None, ["sample-param"]),
            (0, metrics.SampleType.Normal, 0, 3, None, ["sample-param"]),
            (0, metrics.SampleType.Normal, 1, 3, None, ["sample-param"]),
            (0, metrics.SampleType.Normal, 2, 3, None, ["sample-param"])
        ], list(invocations))

    def test_calculate_bounds(self):
        self.assertEqual((0, 1000), driver.bounds(1000, 0, 1))

        self.assertEqual((0, 500),   driver.bounds(1000, 0, 2))
        self.assertEqual((500, 500), driver.bounds(1000, 1, 2))

        self.assertEqual((0, 400),   driver.bounds(800, 0, 2))
        self.assertEqual((400, 400), driver.bounds(800, 1, 2))

        self.assertEqual((0, 267),   driver.bounds(800, 0, 3))
        self.assertEqual((267, 267), driver.bounds(800, 1, 3))
        self.assertEqual((534, 266), driver.bounds(800, 2, 3))

    def test_calculate_number_of_bulks(self):
        t1 = TestType(1)
        t2 = TestType(2)

        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [t1])], 1, 0, 1))
        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [t1])], 2, 0, 1))
        self.assertEqual(20, driver.number_of_bulks(
            [TestIndex("a", [t2, t2, t2, t2, t1]), TestIndex("b", [t2, t2, t2, t2, t2, t1])], 1, 0, 1))
        self.assertEqual(11, driver.number_of_bulks(
            [TestIndex("a", [t2, t2, t2, t2, t1]), TestIndex("b", [t2, t2, t2, t2, t2, t1])], 2, 0, 1))
        self.assertEqual(11, driver.number_of_bulks(
            [TestIndex("a", [t2, t2, t2, t2, t1]), TestIndex("b", [t2, t2, t2, t2, t2, t1])], 3, 0, 1))
        self.assertEqual(11, driver.number_of_bulks(
            [TestIndex("a", [t2, t2, t2, t2, t1]), TestIndex("b", [t2, t2, t2, t2, t2, t1])], 100, 0, 1))

        self.assertEqual(2, driver.number_of_bulks([TestIndex("a", [TestType(800)])], 250, 0, 3))
        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [TestType(800)])], 267, 0, 3))
        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [TestType(80)])], 267, 0, 3))
        # this looks odd at first but we are prioritizing number of clients above bulk size
        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [TestType(80)])], 267, 1, 3))
        self.assertEqual(1, driver.number_of_bulks([TestIndex("a", [TestType(80)])], 267, 2, 3))

    def test_bulk_data_based(self):
        t1 = TestType(1)
        t2 = TestType(2)

        invocations = driver.bulk_data_based(1, 0, 1, 0,
                                             [TestIndex("a", [t2, t2, t2, t2, t1]), TestIndex("b", [t2, t2, t2, t2, t2, t1])], "runner", 2,
                                             track.IndexIdConflict.NoConflicts,
                                             create_reader=lambda index, type, offset, num_docs, bulk_size, id_conflicts: TestIndexReader(
                                                 [[index.name] * type.number_of_documents])
                                             )
        self.assert_schedule([
            (0.0, metrics.SampleType.Normal, 0, 11, "runner", {"body": ["a", "a"]}),
            (1.0, metrics.SampleType.Normal, 1, 11, "runner", {"body": ["a", "a"]}),
            (2.0, metrics.SampleType.Normal, 2, 11, "runner", {"body": ["a", "a"]}),
            (3.0, metrics.SampleType.Normal, 3, 11, "runner", {"body": ["a", "a"]}),
            (4.0, metrics.SampleType.Normal, 4, 11, "runner", {"body": ["a"]}),
            (5.0, metrics.SampleType.Normal, 5, 11, "runner", {"body": ["b", "b"]}),
            (6.0, metrics.SampleType.Normal, 6, 11, "runner", {"body": ["b", "b"]}),
            (7.0, metrics.SampleType.Normal, 7, 11, "runner", {"body": ["b", "b"]}),
            (8.0, metrics.SampleType.Normal, 8, 11, "runner", {"body": ["b", "b"]}),
            (9.0, metrics.SampleType.Normal, 9, 11, "runner", {"body": ["b", "b"]}),
            (10.0, metrics.SampleType.Normal, 10, 11, "runner", {"body": ["b"]}),
        ], list(invocations))

    def test_build_conflicting_ids(self):
        self.assertIsNone(driver.build_conflicting_ids(track.IndexIdConflict.NoConflicts, 3, 0))
        self.assertEqual(["         0", "         1", "         2"],
                         driver.build_conflicting_ids(track.IndexIdConflict.SequentialConflicts, 3, 0))
        # we cannot tell anything specific about the contents...
        self.assertEqual(3, len(driver.build_conflicting_ids(track.IndexIdConflict.RandomConflicts, 3, 0)))
