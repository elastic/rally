from unittest import TestCase

from esrally.track import params


class StringAsFileSource:
    @staticmethod
    def open(contents, mode):
        return StringAsFileSource(contents, mode)

    def __init__(self, contents, mode):
        self.contents = contents
        self.current_index = 0

    def seek(self, offset):
        if offset != 0:
            raise AssertionError("StringAsFileSource does not support random seeks")

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

        reader = params.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
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

        reader = params.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
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

        reader = params.IndexDataReader(data, docs_to_index=len(data), conflicting_ids=None, index_name="test_index", type_name="test_type",
                                        bulk_size=bulk_size, file_source=StringAsFileSource)

        # always double the amount as one line contains the data and one line contains the index command
        expected_bulk_lengths = [6, 6, 2]
        with reader:
            bulk_index = 0
            for bulk in reader:
                self.assertEqual(expected_bulk_lengths[bulk_index], len(bulk))
                bulk_index += 1

    def test_read_bulk_smaller_than_number_of_docs_and_multiple_clients(self):
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

        reader = params.IndexDataReader(data, docs_to_index=5, conflicting_ids=None, index_name="test_index", type_name="test_type",
                                        bulk_size=bulk_size, file_source=StringAsFileSource)

        # always double the amount as one line contains the data and one line contains the index command
        expected_bulk_lengths = [6, 4]
        with reader:
            bulk_index = 0
            for bulk in reader:
                self.assertEqual(expected_bulk_lengths[bulk_index], len(bulk))
                bulk_index += 1


class InvocationGeneratorTests(TestCase):
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

    def idx(self, *args, **kwargs):
        return InvocationGeneratorTests.TestIndex(*args, **kwargs)

    def t(self, *args, **kwargs):
        return InvocationGeneratorTests.TestType(*args, **kwargs)

    def test_iterator_chaining_respects_context_manager(self):
        i0 = InvocationGeneratorTests.TestIndexReader([1, 2, 3])
        i1 = InvocationGeneratorTests.TestIndexReader([4, 5, 6])

        self.assertEqual([1, 2, 3, 4, 5, 6], list(params.chain(i0, i1)))
        self.assertEqual(1, i0.enter_count)
        self.assertEqual(1, i0.exit_count)
        self.assertEqual(1, i1.enter_count)
        self.assertEqual(1, i1.exit_count)

    def test_calculate_bounds(self):
        self.assertEqual((0, 1000), params.bounds(1000, 0, 1))

        self.assertEqual((0, 500), params.bounds(1000, 0, 2))
        self.assertEqual((500, 500), params.bounds(1000, 1, 2))

        self.assertEqual((0, 400), params.bounds(800, 0, 2))
        self.assertEqual((400, 400), params.bounds(800, 1, 2))

        self.assertEqual((0, 267), params.bounds(800, 0, 3))
        self.assertEqual((267, 267), params.bounds(800, 1, 3))
        self.assertEqual((534, 266), params.bounds(800, 2, 3))

        self.assertEqual((0, 250), params.bounds(2000, 0, 8))
        self.assertEqual((250, 250), params.bounds(2000, 1, 8))
        self.assertEqual((500, 250), params.bounds(2000, 2, 8))
        self.assertEqual((750, 250), params.bounds(2000, 3, 8))
        self.assertEqual((1000, 250), params.bounds(2000, 4, 8))
        self.assertEqual((1250, 250), params.bounds(2000, 5, 8))
        self.assertEqual((1500, 250), params.bounds(2000, 6, 8))
        self.assertEqual((1750, 250), params.bounds(2000, 7, 8))

    def test_calculate_number_of_bulks(self):
        t1 = self.t(1)
        t2 = self.t(2)

        self.assertEqual(1, self.number_of_bulks([self.idx("a", [t1])], 0, 1, 1))
        self.assertEqual(1, self.number_of_bulks([self.idx("a", [t1])], 0, 1, 2))
        self.assertEqual(20, self.number_of_bulks(
            [self.idx("a", [t2, t2, t2, t2, t1]),
             self.idx("b", [t2, t2, t2, t2, t2, t1])], 0, 1, 1))
        self.assertEqual(11, self.number_of_bulks(
            [self.idx("a", [t2, t2, t2, t2, t1]),
             self.idx("b", [t2, t2, t2, t2, t2, t1])], 0, 1, 2))
        self.assertEqual(11, self.number_of_bulks(
            [self.idx("a", [t2, t2, t2, t2, t1]),
             self.idx("b", [t2, t2, t2, t2, t2, t1])], 0, 1, 3))
        self.assertEqual(11, self.number_of_bulks(
            [self.idx("a", [t2, t2, t2, t2, t1]),
             self.idx("b", [t2, t2, t2, t2, t2, t1])], 0, 1, 100))

        self.assertEqual(2, self.number_of_bulks([self.idx("a", [self.t(800)])], 0, 3, 250))
        self.assertEqual(1, self.number_of_bulks([self.idx("a", [self.t(800)])], 0, 3, 267))
        self.assertEqual(1, self.number_of_bulks([self.idx("a", [self.t(80)])], 0, 3, 267))
        # this looks odd at first but we are prioritizing number of clients above bulk size
        self.assertEqual(1, self.number_of_bulks([self.idx("a", [self.t(80)])], 1, 3, 267))
        self.assertEqual(1, self.number_of_bulks([self.idx("a", [self.t(80)])], 2, 3, 267))

    def number_of_bulks(self, indices, partition_index, total_partitions, bulk_size):
        return params.PartitionBulkIndexParamSource(indices, partition_index, total_partitions, bulk_size).number_of_bulks()

    def test_build_conflicting_ids(self):
        self.assertIsNone(params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 3, 0))
        self.assertEqual(["         0", "         1", "         2"],
                         params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 3, 0))
        # we cannot tell anything specific about the contents...
        self.assertEqual(3, len(params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0)))


class ParamsRegistrationTests(TestCase):
    @staticmethod
    def param_source_function(indices, params):
        return {
            "key": params["parameter"]
        }

    class ParamSourceClass:
        def __init__(self, indices=None, params=None):
            self._indices = indices
            self._params = params

        def partition(self, partition_index, total_partitions):
            return self

        def size(self):
            return 1

        def params(self):
            return {
                "class-key": self._params["parameter"]
            }

    def test_can_register_function_as_param_source(self):
        source_name = "params-test-function-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.param_source_function)
        source = params.param_source_for_name(source_name, None, {"parameter": 42})
        self.assertEqual({"key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)

    def test_can_register_class_as_param_source(self):
        source_name = "params-test-class-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.ParamSourceClass)
        source = params.param_source_for_name(source_name, None, {"parameter": 42})
        self.assertEqual({"class-key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)
