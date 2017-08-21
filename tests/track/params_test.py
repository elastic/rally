from unittest import TestCase

from esrally import exceptions
from esrally.utils import io
from esrally.track import params, track


class SliceTests(TestCase):
    def test_slice_with_source_larger_than_slice(self):
        source = params.Slice(io.StringAsFileSource, 2, 5)
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}',
            '{"key": "value6"}',
            '{"key": "value7"}',
            '{"key": "value8"}',
            '{"key": "value9"}',
            '{"key": "value10"}'
        ]

        source.open(data, "r")
        self.assertEqual(data[2:7], list(source))
        source.close()

    def test_slice_with_slice_larger_than_source(self):
        source = params.Slice(io.StringAsFileSource, 0, 5)
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
        ]

        source.open(data, "r")
        self.assertEqual(data, list(source))
        source.close()


class ConflictingIdsBuilderTests(TestCase):
    def test_no_id_conflicts(self):
        self.assertIsNone(params.build_conflicting_ids(None, 100, 0))
        self.assertIsNone(params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 100, 0))

    def test_sequential_conflicts(self):
        self.assertEqual(
            [
                "         0",
                "         1",
                "         2",
                "         3",
                "         4",
                "         5",
                "         6",
                "         7",
                "         8",
                "         9",
                "        10",
            ],
            params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 11, 0)
        )

        self.assertEqual(
            [
                "         5",
                "         6",
                "         7",
                "         8",
                "         9",
                "        10",
                "        11",
                "        12",
                "        13",
                "        14",
                "        15",
            ],
            params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 11, 5)
        )

    def test_random_conflicts(self):
        self.assertEqual(
            [
                "         3",
                "         3",
                "         3"
            ],
            params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0, rand=lambda x, y: y)
        )

        self.assertEqual(
            [
                "         8",
                "         8",
                "         8"
            ],
            params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 5, rand=lambda x, y: y)
        )


class ActionMetaDataTests(TestCase):
    def test_generate_action_meta_data_without_id_conflicts(self):
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type"}}',
                         next(params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=None)))

    def test_generate_action_meta_data_with_id_conflicts(self):
        pseudo_random_sequence = iter([
            # first column == 3 -> we'll draw a "random" id, second column == "random" id
            3, 1,
            3, 3,
            3, 2,
            0,
            3, 0])

        generator = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=[100, 200, 300, 400],
                                                  rand=lambda x, y: next(pseudo_random_sequence))

        # first one is always not drawn from a random index
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "100"}}', next(generator))
        # now we start using random ids, i.e. look in the first line of the pseudo-random sequence
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "200"}}', next(generator))
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "400"}}', next(generator))
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "300"}}', next(generator))
        # "random" returns 0 instead of 3 -> we draw the next sequential one, which is 200
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "200"}}', next(generator))
        # and we're back to random
        self.assertEqual('{"index": {"_index": "test_index", "_type": "test_type", "_id": "100"}}', next(generator))

    def test_source_file_action_meta_data(self):
        source = params.Slice(io.StringAsFileSource, 0, 5)
        generator = params.SourceActionMetaData(source)

        data = [
            '{"index": {"_index": "test_index", "_type": "test_type", "_id": "1"}}',
            '{"index": {"_index": "test_index", "_type": "test_type", "_id": "2"}}',
            '{"index": {"_index": "test_index", "_type": "test_type", "_id": "3"}}',
            '{"index": {"_index": "test_index", "_type": "test_type", "_id": "4"}}',
            '{"index": {"_index": "test_index", "_type": "test_type", "_id": "5"}}',
        ]

        source.open(data, "r")
        self.assertEqual(data, list(generator))
        source.close()


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

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=None)

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        expected_bulk_sizes = [len(data)]
        # lines should include meta-data
        expected_line_sizes = [len(data) * 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulk_with_offset(self):
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}'
        ]
        bulk_size = 50

        source = params.Slice(io.StringAsFileSource, 3, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=None)

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        expected_bulk_sizes = [(len(data) - 3)]
        # lines should include meta-data
        expected_line_sizes = [(len(data) - 3) * 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

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

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=None)

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        expected_bulk_sizes = [3, 3, 1]
        # lines should include meta-data
        expected_line_sizes = [6, 6, 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

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

        # only 5 documents to index for this client
        source = params.Slice(io.StringAsFileSource, 0, 5)
        am_handler = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=None)

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        expected_bulk_sizes = [3, 2]
        # lines should include meta-data
        expected_line_sizes = [6, 4]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulks_and_assume_metadata_line_in_source_file(self):
        data = [
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value1"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value2"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value3"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value4"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value5"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value6"}',
            '{"index": {"_index": "test_index", "_type": "test_type"}',
            '{"key": "value7"}'
        ]
        bulk_size = 3

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.SourceActionMetaData(source)

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        expected_bulk_sizes = [3, 3, 1]
        # lines should include meta-data
        expected_line_sizes = [6, 6, 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def assert_bulks_sized(self, reader, expected_bulk_sizes, expected_line_sizes):
        with reader:
            bulk_index = 0
            for index, type, batch in reader:
                for bulk_size, bulk in batch:
                    self.assertEqual(expected_bulk_sizes[bulk_index], bulk_size)
                    self.assertEqual(expected_line_sizes[bulk_index], len(bulk))
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
        def __init__(self, number_of_documents, includes_action_and_meta_data=False):
            self.number_of_documents = number_of_documents
            self.includes_action_and_meta_data = includes_action_and_meta_data

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
        self.assertEqual((0, 1000, 1000), params.bounds(1000, 0, 1, includes_action_and_meta_data=False))
        self.assertEqual((0, 1000, 2000), params.bounds(1000, 0, 1, includes_action_and_meta_data=True))

        self.assertEqual((0, 500, 500), params.bounds(1000, 0, 2, includes_action_and_meta_data=False))
        self.assertEqual((500, 500, 500), params.bounds(1000, 1, 2, includes_action_and_meta_data=False))

        self.assertEqual((0, 200, 400), params.bounds(800, 0, 4, includes_action_and_meta_data=True))
        self.assertEqual((400, 200, 400), params.bounds(800, 1, 4, includes_action_and_meta_data=True))
        self.assertEqual((800, 200, 400), params.bounds(800, 2, 4, includes_action_and_meta_data=True))
        self.assertEqual((1200, 200, 400), params.bounds(800, 3, 4, includes_action_and_meta_data=True))

        self.assertEqual((0, 250, 250), params.bounds(2000, 0, 8, includes_action_and_meta_data=False))
        self.assertEqual((250, 250, 250), params.bounds(2000, 1, 8, includes_action_and_meta_data=False))
        self.assertEqual((500, 250, 250), params.bounds(2000, 2, 8, includes_action_and_meta_data=False))
        self.assertEqual((750, 250, 250), params.bounds(2000, 3, 8, includes_action_and_meta_data=False))
        self.assertEqual((1000, 250, 250), params.bounds(2000, 4, 8, includes_action_and_meta_data=False))
        self.assertEqual((1250, 250, 250), params.bounds(2000, 5, 8, includes_action_and_meta_data=False))
        self.assertEqual((1500, 250, 250), params.bounds(2000, 6, 8, includes_action_and_meta_data=False))
        self.assertEqual((1750, 250, 250), params.bounds(2000, 7, 8, includes_action_and_meta_data=False))

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

    @staticmethod
    def number_of_bulks(indices, partition_index, total_partitions, bulk_size):
        return params.number_of_bulks(indices, partition_index, total_partitions, bulk_size)

    def test_build_conflicting_ids(self):
        self.assertIsNone(params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 3, 0))
        self.assertEqual(["         0", "         1", "         2"],
                         params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 3, 0))
        # we cannot tell anything specific about the contents...
        self.assertEqual(3, len(params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0)))


class BulkIndexParamSourceTests(TestCase):
    def test_create_without_params(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={})

        self.assertEqual("Mandatory parameter 'bulk-size' is missing", ctx.exception.args[0])

    def test_create_with_non_numeric_bulk_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "bulk-size": "Three"
            })

        self.assertEqual("'bulk-size' must be numeric", ctx.exception.args[0])

    def test_create_with_negative_bulk_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "bulk-size": -5
            })

        self.assertEqual("'bulk-size' must be positive but was -5", ctx.exception.args[0])

    def test_create_with_fraction_smaller_batch_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "bulk-size": 5,
                "batch-size": 3
            })

        self.assertEqual("'batch-size' must be greater than or equal to 'bulk-size'", ctx.exception.args[0])

    def test_create_with_fraction_larger_batch_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "bulk-size": 5,
                "batch-size": 8
            })

        self.assertEqual("'batch-size' must be a multiple of 'bulk-size'", ctx.exception.args[0])

    def test_create_with_metadata_in_source_file_but_conflicts(self):
        type1 = track.Type("type1", mapping={}, number_of_documents=10, includes_action_and_meta_data=True)
        index1 = track.Index(name="index1", auto_managed=True, types=[type1])

        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[index1], params={
                "conflicts": "random"
            })

        self.assertEqual("Cannot generate id conflicts [random] as type [index1] in index [type1] already contains "
                         "an action and meta-data line.", ctx.exception.args[0])

    def test_create_with_unknown_id_conflicts(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "conflicts": "crazy"
            })

        self.assertEqual("Unknown 'conflicts' setting [crazy]", ctx.exception.args[0])

    def test_create_with_unsupported_action_meta_data(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(indices=[], params={
                "action-and-meta-data": "generate",
            })

        self.assertEqual("The parameter \"action-and-meta-data\" is not supported anymore. Please specify instead the boolean "
                         "flag \"includes-action-and-meta-data\" on each type definition.", ctx.exception.args[0])

    def test_create_valid_param_source(self):
        self.assertIsNotNone(params.BulkIndexParamSource(indices=[], params={
            "conflicts": "random",
            "bulk-size": 5000,
            "batch-size": 20000,
            "pipeline": "test-pipeline"
        }))

    def test_passes_all_indices_by_default(self):
        index1 = track.Index(name="index1", auto_managed=True, types=[])
        index2 = track.Index(name="index2", auto_managed=True, types=[])

        source = params.BulkIndexParamSource(
            indices=[index1, index2],
            params={
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline"
            })

        partition = source.partition(0, 1)
        self.assertEqual(partition.indices, [index1, index2])

    def test_filters_indices(self):
        index1 = track.Index(name="index1", auto_managed=True, types=[])
        index2 = track.Index(name="index2", auto_managed=True, types=[])

        source = params.BulkIndexParamSource(
            indices=[index1, index2],
            params={
                "index": "index2",
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline"
            })

        partition = source.partition(0, 1)
        self.assertEqual(partition.indices, [index2])

    def test_raises_exception_if_no_index_matches(self):
        index1 = track.Index(name="index1", auto_managed=True, types=[])

        source = params.BulkIndexParamSource(
            indices=[index1],
            params={
                "index": "does_not_exist",
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline"
            })

        with self.assertRaises(exceptions.RallyAssertionError) as ctx:
            source.partition(0, 1)
        self.assertEqual("The provided index [does_not_exist] does not match any of the indices [index1].", ctx.exception.args[0])


class BulkDataGeneratorTests(TestCase):
    class TestBulkReader:
        def __init__(self, index_name, type_name, bulks):
            self.index_name = index_name
            self.type_name = type_name
            self.bulks = iter(bulks)

        def __enter__(self):
            return self

        def __iter__(self):
            return self

        def __next__(self):
            batch = []
            bulk = next(self.bulks)
            batch.append((len(bulk), bulk))
            return self.index_name, self.type_name, batch

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

    @classmethod
    def create_test_reader(cls, batches):
        def inner_create_test_reader(index, type, *args):
            return BulkDataGeneratorTests.TestBulkReader(index, type, batches)

        return inner_create_test_reader

    def test_generate_two_bulks(self):
        self.maxDiff = None
        type1 = track.Type("type1", mapping={}, number_of_documents=10)
        index1 = track.Index(name="index1", auto_managed=True, types=[type1])

        bulks = params.bulk_data_based(num_clients=1, client_index=0, indices=[index1],
                                       batch_size=5, bulk_size=5, id_conflicts=params.IndexIdConflict.NoConflicts, pipeline=None,
                                       original_params={
                                           "my-custom-parameter": "foo",
                                           "my-custom-parameter-2": True
                                       }, create_reader=BulkDataGeneratorTests.
                                       create_test_reader([["1", "2", "3", "4", "5"], ["6", "7", "8"]]))
        all_bulks = list(bulks)
        self.assertEqual(2, len(all_bulks))
        self.assertEqual({
            "action_metadata_present": True,
            "body": ["1", "2", "3", "4", "5"],
            "bulk-id": "0-1",
            "bulk-size": 5,
            "index": index1,
            "type": type1,
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[0])

        self.assertEqual({
            "action_metadata_present": True,
            "body": ["6", "7", "8"],
            "bulk-id": "0-2",
            "bulk-size": 3,
            "index": index1,
            "type": type1,
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[1])

    def test_internal_params_take_precedence(self):
        type1 = track.Type("type1", mapping={}, number_of_documents=3)
        index1 = track.Index(name="index1", auto_managed=True, types=[type1])

        bulks = params.bulk_data_based(num_clients=1, client_index=0, indices=[index1], batch_size=3, bulk_size=3,
                                       id_conflicts=params.IndexIdConflict.NoConflicts, pipeline=None,
                                       original_params={
                                           "body": "foo",
                                           "custom-param": "bar"
                                       }, create_reader=BulkDataGeneratorTests.
                                       create_test_reader([["1", "2", "3"]]))
        all_bulks = list(bulks)
        self.assertEqual(1, len(all_bulks))
        # body must not contain 'foo'!
        self.assertEqual({
            "action_metadata_present": True,
            "body": ["1", "2", "3"],
            "bulk-id": "0-1",
            "bulk-size": 3,
            "index": index1,
            "type": type1,
            "custom-param": "bar"
        }, all_bulks[0])


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


class SearchParamSourceTests(TestCase):
    def test_passes_request_parameters(self):
        type1 = track.Type("type1", mapping={}, number_of_documents=3)
        index1 = track.Index(name="index1", auto_managed=True, types=[type1])

        source = params.SearchParamSource(indices=[index1], params={
            "request-params": {
                "_source_include": "some_field"
            },
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        })
        p = source.params()

        self.assertEqual(5, len(p))
        self.assertEqual("index1", p["index"])
        self.assertEqual("type1", p["type"])
        self.assertEqual({
            "_source_include": "some_field"
        }, p["request_params"])
        self.assertFalse(p["use_request_cache"])
        self.assertEqual({
            "query": {
                "match_all": {}
            }
        }, p["body"])
