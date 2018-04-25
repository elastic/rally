import random
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
        self.assertEqual(("index", '{"index": {"_index": "test_index", "_type": "test_type"}}'),
                         next(params.GenerateActionMetaData("test_index", "test_type")))

    def test_generate_action_meta_data_with_id_conflicts(self):
        def idx(id):
            return "index", '{"index": {"_index": "test_index", "_type": "test_type", "_id": "%s"}}' % id

        def conflict(action, id):
            return action, '{"%s": {"_index": "test_index", "_type": "test_type", "_id": "%s"}}' % (action, id)

        pseudo_random_conflicts = iter([
            # if this value is <= our chosen threshold of 0.25 (see conflict_probability) we produce a conflict.
            0.2,
            0.25,
            0.2,
            # no conflict
            0.3,
            # conflict again
            0.0
        ])

        chosen_index_of_conflicting_ids = iter([
            # the "random" index of the id in the array `conflicting_ids` that will produce a conflict
            1,
            3,
            2,
            0])

        conflict_action = random.choice(["index", "update"])

        generator = params.GenerateActionMetaData("test_index", "test_type",
                                                  conflicting_ids=[100, 200, 300, 400],
                                                  conflict_probability=25,
                                                  on_conflict=conflict_action,
                                                  rand=lambda: next(pseudo_random_conflicts),
                                                  randint=lambda x, y: next(chosen_index_of_conflicting_ids))

        # first one is always *not* drawn from a random index
        self.assertEqual(idx("100"), next(generator))
        # now we start using random ids, i.e. look in the first line of the pseudo-random sequence
        self.assertEqual(conflict(conflict_action, "200"), next(generator))
        self.assertEqual(conflict(conflict_action, "400"), next(generator))
        self.assertEqual(conflict(conflict_action, "300"), next(generator))
        # no conflict -> we draw the next sequential one, which is 200
        self.assertEqual(idx("200"), next(generator))
        # and we're back to random
        self.assertEqual(conflict(conflict_action, "100"), next(generator))

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
        self.assertEqual([("source", doc) for doc in data], list(generator))
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
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

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
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

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
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

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
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

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

    def test_read_bulk_with_id_conflicts(self):
        pseudo_random_conflicts = iter([
            # if this value is <= our chosen threshold of 0.25 (see conflict_probability) we produce a conflict.
            0.2,
            0.25,
            0.2,
            # no conflict
            0.3
        ])

        chosen_index_of_conflicting_ids = iter([
            # the "random" index of the id in the array `conflicting_ids` that will produce a conflict
            1,
            3,
            2])

        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
            '{"key": "value4"}',
            '{"key": "value5"}'
        ]
        bulk_size = 2

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type",
                                                   conflicting_ids=[100, 200, 300, 400],
                                                   conflict_probability=25,
                                                   on_conflict="update",
                                                   rand=lambda: next(pseudo_random_conflicts),
                                                   randint=lambda x, y: next(chosen_index_of_conflicting_ids))

        reader = params.IndexDataReader(data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, action_metadata=am_handler,
                                        index_name="test_index", type_name="test_type")

        # consume all bulks
        bulks = []
        with reader:
            for index, type, batch in reader:
                for bulk_size, bulk in batch:
                    bulks.append(bulk)

        self.assertEqual([
            [
                '{"index": {"_index": "test_index", "_type": "test_type", "_id": "100"}}',
                '{"key": "value1"}',
                '{"update": {"_index": "test_index", "_type": "test_type", "_id": "200"}}',
                '{"doc":{"key": "value2"}}'
            ],
            [
                '{"update": {"_index": "test_index", "_type": "test_type", "_id": "400"}}',
                '{"doc":{"key": "value3"}}',
                '{"update": {"_index": "test_index", "_type": "test_type", "_id": "300"}}',
                '{"doc":{"key": "value4"}}'
            ],
            [
                '{"index": {"_index": "test_index", "_type": "test_type", "_id": "200"}}',
                '{"key": "value5"}'
            ]

        ], bulks)

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

        self.assertEqual((  0, 500, 500), params.bounds(1000, 0, 2, includes_action_and_meta_data=False))
        self.assertEqual((500, 500, 500), params.bounds(1000, 1, 2, includes_action_and_meta_data=False))

        self.assertEqual((   0, 200, 400), params.bounds(800, 0, 4, includes_action_and_meta_data=True))
        self.assertEqual(( 400, 200, 400), params.bounds(800, 1, 4, includes_action_and_meta_data=True))
        self.assertEqual(( 800, 200, 400), params.bounds(800, 2, 4, includes_action_and_meta_data=True))
        self.assertEqual((1200, 200, 400), params.bounds(800, 3, 4, includes_action_and_meta_data=True))

        self.assertEqual((   0, 250, 250), params.bounds(2000, 0, 8, includes_action_and_meta_data=False))
        self.assertEqual(( 250, 250, 250), params.bounds(2000, 1, 8, includes_action_and_meta_data=False))
        self.assertEqual(( 500, 250, 250), params.bounds(2000, 2, 8, includes_action_and_meta_data=False))
        self.assertEqual(( 750, 250, 250), params.bounds(2000, 3, 8, includes_action_and_meta_data=False))
        self.assertEqual((1000, 250, 250), params.bounds(2000, 4, 8, includes_action_and_meta_data=False))
        self.assertEqual((1250, 250, 250), params.bounds(2000, 5, 8, includes_action_and_meta_data=False))
        self.assertEqual((1500, 250, 250), params.bounds(2000, 6, 8, includes_action_and_meta_data=False))
        self.assertEqual((1750, 250, 250), params.bounds(2000, 7, 8, includes_action_and_meta_data=False))

    def test_calculate_non_multiple_bounds(self):
        # in this test case, each client would need to read 1333.3333 lines. Instead we let most clients read 1333
        # lines and every third client, one line more (1334).
        self.assertEqual((    0, 1333, 1333), params.bounds(16000,  0, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 1333, 1334, 1334), params.bounds(16000,  1, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 2667, 1333, 1333), params.bounds(16000,  2, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 4000, 1333, 1333), params.bounds(16000,  3, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 5333, 1334, 1334), params.bounds(16000,  4, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 6667, 1333, 1333), params.bounds(16000,  5, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 8000, 1333, 1333), params.bounds(16000,  6, 12, includes_action_and_meta_data=False))
        self.assertEqual(( 9333, 1334, 1334), params.bounds(16000,  7, 12, includes_action_and_meta_data=False))
        self.assertEqual((10667, 1333, 1333), params.bounds(16000,  8, 12, includes_action_and_meta_data=False))
        self.assertEqual((12000, 1333, 1333), params.bounds(16000,  9, 12, includes_action_and_meta_data=False))
        self.assertEqual((13333, 1334, 1334), params.bounds(16000, 10, 12, includes_action_and_meta_data=False))
        self.assertEqual((14667, 1333, 1333), params.bounds(16000, 11, 12, includes_action_and_meta_data=False))

        # With 3500 docs and 6 clients, every client needs to read 583.33 docs. We have two lines per doc, which makes it
        # 2 * 583.333 docs = 1166.6666 lines per client. We let them read 1166 and 1168 lines respectively (583 and 584 docs).
        self.assertEqual((   0, 583, 1166), params.bounds(3500, 0, 6, includes_action_and_meta_data=True))
        self.assertEqual((1166, 584, 1168), params.bounds(3500, 1, 6, includes_action_and_meta_data=True))
        self.assertEqual((2334, 583, 1166), params.bounds(3500, 2, 6, includes_action_and_meta_data=True))
        self.assertEqual((3500, 583, 1166), params.bounds(3500, 3, 6, includes_action_and_meta_data=True))
        self.assertEqual((4666, 584, 1168), params.bounds(3500, 4, 6, includes_action_and_meta_data=True))
        self.assertEqual((5834, 583, 1166), params.bounds(3500, 5, 6, includes_action_and_meta_data=True))

    def test_calculate_number_of_bulks(self):
        docs1 = self.docs(1)
        docs2 = self.docs(2)

        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [docs1])], 0, 1, 1))
        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [docs1])], 0, 1, 2))
        self.assertEqual(20, self.number_of_bulks(
            [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]),
             self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])], 0, 1, 1))
        self.assertEqual(11, self.number_of_bulks(
            [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]),
             self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])], 0, 1, 2))
        self.assertEqual(11, self.number_of_bulks(
            [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]),
             self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])], 0, 1, 3))
        self.assertEqual(11, self.number_of_bulks(
            [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]),
             self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])], 0, 1, 100))

        self.assertEqual(2, self.number_of_bulks([self.corpus("a", [self.docs(800)])], 0, 3, 250))
        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [self.docs(800)])], 0, 3, 267))
        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [self.docs(80)])], 0, 3, 267))
        # this looks odd at first but we are prioritizing number of clients above bulk size
        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [self.docs(80)])], 1, 3, 267))
        self.assertEqual(1, self.number_of_bulks([self.corpus("a", [self.docs(80)])], 2, 3, 267))

    @staticmethod
    def corpus(name, docs):
        return track.DocumentCorpus(name, documents=docs)

    @staticmethod
    def docs(num_docs):
        return track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=num_docs)

    @staticmethod
    def number_of_bulks(corpora, partition_index, total_partitions, bulk_size):
        return params.number_of_bulks(corpora, partition_index, total_partitions, bulk_size)

    def test_build_conflicting_ids(self):
        self.assertIsNone(params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 3, 0))
        self.assertEqual(["         0", "         1", "         2"],
                         params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 3, 0))
        # we cannot tell anything specific about the contents...
        self.assertEqual(3, len(params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0)))


class BulkIndexParamSourceTests(TestCase):
    def test_create_without_params(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={})

        self.assertEqual("Mandatory parameter 'bulk-size' is missing", ctx.exception.args[0])

    def test_create_with_non_numeric_bulk_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": "Three"
            })

        self.assertEqual("'bulk-size' must be numeric", ctx.exception.args[0])

    def test_create_with_negative_bulk_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": -5
            })

        self.assertEqual("'bulk-size' must be positive but was -5", ctx.exception.args[0])

    def test_create_with_fraction_smaller_batch_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": 5,
                "batch-size": 3
            })

        self.assertEqual("'batch-size' must be greater than or equal to 'bulk-size'", ctx.exception.args[0])

    def test_create_with_fraction_larger_batch_size(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": 5,
                "batch-size": 8
            })

        self.assertEqual("'batch-size' must be a multiple of 'bulk-size'", ctx.exception.args[0])

    def test_create_with_metadata_in_source_file_but_conflicts(self):
        corpus = track.DocumentCorpus(name="default", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                            document_archive="docs.json.bz2",
                            document_file="docs.json",
                            number_of_documents=10,
                            includes_action_and_meta_data=True)
        ])

        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test", corpora=[corpus]), params={
                "conflicts": "random"
            })

        self.assertEqual("Cannot generate id conflicts [random] as [docs.json.bz2] in document corpus [default] already contains "
                         "an action and meta-data line.", ctx.exception.args[0])

    def test_create_with_unknown_id_conflicts(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "conflicts": "crazy"
            })

        self.assertEqual("Unknown 'conflicts' setting [crazy]", ctx.exception.args[0])

    def test_create_with_unknown_on_conflict_setting(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "conflicts": "sequential",
                "on-conflict": "delete"
            })

        self.assertEqual("Unknown 'on-conflict' setting [delete]", ctx.exception.args[0])

    def test_create_with_ingest_percentage_too_low(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": 5000,
                "ingest-percentage": 0.0
            })

        self.assertEqual("'ingest-percentage' must be in the range (0.0, 100.0] but was 0.0", ctx.exception.args[0])

    def test_create_with_ingest_percentage_too_high(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": 5000,
                "ingest-percentage": 100.1
            })

        self.assertEqual("'ingest-percentage' must be in the range (0.0, 100.0] but was 100.1", ctx.exception.args[0])

    def test_create_with_ingest_percentage_not_numeric(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.BulkIndexParamSource(track=track.Track(name="unit-test"), params={
                "bulk-size": 5000,
                "ingest-percentage": "100 percent"
            })

        self.assertEqual("'ingest-percentage' must be numeric", ctx.exception.args[0])

    def test_create_valid_param_source(self):
        self.assertIsNotNone(params.BulkIndexParamSource(track.Track(name="unit-test"), params={
            "conflicts": "random",
            "bulk-size": 5000,
            "batch-size": 20000,
            "ingest-percentage": 20.5,
            "pipeline": "test-pipeline"
        }))

    def test_passes_all_corpora_by_default(self):
        corpora = [
            track.DocumentCorpus(name="default", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=10,
                                target_index="test-idx",
                                target_type="test-type"
                                )
            ]),
            track.DocumentCorpus(name="special", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=100,
                                target_index="test-idx2",
                                target_type="type"
                                )
            ]),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline"
            })

        partition = source.partition(0, 1)
        self.assertEqual(partition.corpora, corpora)

    def test_filters_corpora(self):
        corpora = [
            track.DocumentCorpus(name="default", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=10,
                                target_index="test-idx",
                                target_type="test-type"
                                )
            ]),
            track.DocumentCorpus(name="special", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=100,
                                target_index="test-idx2",
                                target_type="type"
                                )
            ]),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "corpora": ["special"],
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline"
            })

        partition = source.partition(0, 1)
        self.assertEqual(partition.corpora, [corpora[1]])

    def test_raises_exception_if_no_corpus_matches(self):
        corpus = track.DocumentCorpus(name="default", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                            number_of_documents=10,
                            target_index="test-idx",
                            target_type="test-type"
                            )])

        with self.assertRaises(exceptions.RallyAssertionError) as ctx:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "corpora": "does_not_exist",
                    "conflicts": "random",
                    "bulk-size": 5000,
                    "batch-size": 20000,
                    "pipeline": "test-pipeline"
                })

        self.assertEqual("The provided corpus ['does_not_exist'] does not match any of the corpora ['default'].", ctx.exception.args[0])

    def test_ingests_all_documents_by_default(self):
        corpora = [
            track.DocumentCorpus(name="default", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=300000,
                                target_index="test-idx",
                                target_type="test-type"
                                )
            ]),
            track.DocumentCorpus(name="special", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=700000,
                                target_index="test-idx2",
                                target_type="type"
                                )
            ]),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "bulk-size": 10000
            })

        partition = source.partition(0, 1)
        # # no ingest-percentage specified, should issue all one hundred bulk requests
        self.assertEqual(100, partition.size())

    def test_restricts_number_of_bulks_if_required(self):
        corpora = [
            track.DocumentCorpus(name="default", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=300000,
                                target_index="test-idx",
                                target_type="test-type"
                                )
            ]),
            track.DocumentCorpus(name="special", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=700000,
                                target_index="test-idx2",
                                target_type="type"
                                )
            ]),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "bulk-size": 10000,
                "ingest-percentage": 2.5
            })

        partition = source.partition(0, 1)
        # should issue three bulks of size 10.000
        self.assertEqual(3, partition.size())


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
        def inner_create_test_reader(docs, *args):
            return BulkDataGeneratorTests.TestBulkReader(docs.target_index, docs.target_type, batches)

        return inner_create_test_reader

    def test_generate_two_bulks(self):
        corpus = track.DocumentCorpus(name="default", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                            number_of_documents=10,
                            target_index="test-idx",
                            target_type="test-type"
                            )
        ])

        bulks = params.bulk_data_based(num_clients=1, client_index=0, corpora=[corpus],
                                       batch_size=5, bulk_size=5,
                                       id_conflicts=params.IndexIdConflict.NoConflicts, conflict_probability=None, on_conflict=None,
                                       pipeline=None,
                                       original_params={
                                           "my-custom-parameter": "foo",
                                           "my-custom-parameter-2": True
                                       }, create_reader=BulkDataGeneratorTests.
                                       create_test_reader([["1", "2", "3", "4", "5"], ["6", "7", "8"]]))
        all_bulks = list(bulks)
        self.assertEqual(2, len(all_bulks))
        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["1", "2", "3", "4", "5"],
            "bulk-id": "0-1",
            "bulk-size": 5,
            "index": "test-idx",
            "type": "test-type",
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[0])

        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["6", "7", "8"],
            "bulk-id": "0-2",
            "bulk-size": 3,
            "index": "test-idx",
            "type": "test-type",
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[1])

    def test_generate_bulks_from_multiple_corpora(self):
        corpora = [
            track.DocumentCorpus(name="default", documents=[
                        track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                        number_of_documents=5,
                                        target_index="logs-2018-01",
                                        target_type="docs"
                                        ),
                        track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                        number_of_documents=5,
                                        target_index="logs-2018-02",
                                        target_type="docs"
                                        ),

                    ]),
            track.DocumentCorpus(name="special", documents=[
                track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                                number_of_documents=5,
                                target_index="logs-2017-01",
                                target_type="docs"
                                )
            ])

            ]

        bulks = params.bulk_data_based(num_clients=1, client_index=0, corpora=corpora,
                                       batch_size=5, bulk_size=5,
                                       id_conflicts=params.IndexIdConflict.NoConflicts, conflict_probability=None, on_conflict=None,
                                       pipeline=None,
                                       original_params={
                                           "my-custom-parameter": "foo",
                                           "my-custom-parameter-2": True
                                       }, create_reader=BulkDataGeneratorTests.
                                       create_test_reader([["1", "2", "3", "4", "5"]]))
        all_bulks = list(bulks)
        self.assertEqual(3, len(all_bulks))
        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["1", "2", "3", "4", "5"],
            "bulk-id": "0-1",
            "bulk-size": 5,
            "index": "logs-2018-01",
            "type": "docs",
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[0])

        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["1", "2", "3", "4", "5"],
            "bulk-id": "0-2",
            "bulk-size": 5,
            "index": "logs-2018-02",
            "type": "docs",
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[1])

        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["1", "2", "3", "4", "5"],
            "bulk-id": "0-3",
            "bulk-size": 5,
            "index": "logs-2017-01",
            "type": "docs",
            "my-custom-parameter": "foo",
            "my-custom-parameter-2": True
        }, all_bulks[2])

    def test_internal_params_take_precedence(self):
        corpus = track.DocumentCorpus(name="default", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK,
                            number_of_documents=3,
                            target_index="test-idx",
                            target_type="test-type"
                            )
        ])

        bulks = params.bulk_data_based(num_clients=1, client_index=0, corpora=[corpus], batch_size=3, bulk_size=3,
                                       id_conflicts=params.IndexIdConflict.NoConflicts, conflict_probability=None, on_conflict=None,
                                       pipeline=None,
                                       original_params={
                                           "body": "foo",
                                           "custom-param": "bar"
                                       }, create_reader=BulkDataGeneratorTests.
                                       create_test_reader([["1", "2", "3"]]))
        all_bulks = list(bulks)
        self.assertEqual(1, len(all_bulks))
        # body must not contain 'foo'!
        self.assertEqual({
            "action-metadata-present": True,
            "action_metadata_present": True,
            "body": ["1", "2", "3"],
            "bulk-id": "0-1",
            "bulk-size": 3,
            "index": "test-idx",
            "type": "test-type",
            "custom-param": "bar"
        }, all_bulks[0])


class ParamsRegistrationTests(TestCase):
    @staticmethod
    def param_source_legacy_function(indices, params):
        return {
            "key": params["parameter"]
        }

    @staticmethod
    def param_source_function(track, params, **kwargs):
        return {
            "key": params["parameter"]
        }

    class ParamSourceLegacyClass:
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

    class ParamSourceClass:
        def __init__(self, track=None, params=None, **kwargs):
            self._track = track
            self._params = params

        def partition(self, partition_index, total_partitions):
            return self

        def size(self):
            return 1

        def params(self):
            return {
                "class-key": self._params["parameter"]
            }

    def test_can_register_legacy_function_as_param_source(self):
        source_name = "legacy-params-test-function-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.param_source_legacy_function)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        self.assertEqual({"key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)

    def test_can_register_function_as_param_source(self):
        source_name = "params-test-function-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.param_source_function)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        self.assertEqual({"key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)

    def test_can_register_legacy_class_as_param_source(self):
        source_name = "legacy-params-test-class-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.ParamSourceLegacyClass)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        self.assertEqual({"class-key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)

    def test_can_register_class_as_param_source(self):
        source_name = "params-test-class-param-source"

        params.register_param_source_for_name(source_name, ParamsRegistrationTests.ParamSourceClass)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        self.assertEqual({"class-key": 42}, source.params())

        params._unregister_param_source_for_name(source_name)


class CreateIndexParamSourceTests(TestCase):
    def test_create_index_inline_with_body(self):
        source = params.CreateIndexParamSource(track.Track(name="unit-test"), params={
            "index": "test",
            "body": {
                "settings": {
                    "index.number_of_replicas": 0
                },
                "mappings": {
                    "doc": {
                        "properties": {
                            "name": {
                                "type": "keyword",
                            }
                        }
                    }
                }
            }
        })

        p = source.params()
        self.assertEqual(1, len(p["indices"]))
        index, body = p["indices"][0]
        self.assertEqual("test", index)
        self.assertTrue(len(body) > 0)
        self.assertEqual({}, p["request-params"])

    def test_create_index_inline_without_body(self):
        source = params.CreateIndexParamSource(track.Track(name="unit-test"), params={
            "index": "test",
            "request-params": {
                "wait_for_active_shards": True
            }
        })

        p = source.params()
        self.assertEqual(1, len(p["indices"]))
        index, body = p["indices"][0]
        self.assertEqual("test", index)
        self.assertIsNone(body)
        self.assertDictEqual({
            "wait_for_active_shards": True
        }, p["request-params"])

    def test_create_index_from_track_with_settings(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(name="index2", types=["type1"], body={
            "settings": {
                "index.number_of_replicas": 0,
                "index.number_of_shards": 3
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            }
        })

        source = params.CreateIndexParamSource(track.Track(name="unit-test", indices=[index1, index2]), params={
            "settings": {
                "index.number_of_replicas": 1
            }
        })

        p = source.params()
        self.assertEqual(2, len(p["indices"]))

        index, body = p["indices"][0]
        self.assertEqual("index1", index)
        # index did not specify any body
        self.assertDictEqual({
            "settings": {
                "index.number_of_replicas": 1
            }
        }, body)

        index, body = p["indices"][1]
        self.assertEqual("index2", index)
        # index specified a body + we need to merge settings
        self.assertDictEqual({
            "settings": {
                # we have properly merged (overridden) an existing setting
                "index.number_of_replicas": 1,
                # and we have preserved one that was specified in the original index body
                "index.number_of_shards": 3
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            }
        }, body)

    def test_create_index_from_track_without_settings(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(name="index2", types=["type1"], body={
            "settings": {
                "index.number_of_replicas": 0,
                "index.number_of_shards": 3
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            }
        })

        source = params.CreateIndexParamSource(track.Track(name="unit-test", indices=[index1, index2]), params={})

        p = source.params()
        self.assertEqual(2, len(p["indices"]))

        index, body = p["indices"][0]
        self.assertEqual("index1", index)
        # index did not specify any body
        self.assertDictEqual({}, body)

        index, body = p["indices"][1]
        self.assertEqual("index2", index)
        # index specified a body
        self.assertDictEqual({
            "settings": {
                "index.number_of_replicas": 0,
                "index.number_of_shards": 3
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            }
        }, body)

    def test_filter_index(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(name="index2", types=["type1"])
        index3 = track.Index(name="index3", types=["type1"])

        source = params.CreateIndexParamSource(track.Track(name="unit-test", indices=[index1, index2, index3]), params={
            "index": "index2"
        })

        p = source.params()
        self.assertEqual(1, len(p["indices"]))

        index, body = p["indices"][0]
        self.assertEqual("index2", index)


class DeleteIndexParamSourceTests(TestCase):
    def test_delete_index_from_track(self):
        source = params.DeleteIndexParamSource(track.Track(name="unit-test", indices=[
            track.Index(name="index1"),
            track.Index(name="index2"),
            track.Index(name="index3")
        ]), params={})

        p = source.params()

        self.assertEqual(["index1", "index2", "index3"], p["indices"])
        self.assertDictEqual({}, p["request-params"])
        self.assertTrue(p["only-if-exists"])

    def test_filter_index_from_track(self):
        source = params.DeleteIndexParamSource(track.Track(name="unit-test", indices=[
            track.Index(name="index1"),
            track.Index(name="index2"),
            track.Index(name="index3")
        ]), params={"index": "index2", "only-if-exists": False, "request-params": {"allow_no_indices": True}})

        p = source.params()

        self.assertEqual(["index2"], p["indices"])
        self.assertDictEqual({"allow_no_indices": True}, p["request-params"])
        self.assertFalse(p["only-if-exists"])

    def test_delete_index_by_name(self):
        source = params.DeleteIndexParamSource(track.Track(name="unit-test"), params={"index": "index2"})

        p = source.params()

        self.assertEqual(["index2"], p["indices"])

    def test_delete_no_index(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.DeleteIndexParamSource(track.Track(name="unit-test"), params={})
        self.assertEqual("delete-index operation targets no index", ctx.exception.args[0])


class CreateIndexTemplateParamSourceTests(TestCase):
    def test_create_index_template_inline(self):
        source = params.CreateIndexTemplateParamSource(track=track.Track(name="unit-test"), params={
            "template": "test",
            "body": {
                "index_patterns": ["*"],
                "settings": {
                    "index.number_of_shards": 3
                },
                "mappings": {
                    "docs": {
                        "_source": {
                            "enabled": False
                        }
                    }
                }
            }
        })

        p = source.params()

        self.assertEqual(1, len(p["templates"]))
        self.assertDictEqual({}, p["request-params"])
        template, body = p["templates"][0]
        self.assertEqual("test", template)
        self.assertDictEqual({
            "index_patterns": ["*"],
            "settings": {
                "index.number_of_shards": 3
            },
            "mappings": {
                "docs": {
                    "_source": {
                        "enabled": False
                    }
                }
            }
        }, body)

    def test_create_index_template_from_track(self):
        tpl = track.IndexTemplate(name="default", pattern="*", content={
            "index_patterns": ["*"],
            "settings": {
                "index.number_of_shards": 3
            },
            "mappings": {
                "docs": {
                    "_source": {
                        "enabled": False
                    }
                }
            }
        })

        source = params.CreateIndexTemplateParamSource(track=track.Track(name="unit-test", templates=[tpl]), params={
            "settings": {
                "index.number_of_replicas": 1
            }
        })

        p = source.params()

        self.assertEqual(1, len(p["templates"]))
        self.assertDictEqual({}, p["request-params"])
        template, body = p["templates"][0]
        self.assertEqual("default", template)
        self.assertDictEqual({
            "index_patterns": ["*"],
            "settings": {
                "index.number_of_shards": 3,
                "index.number_of_replicas": 1
            },
            "mappings": {
                "docs": {
                    "_source": {
                        "enabled": False
                    }
                }
            }
        }, body)


class DeleteIndexTemplateParamSourceTests(TestCase):
    def test_delete_index_template_by_name(self):
        source = params.DeleteIndexTemplateParamSource(track.Track(name="unit-test"), params={"template": "default"})

        p = source.params()

        self.assertEqual(1, len(p["templates"]))
        self.assertEqual(("default", False, None), p["templates"][0])
        self.assertTrue(p["only-if-exists"])
        self.assertDictEqual({}, p["request-params"])

    def test_delete_index_template_by_name_and_matching_indices(self):
        source = params.DeleteIndexTemplateParamSource(track.Track(name="unit-test"),
                                                       params={
                                                           "template": "default",
                                                           "delete-matching-indices": True,
                                                           "index-pattern": "logs-*"
                                                       })

        p = source.params()

        self.assertEqual(1, len(p["templates"]))
        self.assertEqual(("default", True, "logs-*"), p["templates"][0])
        self.assertTrue(p["only-if-exists"])
        self.assertDictEqual({}, p["request-params"])

    def test_delete_index_template_by_name_and_matching_indices_missing_index_pattern(self):
        with self.assertRaises(exceptions.InvalidSyntax) as ctx:
            params.DeleteIndexTemplateParamSource(track.Track(name="unit-test"),
                                                  params={
                                                      "template": "default",
                                                      "delete-matching-indices": True
                                                  })
        self.assertEqual("The property 'index-pattern' is required for delete-index-template if 'delete-matching-indices' is true.",
                         ctx.exception.args[0])

    def test_delete_index_template_from_track(self):
        tpl1 = track.IndexTemplate(name="metrics", pattern="metrics-*", delete_matching_indices=True, content={
            "index_patterns": ["metrics-*"],
            "settings": {},
            "mappings": {}
        })
        tpl2 = track.IndexTemplate(name="logs", pattern="logs-*", delete_matching_indices=False, content={
            "index_patterns": ["logs-*"],
            "settings": {},
            "mappings": {}
        })

        source = params.DeleteIndexTemplateParamSource(track.Track(name="unit-test", templates=[tpl1, tpl2]), params={
            "request-params": {
                "master_timeout": 20
            },
            "only-if-exists": False
        })

        p = source.params()

        self.assertEqual(2, len(p["templates"]))
        self.assertEqual(("metrics", True, "metrics-*"), p["templates"][0])
        self.assertEqual(("logs", False, "logs-*"), p["templates"][1])
        self.assertFalse(p["only-if-exists"])
        self.assertDictEqual({"master_timeout": 20}, p["request-params"])


class SearchParamSourceTests(TestCase):
    def test_passes_request_parameters(self):
        index1 = track.Index(name="index1", types=["type1"])

        source = params.SearchParamSource(track=track.Track(name="unit-test", indices=[index1]), params={
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

        self.assertEqual(7, len(p))
        self.assertEqual("index1", p["index"])
        self.assertIsNone(p["type"])
        self.assertEqual({
            "_source_include": "some_field"
        }, p["request-params"])
        self.assertFalse(p["cache"])
        self.assertEqual({
            "query": {
                "match_all": {}
            }
        }, p["body"])
        # backwards-compatibility options
        self.assertFalse(p["use_request_cache"])
        self.assertEqual({
            "_source_include": "some_field"
        }, p["request_params"])

    def test_user_specified_overrides_defaults(self):
        index1 = track.Index(name="index1", types=["type1"])

        source = params.SearchParamSource(track=track.Track(name="unit-test", indices=[index1]), params={
            "index": "_all",
            "type": "type1",
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        })
        p = source.params()

        self.assertEqual(7, len(p))
        self.assertEqual("_all", p["index"])
        self.assertEqual("type1", p["type"])
        self.assertDictEqual({}, p["request-params"])
        self.assertFalse(p["cache"])
        self.assertEqual({
            "query": {
                "match_all": {}
            }
        }, p["body"])
        # backwards-compatibility options
        self.assertFalse(p["use_request_cache"])
        self.assertDictEqual({}, p["request_params"])

    def test_replaces_body_params(self):
        import copy

        search = params.SearchParamSource(track=track.Track(name="unit-test"), params={
            "index": "_all",
            "body": {
                "suggest": {
                    "song-suggest": {
                        "prefix": "nor",
                        "completion": {
                            "field": "suggest",
                            "fuzzy": {
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                }
            },
            "body-params": {
                "suggest.song-suggest.prefix": ["a", "b"]
            }
        })

        # the implementation modifies the internal dict in-place (safe because we only have one client per process) hence we need to copy.
        first = copy.deepcopy(search.params(choice=lambda d: d[0]))
        second = copy.deepcopy(search.params(choice=lambda d: d[1]))

        self.assertNotEqual(first, second)
