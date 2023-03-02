# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=protected-access

import random

import pytest

from esrally import exceptions
from esrally.track import params, track
from esrally.utils import io


class StaticBulkReader:
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


class TestSlice:
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
            '{"key": "value10"}',
        ]

        source.open(data, "r", 5)
        # lines are returned as a list so we have to wrap our data once more
        assert list(source) == [data[2:7]]
        source.close()

    def test_slice_with_slice_larger_than_source(self):
        source = params.Slice(io.StringAsFileSource, 0, 5)
        data = [
            '{"key": "value1"}',
            '{"key": "value2"}',
            '{"key": "value3"}',
        ]

        source.open(data, "r", 5)
        # lines are returned as a list so we have to wrap our data once more
        assert list(source) == [data]
        source.close()


class TestConflictingIdsBuilder:
    def test_no_id_conflicts(self):
        assert params.build_conflicting_ids(None, 100, 0) is None
        assert params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 100, 0) is None

    def test_sequential_conflicts(self):
        assert params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 11, 0) == [
            "0000000000",
            "0000000001",
            "0000000002",
            "0000000003",
            "0000000004",
            "0000000005",
            "0000000006",
            "0000000007",
            "0000000008",
            "0000000009",
            "0000000010",
        ]

        assert params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 11, 5) == [
            "0000000005",
            "0000000006",
            "0000000007",
            "0000000008",
            "0000000009",
            "0000000010",
            "0000000011",
            "0000000012",
            "0000000013",
            "0000000014",
            "0000000015",
        ]

    def test_random_conflicts(self):
        predictable_shuffle = list.reverse

        assert params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0, shuffle=predictable_shuffle) == [
            "0000000002",
            "0000000001",
            "0000000000",
        ]

        assert params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 5, shuffle=predictable_shuffle) == [
            "0000000007",
            "0000000006",
            "0000000005",
        ]


class TestActionMetaData:
    def test_generate_action_meta_data_without_id_conflicts(self):
        assert next(params.GenerateActionMetaData("test_index", "test_type")) == (
            "index",
            '{"index": {"_index": "test_index", "_type": "test_type"}}\n',
        )

    def test_generate_action_meta_data_create(self):
        assert next(params.GenerateActionMetaData("test_index", None, use_create=True)) == (
            "create",
            '{"create": {"_index": "test_index"}}\n',
        )

    def test_generate_action_meta_data_create_with_conflicts(self):
        with pytest.raises(exceptions.RallyError) as exc:
            params.GenerateActionMetaData("test_index", None, conflicting_ids=[100, 200, 300, 400], use_create=True)
        assert exc.value.args[0] == "Index mode '_create' cannot be used with conflicting ids"

    def test_generate_action_meta_data_typeless(self):
        assert next(params.GenerateActionMetaData("test_index", type_name=None)) == (
            "index",
            '{"index": {"_index": "test_index"}}\n',
        )

    def test_generate_action_meta_data_with_id_conflicts(self):
        def idx(id):
            return "index", '{"index": {"_index": "test_index", "_type": "test_type", "_id": "%s"}}\n' % id

        def conflict(action, id):
            return action, f'{{"{action}": {{"_index": "test_index", "_type": "test_type", "_id": "{id}"}}}}\n'

        pseudo_random_conflicts = iter(
            [
                # if this value is <= our chosen threshold of 0.25 (see conflict_probability) we produce a conflict.
                0.2,
                0.25,
                0.2,
                # no conflict
                0.3,
                # conflict again
                0.0,
            ]
        )

        chosen_index_of_conflicting_ids = iter(
            [
                # the "random" index of the id in the array `conflicting_ids` that will produce a conflict
                1,
                3,
                2,
                0,
            ]
        )

        conflict_action = random.choice(["index", "update"])

        generator = params.GenerateActionMetaData(
            "test_index",
            "test_type",
            conflicting_ids=[100, 200, 300, 400],
            conflict_probability=25,
            on_conflict=conflict_action,
            rand=lambda: next(pseudo_random_conflicts),
            randint=lambda x, y: next(chosen_index_of_conflicting_ids),
        )

        # first one is always *not* drawn from a random index
        assert next(generator) == idx("100")
        # now we start using random ids, i.e. look in the first line of the pseudo-random sequence
        assert next(generator) == conflict(conflict_action, "200")
        assert next(generator) == conflict(conflict_action, "400")
        assert next(generator) == conflict(conflict_action, "300")
        # no conflict -> we draw the next sequential one, which is 200
        assert next(generator) == idx("200")
        # and we're back to random
        assert next(generator) == conflict(conflict_action, "100")

    def test_generate_action_meta_data_with_id_conflicts_and_recency_bias(self):
        def idx(type_name, id):
            if type_name:
                return "index", f'{{"index": {{"_index": "test_index", "_type": "{type_name}", "_id": "{id}"}}}}\n'
            else:
                return "index", '{"index": {"_index": "test_index", "_id": "%s"}}\n' % id

        def conflict(action, type_name, id):
            if type_name:
                return action, f'{{"{action}": {{"_index": "test_index", "_type": "{type_name}", "_id": "{id}"}}}}\n'
            else:
                return action, f'{{"{action}": {{"_index": "test_index", "_id": "{id}"}}}}\n'

        pseudo_random_conflicts = iter(
            [
                # if this value is <= our chosen threshold of 0.25 (see conflict_probability) we produce a conflict.
                0.2,
                0.25,
                0.2,
                # no conflict
                0.3,
                0.4,
                0.35,
                # conflict again
                0.0,
                0.2,
                0.15,
            ]
        )

        # we use this value as `idx_range` in the calculation: idx = round((self.id_up_to - 1) * (1 - idx_range))
        pseudo_exponential_distribution = iter(
            [
                # id_up_to = 1 -> idx = 0
                0.013375248172714948,
                # id_up_to = 1 -> idx = 0
                0.042495604491024914,
                # id_up_to = 1 -> idx = 0
                0.005491072642023834,
                # no conflict: id_up_to = 2
                # no conflict: id_up_to = 3
                # no conflict: id_up_to = 4
                # id_up_to = 4 -> idx = round((4 - 1) * (1 - 0.028557879547255083)) = 3
                0.028557879547255083,
                # id_up_to = 4 -> idx = round((4 - 1) * (1 - 0.209771474243926352)) = 2
                0.209771474243926352,
            ]
        )

        conflict_action = random.choice(["index", "update"])
        type_name = random.choice([None, "test_type"])

        generator = params.GenerateActionMetaData(
            "test_index",
            type_name=type_name,
            conflicting_ids=[100, 200, 300, 400, 500, 600],
            conflict_probability=25,
            # heavily biased towards recent ids
            recency=1.0,
            on_conflict=conflict_action,
            rand=lambda: next(pseudo_random_conflicts),
            # we don't use this one here because recency is > 0.
            # randint=lambda x, y: next(chosen_index_of_conflicting_ids),
            randexp=lambda lmbda: next(pseudo_exponential_distribution),
        )

        # first one is always *not* drawn from a random index
        assert next(generator) == idx(type_name, "100")
        # now we start using random ids
        assert next(generator) == conflict(conflict_action, type_name, "100")
        assert next(generator) == conflict(conflict_action, type_name, "100")
        assert next(generator) == conflict(conflict_action, type_name, "100")
        # no conflict
        assert next(generator) == idx(type_name, "200")
        assert next(generator) == idx(type_name, "300")
        assert next(generator) == idx(type_name, "400")
        # conflict
        assert next(generator) == conflict(conflict_action, type_name, "400")
        assert next(generator) == conflict(conflict_action, type_name, "300")

    def test_generate_action_meta_data_with_id_and_zero_conflict_probability(self):
        def idx(id):
            return "index", '{"index": {"_index": "test_index", "_type": "test_type", "_id": "%s"}}\n' % id

        test_ids = [100, 200, 300, 400]

        generator = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=test_ids, conflict_probability=0)

        assert list(generator) == [idx(id) for id in test_ids]


class TestIndexDataReader:
    def test_read_bulk_larger_than_number_of_docs(self):
        data = [b'{"key": "value1"}\n', b'{"key": "value2"}\n', b'{"key": "value3"}\n', b'{"key": "value4"}\n', b'{"key": "value5"}\n']
        bulk_size = 50

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        expected_bulk_sizes = [len(data)]
        # lines should include meta-data
        expected_line_sizes = [len(data) * 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulk_with_offset(self):
        data = [b'{"key": "value1"}\n', b'{"key": "value2"}\n', b'{"key": "value3"}\n', b'{"key": "value4"}\n', b'{"key": "value5"}\n']
        bulk_size = 50

        source = params.Slice(io.StringAsFileSource, 3, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        expected_bulk_sizes = [(len(data) - 3)]
        # lines should include meta-data
        expected_line_sizes = [(len(data) - 3) * 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulk_smaller_than_number_of_docs(self):
        data = [
            b'{"key": "value1"}\n',
            b'{"key": "value2"}\n',
            b'{"key": "value3"}\n',
            b'{"key": "value4"}\n',
            b'{"key": "value5"}\n',
            b'{"key": "value6"}\n',
            b'{"key": "value7"}\n',
        ]
        bulk_size = 3

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        expected_bulk_sizes = [3, 3, 1]
        # lines should include meta-data
        expected_line_sizes = [6, 6, 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulk_smaller_than_number_of_docs_and_multiple_clients(self):
        data = [
            b'{"key": "value1"}\n',
            b'{"key": "value2"}\n',
            b'{"key": "value3"}\n',
            b'{"key": "value4"}\n',
            b'{"key": "value5"}\n',
            b'{"key": "value6"}\n',
            b'{"key": "value7"}\n',
        ]
        bulk_size = 3

        # only 5 documents to index for this client
        source = params.Slice(io.StringAsFileSource, 0, 5)
        am_handler = params.GenerateActionMetaData("test_index", "test_type")

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        expected_bulk_sizes = [3, 2]
        # lines should include meta-data
        expected_line_sizes = [6, 4]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulks_and_assume_metadata_line_in_source_file(self):
        data = [
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value1"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value2"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value3"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value4"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value5"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value6"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type"}\n',
            b'{"key": "value7"}\n',
        ]
        bulk_size = 3

        source = params.Slice(io.StringAsFileSource, 0, len(data))

        reader = params.SourceOnlyIndexDataReader(
            data, batch_size=bulk_size, bulk_size=bulk_size, file_source=source, index_name="test_index", type_name="test_type"
        )

        expected_bulk_sizes = [3, 3, 1]
        # lines should include meta-data
        expected_line_sizes = [6, 6, 2]
        self.assert_bulks_sized(reader, expected_bulk_sizes, expected_line_sizes)

    def test_read_bulk_with_id_conflicts(self):
        pseudo_random_conflicts = iter(
            [
                # if this value is <= our chosen threshold of 0.25 (see conflict_probability) we produce a conflict.
                0.2,
                0.25,
                0.2,
                # no conflict
                0.3,
            ]
        )

        chosen_index_of_conflicting_ids = iter(
            [
                # the "random" index of the id in the array `conflicting_ids` that will produce a conflict
                1,
                3,
                2,
            ]
        )

        data = [b'{"key": "value1"}\n', b'{"key": "value2"}\n', b'{"key": "value3"}\n', b'{"key": "value4"}\n', b'{"key": "value5"}\n']
        bulk_size = 2

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData(
            "test_index",
            "test_type",
            conflicting_ids=[100, 200, 300, 400],
            conflict_probability=25,
            on_conflict="update",
            rand=lambda: next(pseudo_random_conflicts),
            randint=lambda x, y: next(chosen_index_of_conflicting_ids),
        )

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        # consume all bulks
        bulks = []
        with reader:
            for _, _, batch in reader:
                for bulk_size, bulk in batch:
                    bulks.append(bulk)

        assert bulks == [
            b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "100"}}\n'
            + b'{"key": "value1"}\n'
            + b'{"update": {"_index": "test_index", "_type": "test_type", "_id": "200"}}\n'
            + b'{"doc":{"key": "value2"}}\n',
            b'{"update": {"_index": "test_index", "_type": "test_type", "_id": "400"}}\n'
            + b'{"doc":{"key": "value3"}}\n'
            + b'{"update": {"_index": "test_index", "_type": "test_type", "_id": "300"}}\n'
            + b'{"doc":{"key": "value4"}}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "200"}}\n' + b'{"key": "value5"}\n',
        ]

    def test_read_bulk_with_external_id_and_zero_conflict_probability(self):
        data = [b'{"key": "value1"}\n', b'{"key": "value2"}\n', b'{"key": "value3"}\n', b'{"key": "value4"}\n']
        bulk_size = 2

        source = params.Slice(io.StringAsFileSource, 0, len(data))
        am_handler = params.GenerateActionMetaData("test_index", "test_type", conflicting_ids=[100, 200, 300, 400], conflict_probability=0)

        reader = params.MetadataIndexDataReader(
            data,
            batch_size=bulk_size,
            bulk_size=bulk_size,
            file_source=source,
            action_metadata=am_handler,
            index_name="test_index",
            type_name="test_type",
        )

        # consume all bulks
        bulks = []
        with reader:
            for _, _, batch in reader:
                for bulk_size, bulk in batch:
                    bulks.append(bulk)

        assert bulks == [
            b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "100"}}\n'
            + b'{"key": "value1"}\n'
            + b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "200"}}\n'
            + b'{"key": "value2"}\n',
            b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "300"}}\n'
            + b'{"key": "value3"}\n'
            + b'{"index": {"_index": "test_index", "_type": "test_type", "_id": "400"}}\n'
            + b'{"key": "value4"}\n',
        ]

    def assert_bulks_sized(self, reader, expected_bulk_sizes, expected_line_sizes):
        assert len(expected_bulk_sizes) == len(expected_line_sizes), "Bulk sizes and line sizes must be equal"
        with reader:
            bulk_index = 0
            for _, _, batch in reader:
                for bulk_size, bulk in batch:
                    assert bulk_size == expected_bulk_sizes[bulk_index]
                    assert bulk.count(b"\n") == expected_line_sizes[bulk_index]
                    bulk_index += 1
            assert bulk_index == len(expected_bulk_sizes)


class TestInvocationGenerator:
    class MockIndexReader:
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

    class MockIndex:
        def __init__(self, name, types):
            self.name = name
            self.types = types

    class MockType:
        def __init__(self, number_of_documents, includes_action_and_meta_data=False):
            self.number_of_documents = number_of_documents
            self.includes_action_and_meta_data = includes_action_and_meta_data

    def idx(self, *args, **kwargs):
        return self.MockIndex(*args, **kwargs)

    def t(self, *args, **kwargs):
        return self.MockType(*args, **kwargs)

    def test_iterator_chaining_respects_context_manager(self):
        i0 = self.MockIndexReader([1, 2, 3])
        i1 = self.MockIndexReader([4, 5, 6])

        assert list(params.chain(i0, i1)) == [1, 2, 3, 4, 5, 6]
        assert i0.enter_count == 1
        assert i0.exit_count == 1
        assert i1.enter_count == 1
        assert i1.exit_count == 1

    def test_calculate_bounds(self):
        num_docs = 1000
        clients = 1
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=False) == (0, 1000, 1000)
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=True) == (0, 1000, 2000)

        num_docs = 1000
        clients = 2
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=False) == (0, 500, 500)
        assert params.bounds(num_docs, 1, 1, clients, includes_action_and_meta_data=False) == (500, 500, 500)

        num_docs = 800
        clients = 4
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=True) == (0, 200, 400)
        assert params.bounds(num_docs, 1, 1, clients, includes_action_and_meta_data=True) == (400, 200, 400)
        assert params.bounds(num_docs, 2, 2, clients, includes_action_and_meta_data=True) == (800, 200, 400)
        assert params.bounds(num_docs, 3, 3, clients, includes_action_and_meta_data=True) == (1200, 200, 400)

        num_docs = 2000
        clients = 8
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=False) == (0, 250, 250)
        assert params.bounds(num_docs, 1, 1, clients, includes_action_and_meta_data=False) == (250, 250, 250)
        assert params.bounds(num_docs, 2, 2, clients, includes_action_and_meta_data=False) == (500, 250, 250)
        assert params.bounds(num_docs, 3, 3, clients, includes_action_and_meta_data=False) == (750, 250, 250)
        assert params.bounds(num_docs, 4, 4, clients, includes_action_and_meta_data=False) == (1000, 250, 250)
        assert params.bounds(num_docs, 5, 5, clients, includes_action_and_meta_data=False) == (1250, 250, 250)
        assert params.bounds(num_docs, 6, 6, clients, includes_action_and_meta_data=False) == (1500, 250, 250)
        assert params.bounds(num_docs, 7, 7, clients, includes_action_and_meta_data=False) == (1750, 250, 250)

    def test_calculate_non_multiple_bounds_16_clients(self):
        # in this test case, each client would need to read 1333.3333 lines. Instead we let most clients read 1333
        # lines and every third client, one line more (1334).
        num_docs = 16000
        clients = 12
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=False) == (0, 1333, 1333)
        assert params.bounds(num_docs, 1, 1, clients, includes_action_and_meta_data=False) == (1333, 1334, 1334)
        assert params.bounds(num_docs, 2, 2, clients, includes_action_and_meta_data=False) == (2667, 1333, 1333)
        assert params.bounds(num_docs, 3, 3, clients, includes_action_and_meta_data=False) == (4000, 1333, 1333)
        assert params.bounds(num_docs, 4, 4, clients, includes_action_and_meta_data=False) == (5333, 1334, 1334)
        assert params.bounds(num_docs, 5, 5, clients, includes_action_and_meta_data=False) == (6667, 1333, 1333)
        assert params.bounds(num_docs, 6, 6, clients, includes_action_and_meta_data=False) == (8000, 1333, 1333)
        assert params.bounds(num_docs, 7, 7, clients, includes_action_and_meta_data=False) == (9333, 1334, 1334)
        assert params.bounds(num_docs, 8, 8, clients, includes_action_and_meta_data=False) == (10667, 1333, 1333)
        assert params.bounds(num_docs, 9, 9, clients, includes_action_and_meta_data=False) == (12000, 1333, 1333)
        assert params.bounds(num_docs, 10, 10, clients, includes_action_and_meta_data=False) == (13333, 1334, 1334)
        assert params.bounds(num_docs, 11, 11, clients, includes_action_and_meta_data=False) == (14667, 1333, 1333)

    def test_calculate_non_multiple_bounds_6_clients(self):
        # With 3500 docs and 6 clients, every client needs to read 583.33 docs. We have two lines per doc, which makes it
        # 2 * 583.333 docs = 1166.6666 lines per client. We let them read 1166 and 1168 lines respectively (583 and 584 docs).
        num_docs = 3500
        clients = 6
        assert params.bounds(num_docs, 0, 0, clients, includes_action_and_meta_data=True) == (0, 583, 1166)
        assert params.bounds(num_docs, 1, 1, clients, includes_action_and_meta_data=True) == (1166, 584, 1168)
        assert params.bounds(num_docs, 2, 2, clients, includes_action_and_meta_data=True) == (2334, 583, 1166)
        assert params.bounds(num_docs, 3, 3, clients, includes_action_and_meta_data=True) == (3500, 583, 1166)
        assert params.bounds(num_docs, 4, 4, clients, includes_action_and_meta_data=True) == (4666, 584, 1168)
        assert params.bounds(num_docs, 5, 5, clients, includes_action_and_meta_data=True) == (5834, 583, 1166)

    def test_calculate_bounds_for_multiple_clients_per_worker(self):
        num_docs = 2000
        clients = 8
        # four clients per worker, each reads 250 lines
        assert params.bounds(num_docs, 0, 3, clients, includes_action_and_meta_data=False) == (0, 1000, 1000)
        assert params.bounds(num_docs, 4, 7, clients, includes_action_and_meta_data=False) == (1000, 1000, 1000)

        # four clients per worker, each reads 500 lines (includes action and metadata)
        assert params.bounds(num_docs, 0, 3, clients, includes_action_and_meta_data=True) == (0, 1000, 2000)
        assert params.bounds(num_docs, 4, 7, clients, includes_action_and_meta_data=True) == (2000, 1000, 2000)

    def test_calculate_number_of_bulks(self):
        docs1 = self.docs(1)
        docs2 = self.docs(2)

        assert self.number_of_bulks([self.corpus("a", [docs1])], 0, 0, 1, 1) == 1
        assert self.number_of_bulks([self.corpus("a", [docs1])], 0, 0, 1, 2) == 1
        assert (
            self.number_of_bulks(
                [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]), self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])],
                0,
                0,
                1,
                1,
            )
            == 20
        )
        assert (
            self.number_of_bulks(
                [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]), self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])],
                0,
                0,
                1,
                2,
            )
            == 11
        )
        assert (
            self.number_of_bulks(
                [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]), self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])],
                0,
                0,
                1,
                3,
            )
            == 11
        )
        assert (
            self.number_of_bulks(
                [self.corpus("a", [docs2, docs2, docs2, docs2, docs1]), self.corpus("b", [docs2, docs2, docs2, docs2, docs2, docs1])],
                0,
                0,
                1,
                100,
            )
            == 11
        )

        assert self.number_of_bulks([self.corpus("a", [self.docs(800)])], 0, 0, 3, 250) == 2
        assert self.number_of_bulks([self.corpus("a", [self.docs(800)])], 0, 0, 3, 267) == 1
        assert self.number_of_bulks([self.corpus("a", [self.docs(80)])], 0, 0, 3, 267) == 1
        # this looks odd at first but we are prioritizing number of clients above bulk size
        assert self.number_of_bulks([self.corpus("a", [self.docs(80)])], 1, 1, 3, 267) == 1
        assert self.number_of_bulks([self.corpus("a", [self.docs(80)])], 2, 2, 3, 267) == 1

    @staticmethod
    def corpus(name, docs):
        return track.DocumentCorpus(name, documents=docs)

    @staticmethod
    def docs(num_docs):
        return track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=num_docs)

    @staticmethod
    def number_of_bulks(corpora, first_partition_index, last_partition_index, total_partitions, bulk_size):
        return params.number_of_bulks(corpora, first_partition_index, last_partition_index, total_partitions, bulk_size)

    def test_build_conflicting_ids(self):
        assert params.build_conflicting_ids(params.IndexIdConflict.NoConflicts, 3, 0) is None
        assert params.build_conflicting_ids(params.IndexIdConflict.SequentialConflicts, 3, 0) == ["0000000000", "0000000001", "0000000002"]
        # we cannot tell anything specific about the contents...
        assert len(params.build_conflicting_ids(params.IndexIdConflict.RandomConflicts, 3, 0)) == 3


# pylint: disable=too-many-public-methods
class TestBulkIndexParamSource:
    def test_create_without_params(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={},
            )

        assert exc.value.args[0] == "Mandatory parameter 'bulk-size' is missing"

    def test_create_without_corpora_definition(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"),
                params={},
            )

        assert exc.value.args[0] == (
            "There is no document corpus definition for track unit-test. "
            "You must add at least one before making bulk requests to Elasticsearch."
        )

    def test_create_with_non_numeric_bulk_size(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": "Three",
                },
            )

        assert exc.value.args[0] == "'bulk-size' must be numeric"

    def test_create_with_negative_bulk_size(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": -5,
                },
            )

        assert exc.value.args[0] == "'bulk-size' must be positive but was -5"

    def test_create_with_fraction_smaller_batch_size(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": 5,
                    "batch-size": 3,
                },
            )

        assert exc.value.args[0] == "'batch-size' must be greater than or equal to 'bulk-size'"

    def test_create_with_fraction_larger_batch_size(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": 5,
                    "batch-size": 8,
                },
            )

        assert exc.value.args[0] == "'batch-size' must be a multiple of 'bulk-size'"

    def test_create_with_metadata_in_source_file_but_conflicts(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    document_archive="docs.json.bz2",
                    document_file="docs.json",
                    number_of_documents=10,
                    includes_action_and_meta_data=True,
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "conflicts": "random",
                },
            )

        assert exc.value.args[0] == (
            "Cannot generate id conflicts [random] as [docs.json.bz2] in document corpus [default] already contains "
            "an action and meta-data line."
        )

    def test_create_with_unknown_id_conflicts(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"),
                params={
                    "conflicts": "crazy",
                },
            )

        assert exc.value.args[0] == "Unknown 'conflicts' setting [crazy]"

    def test_create_with_unknown_on_conflict_setting(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"),
                params={
                    "conflicts": "sequential",
                    "on-conflict": "delete",
                },
            )

        assert exc.value.args[0] == "Unknown 'on-conflict' setting [delete]"

    def test_create_with_conflicts_and_data_streams(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"),
                params={
                    "data-streams": ["test-data-stream-1", "test-data-stream-2"],
                    "conflicts": "sequential",
                },
            )

        assert exc.value.args[0] == "'conflicts' cannot be used with 'data-streams'"

    def test_create_with_ingest_percentage_too_low(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": 5000,
                    "ingest-percentage": 0.0,
                },
            )

        assert exc.value.args[0] == "'ingest-percentage' must be in the range (0.0, 100.0] but was 0.0"

    def test_create_with_ingest_percentage_too_high(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": 5000,
                    "ingest-percentage": 100.1,
                },
            )

        assert exc.value.args[0] == "'ingest-percentage' must be in the range (0.0, 100.0] but was 100.1"

    def test_create_with_ingest_percentage_not_numeric(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "bulk-size": 5000,
                    "ingest-percentage": "100 percent",
                },
            )

        assert exc.value.args[0] == "'ingest-percentage' must be numeric"

    def test_create_valid_param_source(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        assert (
            params.BulkIndexParamSource(
                track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "conflicts": "random",
                    "bulk-size": 5000,
                    "batch-size": 20000,
                    "ingest-percentage": 20.5,
                    "pipeline": "test-pipeline",
                },
            )
            is not None
        )

    def test_passes_all_corpora_by_default(self):
        corpora = [
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=10,
                        target_index="test-idx",
                        target_type="test-type",
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=100,
                        target_index="test-idx2",
                        target_type="type",
                    )
                ],
            ),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline",
            },
        )

        partition = source.partition(0, 1)
        assert partition.corpora == corpora

    def test_filters_corpora(self):
        corpora = [
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=10,
                        target_index="test-idx",
                        target_type="test-type",
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=100,
                        target_index="test-idx2",
                        target_type="type",
                    )
                ],
            ),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "corpora": ["special"],
                "conflicts": "random",
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline",
            },
        )

        partition = source.partition(0, 1)
        assert partition.corpora == [corpora[1]]

    def test_filters_corpora_by_data_stream(self):
        corpora = [
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=10, target_data_stream="test-data-stream-1"
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=100,
                        target_index="test-idx2",
                        target_type="type",
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special-2",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=10, target_data_stream="test-data-stream-2"
                    )
                ],
            ),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "data-streams": ["test-data-stream-1", "test-data-stream-2"],
                "bulk-size": 5000,
                "batch-size": 20000,
                "pipeline": "test-pipeline",
            },
        )

        partition = source.partition(0, 1)
        assert partition.corpora == [corpora[0], corpora[2]]

    def test_raises_exception_if_no_corpus_matches(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        with pytest.raises(exceptions.RallyAssertionError) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test", corpora=[corpus]),
                params={
                    "corpora": "does_not_exist",
                    "conflicts": "random",
                    "bulk-size": 5000,
                    "batch-size": 20000,
                    "pipeline": "test-pipeline",
                },
            )

        assert exc.value.args[0] == "The provided corpus ['does_not_exist'] does not match any of the corpora ['default']."

    def test_ingests_all_documents_by_default(self):
        corpora = [
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=300000,
                        target_index="test-idx",
                        target_type="test-type",
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=700000,
                        target_index="test-idx2",
                        target_type="type",
                    )
                ],
            ),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "bulk-size": 10000,
            },
        )

        partition = source.partition(0, 1)
        partition._init_internal_params()
        # # no ingest-percentage specified, should issue all one hundred bulk requests
        assert partition.total_bulks == 100

    def test_restricts_number_of_bulks_if_required(self):
        def create_unit_test_reader(*args):
            return StaticBulkReader(
                "idx",
                "doc",
                bulks=[
                    ['{"location" : [-0.1485188, 51.5250666]}'],
                    ['{"location" : [-0.1479949, 51.5252071]}'],
                    ['{"location" : [-0.1458559, 51.5289059]}'],
                    ['{"location" : [-0.1498551, 51.5282564]}'],
                    ['{"location" : [-0.1487043, 51.5254843]}'],
                    ['{"location" : [-0.1533367, 51.5261779]}'],
                    ['{"location" : [-0.1543018, 51.5262398]}'],
                    ['{"location" : [-0.1522118, 51.5266564]}'],
                    ['{"location" : [-0.1529092, 51.5263360]}'],
                    ['{"location" : [-0.1537008, 51.5265365]}'],
                ],
            )

        def schedule(param_source):
            while True:
                try:
                    yield param_source.params()
                except StopIteration:
                    return

        corpora = [
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=300000,
                        target_index="test-idx",
                        target_type="test-type",
                    )
                ],
            ),
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=700000,
                        target_index="test-idx2",
                        target_type="type",
                    )
                ],
            ),
        ]

        source = params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=corpora),
            params={
                "bulk-size": 10000,
                "ingest-percentage": 2.5,
                "__create_reader": create_unit_test_reader,
            },
        )

        partition = source.partition(0, 1)
        partition._init_internal_params()
        # should issue three bulks of size 10.000
        assert partition.total_bulks == 3
        assert len(list(schedule(partition))) == 3

    def test_create_with_conflict_probability_zero(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        params.BulkIndexParamSource(
            track=track.Track(name="unit-test", corpora=[corpus]),
            params={"bulk-size": 5000, "conflicts": "sequential", "conflict-probability": 0},
        )

    def test_create_with_conflict_probability_too_low(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"), params={"bulk-size": 5000, "conflicts": "sequential", "conflict-probability": -0.1}
            )

        assert exc.value.args[0] == "'conflict-probability' must be in the range [0.0, 100.0] but was -0.1"

    def test_create_with_conflict_probability_too_high(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"), params={"bulk-size": 5000, "conflicts": "sequential", "conflict-probability": 100.1}
            )

        assert exc.value.args[0] == "'conflict-probability' must be in the range [0.0, 100.0] but was 100.1"

    def test_create_with_conflict_probability_not_numeric(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.BulkIndexParamSource(
                track=track.Track(name="unit-test"),
                params={"bulk-size": 5000, "conflicts": "sequential", "conflict-probability": "100 percent"},
            )

        assert exc.value.args[0] == "'conflict-probability' must be numeric"


class TestBulkDataGenerator:
    @classmethod
    def create_test_reader(cls, batches):
        def inner_create_test_reader(docs, *args):
            return StaticBulkReader(docs.target_index, docs.target_type, batches)

        return inner_create_test_reader

    def test_generate_two_bulks(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=10,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        bulks = params.bulk_data_based(
            num_clients=1,
            start_client_index=0,
            end_client_index=0,
            corpora=[corpus],
            batch_size=5,
            bulk_size=5,
            id_conflicts=params.IndexIdConflict.NoConflicts,
            conflict_probability=None,
            on_conflict=None,
            recency=None,
            pipeline=None,
            original_params={"my-custom-parameter": "foo", "my-custom-parameter-2": True},
            create_reader=self.create_test_reader([["1", "2", "3", "4", "5"], ["6", "7", "8"]]),
        )
        assert list(bulks) == [
            {
                "action-metadata-present": True,
                "body": ["1", "2", "3", "4", "5"],
                "bulk-size": 5,
                "unit": "docs",
                "index": "test-idx",
                "type": "test-type",
                "my-custom-parameter": "foo",
                "my-custom-parameter-2": True,
            },
            {
                "action-metadata-present": True,
                "body": ["6", "7", "8"],
                "bulk-size": 3,
                "unit": "docs",
                "index": "test-idx",
                "type": "test-type",
                "my-custom-parameter": "foo",
                "my-custom-parameter-2": True,
            },
        ]

    def test_generate_bulks_from_multiple_corpora(self):
        corpora = [
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2017-01",
                        target_type="docs",
                    ),
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=0,
                        target_index="logs-2017-02",
                        target_type="docs",
                    ),
                ],
            ),
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2018-01",
                        target_type="docs",
                    ),
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2018-02",
                        target_type="docs",
                    ),
                ],
            ),
        ]

        bulks = params.bulk_data_based(
            num_clients=1,
            start_client_index=0,
            end_client_index=0,
            corpora=corpora,
            batch_size=5,
            bulk_size=5,
            id_conflicts=params.IndexIdConflict.NoConflicts,
            conflict_probability=None,
            on_conflict=None,
            recency=None,
            pipeline=None,
            original_params={"my-custom-parameter": "foo", "my-custom-parameter-2": True},
            create_reader=self.create_test_reader([["1", "2", "3", "4", "5"]]),
        )
        assert list(bulks) == [
            {
                "action-metadata-present": True,
                "body": ["1", "2", "3", "4", "5"],
                "bulk-size": 5,
                "unit": "docs",
                "index": "logs-2017-01",
                "type": "docs",
                "my-custom-parameter": "foo",
                "my-custom-parameter-2": True,
            },
            {
                "action-metadata-present": True,
                "body": ["1", "2", "3", "4", "5"],
                "bulk-size": 5,
                "unit": "docs",
                "index": "logs-2018-01",
                "type": "docs",
                "my-custom-parameter": "foo",
                "my-custom-parameter-2": True,
            },
            {
                "action-metadata-present": True,
                "body": ["1", "2", "3", "4", "5"],
                "bulk-size": 5,
                "unit": "docs",
                "index": "logs-2018-02",
                "type": "docs",
                "my-custom-parameter": "foo",
                "my-custom-parameter-2": True,
            },
        ]

    def test_generate_bulks_with_more_clients_than_corpora(self):
        corpora = [
            track.DocumentCorpus(
                name="special",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2017-01",
                        target_type="docs",
                    ),
                ],
            ),
            track.DocumentCorpus(
                name="default",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2018-01",
                        target_type="docs",
                    ),
                ],
            ),
            track.DocumentCorpus(
                name="defaults",
                documents=[
                    track.Documents(
                        source_format=track.Documents.SOURCE_FORMAT_BULK,
                        number_of_documents=5,
                        target_index="logs-2019-01",
                        target_type="docs",
                    ),
                ],
            ),
        ]

        for client_index, indices in [
            (0, ["logs-2017-01", "logs-2018-01", "logs-2019-01"]),
            (1, ["logs-2018-01", "logs-2019-01", "logs-2017-01"]),
            (2, ["logs-2019-01", "logs-2017-01", "logs-2018-01"]),
            (3, ["logs-2017-01", "logs-2018-01", "logs-2019-01"]),
            (4, ["logs-2018-01", "logs-2019-01", "logs-2017-01"]),
        ]:
            bulks = params.bulk_data_based(
                num_clients=5,
                start_client_index=client_index,
                end_client_index=client_index,
                corpora=corpora,
                batch_size=5,
                bulk_size=5,
                id_conflicts=params.IndexIdConflict.NoConflicts,
                conflict_probability=None,
                on_conflict=None,
                recency=None,
                pipeline=None,
                original_params={},
                create_reader=self.create_test_reader([["1", "2", "3", "4", "5"]]),
            )
            assert [bulk["index"] for bulk in bulks] == indices

    def test_internal_params_take_precedence(self):
        corpus = track.DocumentCorpus(
            name="default",
            documents=[
                track.Documents(
                    source_format=track.Documents.SOURCE_FORMAT_BULK,
                    number_of_documents=3,
                    target_index="test-idx",
                    target_type="test-type",
                )
            ],
        )

        bulks = params.bulk_data_based(
            num_clients=1,
            start_client_index=0,
            end_client_index=0,
            corpora=[corpus],
            batch_size=3,
            bulk_size=3,
            id_conflicts=params.IndexIdConflict.NoConflicts,
            conflict_probability=None,
            on_conflict=None,
            recency=None,
            pipeline=None,
            original_params={"body": "foo", "custom-param": "bar"},
            create_reader=self.create_test_reader([["1", "2", "3"]]),
        )
        # body must not contain 'foo'!
        assert list(bulks) == [
            {
                "action-metadata-present": True,
                "body": ["1", "2", "3"],
                "bulk-size": 3,
                "unit": "docs",
                "index": "test-idx",
                "type": "test-type",
                "custom-param": "bar",
            }
        ]


class TestParamsRegistration:
    @staticmethod
    def param_source_legacy_function(indices, params):
        return {"key": params["parameter"]}

    @staticmethod
    def param_source_function(track, params, **kwargs):
        return {"key": params["parameter"]}

    class ParamSourceLegacyClass:
        def __init__(self, indices=None, params=None):
            self._indices = indices
            self._params = params

        def partition(self, partition_index, total_partitions):
            return self

        def size(self):
            return 1

        def params(self):
            return {"class-key": self._params["parameter"]}

    class ParamSourceClass:
        def __init__(self, track=None, params=None, **kwargs):
            self._track = track
            self._params = params

        def partition(self, partition_index, total_partitions):
            return self

        def size(self):
            return 1

        def params(self):
            return {"class-key": self._params["parameter"]}

        def __str__(self):
            return "test param source"

    def test_can_register_legacy_function_as_param_source(self):
        source_name = "legacy-params-test-function-param-source"

        params.register_param_source_for_name(source_name, self.param_source_legacy_function)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        assert source.params() == {"key": 42}

        params._unregister_param_source_for_name(source_name)

    def test_can_register_function_as_param_source(self):
        source_name = "params-test-function-param-source"

        params.register_param_source_for_name(source_name, self.param_source_function)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        assert source.params() == {"key": 42}

        params._unregister_param_source_for_name(source_name)

    def test_can_register_legacy_class_as_param_source(self):
        source_name = "legacy-params-test-class-param-source"

        params.register_param_source_for_name(source_name, self.ParamSourceLegacyClass)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        assert source.params() == {"class-key": 42}

        params._unregister_param_source_for_name(source_name)

    def test_can_register_class_as_param_source(self):
        source_name = "params-test-class-param-source"

        params.register_param_source_for_name(source_name, self.ParamSourceClass)
        source = params.param_source_for_name(source_name, track.Track(name="unit-test"), {"parameter": 42})
        assert source.params() == {"class-key": 42}

        params._unregister_param_source_for_name(source_name)

    def test_cannot_register_an_instance_as_param_source(self):
        source_name = "params-test-class-param-source"
        # we create an instance, instead of passing the class
        with pytest.raises(
            exceptions.RallyAssertionError, match="Parameter source \\[test param source\\] must be either a function or a class\\."
        ):
            params.register_param_source_for_name(source_name, self.ParamSourceClass())


class TestSleepParamSource:
    def test_missing_duration_parameter(self):
        with pytest.raises(exceptions.InvalidSyntax, match="parameter 'duration' is mandatory for sleep operation"):
            params.SleepParamSource(track.Track(name="unit-test"), params={})

    def test_duration_parameter_wrong_type(self):
        with pytest.raises(exceptions.InvalidSyntax, match="parameter 'duration' for sleep operation must be a number"):
            params.SleepParamSource(track.Track(name="unit-test"), params={"duration": "this is a string"})

    def test_duration_parameter_negative_number(self):
        with pytest.raises(exceptions.InvalidSyntax, match="parameter 'duration' must be non-negative but was -1.0"):
            params.SleepParamSource(track.Track(name="unit-test"), params={"duration": -1.0})

    def test_param_source_passes_all_parameters(self):
        p = params.SleepParamSource(track.Track(name="unit-test"), params={"duration": 3.4, "additional": True})
        assert {"duration": 3.4, "additional": True} == p.params()


class TestCreateIndexParamSource:
    def test_create_index_inline_with_body(self):
        source = params.CreateIndexParamSource(
            track.Track(name="unit-test"),
            params={
                "index": "test",
                "body": {
                    "settings": {
                        "index.number_of_replicas": 0,
                    },
                    "mappings": {
                        "doc": {
                            "properties": {
                                "name": {
                                    "type": "keyword",
                                }
                            }
                        }
                    },
                },
            },
        )

        p = source.params()
        assert len(p["indices"]) == 1
        index, body = p["indices"][0]
        assert index == "test"
        assert len(body) > 0
        assert p["request-params"] == {}

    def test_create_index_inline_without_body(self):
        source = params.CreateIndexParamSource(
            track.Track(name="unit-test"),
            params={"index": "test", "request-params": {"wait_for_active_shards": True}},
        )

        p = source.params()
        assert len(p["indices"]) == 1
        index, body = p["indices"][0]
        assert index == "test"
        assert body is None
        assert p["request-params"] == {"wait_for_active_shards": True}

    def test_create_index_from_track_with_settings(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(
            name="index2",
            types=["type1"],
            body={
                "settings": {
                    "index.number_of_replicas": 0,
                    "index.number_of_shards": 3,
                },
                "mappings": {
                    "type1": {
                        "properties": {
                            "name": {
                                "type": "keyword",
                            }
                        }
                    }
                },
            },
        )

        source = params.CreateIndexParamSource(
            track.Track(name="unit-test", indices=[index1, index2]),
            params={
                "settings": {
                    "index.number_of_replicas": 1,
                },
            },
        )

        p = source.params()
        assert len(p["indices"]) == 2

        index, body = p["indices"][0]
        assert index == "index1"
        # index did not specify any body
        assert body == {"settings": {"index.number_of_replicas": 1}}

        index, body = p["indices"][1]
        assert index == "index2"
        # index specified a body + we need to merge settings
        assert body == {
            "settings": {
                # we have properly merged (overridden) an existing setting
                "index.number_of_replicas": 1,
                # and we have preserved one that was specified in the original index body
                "index.number_of_shards": 3,
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            },
        }

    def test_create_index_from_track_without_settings(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(
            name="index2",
            types=["type1"],
            body={
                "settings": {
                    "index.number_of_replicas": 0,
                    "index.number_of_shards": 3,
                },
                "mappings": {
                    "type1": {
                        "properties": {
                            "name": {
                                "type": "keyword",
                            }
                        }
                    }
                },
            },
        )

        source = params.CreateIndexParamSource(
            track.Track(name="unit-test", indices=[index1, index2]),
            params={},
        )

        p = source.params()
        assert len(p["indices"]) == 2

        index, body = p["indices"][0]
        assert index == "index1"
        # index did not specify any body
        assert body == {}

        index, body = p["indices"][1]
        assert index == "index2"
        # index specified a body
        assert body == {
            "settings": {
                "index.number_of_replicas": 0,
                "index.number_of_shards": 3,
            },
            "mappings": {
                "type1": {
                    "properties": {
                        "name": {
                            "type": "keyword",
                        }
                    }
                }
            },
        }

    def test_filter_index(self):
        index1 = track.Index(name="index1", types=["type1"])
        index2 = track.Index(name="index2", types=["type1"])
        index3 = track.Index(name="index3", types=["type1"])

        source = params.CreateIndexParamSource(
            track.Track(name="unit-test", indices=[index1, index2, index3]),
            params={"index": "index2"},
        )

        p = source.params()
        assert len(p["indices"]) == 1

        index, _ = p["indices"][0]
        assert index == "index2"


class TestCreateDataStreamParamSource:
    def test_create_data_stream(self):
        source = params.CreateDataStreamParamSource(
            track.Track(name="unit-test"),
            params={"data-stream": "test-data-stream"},
        )

        assert source.params() == {
            "data-stream": "test-data-stream",
            "data-streams": ["test-data-stream"],
            "request-params": {},
        }

    def test_create_data_stream_inline_without_body(self):
        source = params.CreateDataStreamParamSource(
            track.Track(name="unit-test"),
            params={
                "data-stream": "test-data-stream",
                "request-params": {"wait_for_active_shards": True},
            },
        )

        assert source.params() == {
            "data-stream": "test-data-stream",
            "data-streams": ["test-data-stream"],
            "request-params": {"wait_for_active_shards": True},
        }

    def test_filter_data_stream(self):
        source = params.CreateDataStreamParamSource(
            track.Track(
                name="unit-test",
                data_streams=[
                    track.DataStream(name="data-stream-1"),
                    track.DataStream(name="data-stream-2"),
                    track.DataStream(name="data-stream-3"),
                ],
            ),
            params={"data-stream": "data-stream-2"},
        )

        assert source.params() == {
            "data-stream": "data-stream-2",
            "data-streams": ["data-stream-2"],
            "request-params": {},
        }


class TestDeleteIndexParamSource:
    def test_delete_index_from_track(self):
        source = params.DeleteIndexParamSource(
            track.Track(
                name="unit-test",
                indices=[
                    track.Index(name="index1"),
                    track.Index(name="index2"),
                    track.Index(name="index3"),
                ],
            ),
            params={},
        )

        assert source.params() == {
            "indices": ["index1", "index2", "index3"],
            "request-params": {},
            "only-if-exists": True,
        }

    def test_filter_index_from_track(self):
        source = params.DeleteIndexParamSource(
            track.Track(
                name="unit-test",
                indices=[
                    track.Index(name="index1"),
                    track.Index(name="index2"),
                    track.Index(name="index3"),
                ],
            ),
            params={
                "index": "index2",
                "only-if-exists": False,
                "request-params": {"allow_no_indices": True},
            },
        )

        assert source.params() == {
            "indices": ["index2"],
            "index": "index2",
            "request-params": {"allow_no_indices": True},
            "only-if-exists": False,
        }

    def test_delete_index_by_name(self):
        source = params.DeleteIndexParamSource(
            track.Track(
                name="unit-test",
            ),
            params={"index": "index2"},
        )

        assert source.params() == {
            "indices": ["index2"],
            "index": "index2",
            "request-params": {},
            "only-if-exists": True,
        }

    def test_delete_no_index(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.DeleteIndexParamSource(track.Track(name="unit-test"), params={})
        assert exc.value.args[0] == "delete-index operation targets no index"


class TestDeleteDataStreamParamSource:
    def test_delete_data_stream_from_track(self):
        source = params.DeleteDataStreamParamSource(
            track.Track(
                name="unit-test",
                data_streams=[
                    track.DataStream(name="data-stream-1"),
                    track.DataStream(name="data-stream-2"),
                    track.DataStream(name="data-stream-3"),
                ],
            ),
            params={},
        )

        assert source.params() == {
            "data-streams": ["data-stream-1", "data-stream-2", "data-stream-3"],
            "request-params": {},
            "only-if-exists": True,
        }

    def test_filter_data_stream_from_track(self):
        source = params.DeleteDataStreamParamSource(
            track.Track(
                name="unit-test",
                data_streams=[
                    track.DataStream(name="data-stream-1"),
                    track.DataStream(name="data-stream-2"),
                    track.DataStream(name="data-stream-3"),
                ],
            ),
            params={
                "data-stream": "data-stream-2",
                "only-if-exists": False,
                "request-params": {"allow_no_indices": True},
            },
        )

        assert source.params() == {
            "data-stream": "data-stream-2",
            "data-streams": ["data-stream-2"],
            "only-if-exists": False,
            "request-params": {"allow_no_indices": True},
        }

    def test_delete_data_stream_by_name(self):
        source = params.DeleteDataStreamParamSource(
            track.Track(name="unit-test"),
            params={"data-stream": "data-stream-2"},
        )

        assert source.params() == {
            "data-stream": "data-stream-2",
            "data-streams": ["data-stream-2"],
            "request-params": {},
            "only-if-exists": True,
        }

    def test_delete_no_data_stream(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.DeleteDataStreamParamSource(
                track.Track(name="unit-test"),
                params={},
            )
        assert exc.value.args[0] == "delete-data-stream operation targets no data stream"


class TestCreateIndexTemplateParamSource:
    def test_create_index_template_inline(self):
        source = params.CreateIndexTemplateParamSource(
            track=track.Track(name="unit-test"),
            params={
                "template": "test",
                "body": {
                    "index_patterns": ["*"],
                    "settings": {"index.number_of_shards": 3},
                    "mappings": {"docs": {"_source": {"enabled": False}}},
                },
            },
        )

        p = source.params()

        assert len(p["templates"]) == 1
        assert p["request-params"] == {}
        template, body = p["templates"][0]
        assert template == "test"
        assert body == {
            "index_patterns": ["*"],
            "settings": {"index.number_of_shards": 3},
            "mappings": {
                "docs": {
                    "_source": {"enabled": False},
                },
            },
        }

    def test_create_index_template_from_track(self):
        tpl = track.IndexTemplate(
            name="default",
            pattern="*",
            content={
                "index_patterns": ["*"],
                "settings": {"index.number_of_shards": 3},
                "mappings": {
                    "docs": {
                        "_source": {"enabled": False},
                    },
                },
            },
        )

        source = params.CreateIndexTemplateParamSource(
            track=track.Track(name="unit-test", templates=[tpl]),
            params={
                "settings": {
                    "index.number_of_replicas": 1,
                }
            },
        )

        p = source.params()

        assert len(p["templates"]) == 1
        assert p["request-params"] == {}
        template, body = p["templates"][0]
        assert template == "default"
        assert body == {
            "index_patterns": ["*"],
            "settings": {"index.number_of_shards": 3, "index.number_of_replicas": 1},
            "mappings": {"docs": {"_source": {"enabled": False}}},
        }


class TestDeleteIndexTemplateParamSource:
    def test_delete_index_template_by_name(self):
        source = params.DeleteIndexTemplateParamSource(
            track.Track(name="unit-test"),
            params={"template": "default"},
        )

        assert source.params() == {
            "template": "default",
            "templates": [("default", False, None)],
            "only-if-exists": True,
            "request-params": {},
        }

    def test_delete_index_template_by_name_and_matching_indices(self):
        source = params.DeleteIndexTemplateParamSource(
            track.Track(name="unit-test"),
            params={
                "template": "default",
                "delete-matching-indices": True,
                "index-pattern": "logs-*",
            },
        )

        assert source.params() == {
            "template": "default",
            "delete-matching-indices": True,
            "index-pattern": "logs-*",
            "templates": [("default", True, "logs-*")],
            "only-if-exists": True,
            "request-params": {},
        }

    def test_delete_index_template_by_name_and_matching_indices_missing_index_pattern(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.DeleteIndexTemplateParamSource(
                track.Track(name="unit-test"),
                params={
                    "template": "default",
                    "delete-matching-indices": True,
                },
            )
        assert (
            exc.value.args[0] == "The property 'index-pattern' is required for delete-index-template if 'delete-matching-indices' is true."
        )

    def test_delete_index_template_from_track(self):
        tpl1 = track.IndexTemplate(
            name="metrics",
            pattern="metrics-*",
            delete_matching_indices=True,
            content={
                "index_patterns": ["metrics-*"],
                "settings": {},
                "mappings": {},
            },
        )
        tpl2 = track.IndexTemplate(
            name="logs",
            pattern="logs-*",
            delete_matching_indices=False,
            content={
                "index_patterns": ["logs-*"],
                "settings": {},
                "mappings": {},
            },
        )

        source = params.DeleteIndexTemplateParamSource(
            track.Track(name="unit-test", templates=[tpl1, tpl2]),
            params={
                "request-params": {"master_timeout": 20},
                "only-if-exists": False,
            },
        )

        assert source.params() == {
            "request-params": {"master_timeout": 20},
            "only-if-exists": False,
            "templates": [
                ("metrics", True, "metrics-*"),
                ("logs", False, "logs-*"),
            ],
        }


class TestCreateComposableTemplateParamSource:
    def test_create_index_template_inline(self):
        source = params.CreateComposableTemplateParamSource(
            track=track.Track(name="unit-test"),
            params={
                "template": "test",
                "body": {
                    "index_patterns": ["my*"],
                    "template": {"settings": {"index.number_of_shards": 3}},
                    "composed_of": ["ct1", "ct2"],
                },
            },
        )

        assert source.params() == {
            "request-params": {},
            "templates": [
                (
                    "test",
                    {
                        "index_patterns": ["my*"],
                        "template": {"settings": {"index.number_of_shards": 3}},
                        "composed_of": ["ct1", "ct2"],
                    },
                )
            ],
        }

    def test_create_composable_index_template_from_track(self):
        tpl = track.IndexTemplate(
            name="default",
            pattern="*",
            content={
                "index_patterns": ["my*"],
                "template": {"settings": {"index.number_of_shards": 3}},
                "composed_of": ["ct1", "ct2"],
            },
        )

        source = params.CreateComposableTemplateParamSource(
            track=track.Track(name="unit-test", composable_templates=[tpl]),
            params={
                "settings": {"index.number_of_replicas": 1},
            },
        )

        assert source.params() == {
            "request-params": {},
            "templates": [
                (
                    "default",
                    {
                        "index_patterns": ["my*"],
                        "template": {
                            "settings": {
                                "index.number_of_shards": 3,
                                "index.number_of_replicas": 1,
                            }
                        },
                        "composed_of": ["ct1", "ct2"],
                    },
                )
            ],
        }

    def test_create_composable_index_template_from_track_wrong_filter(self):
        t1 = track.IndexTemplate(
            name="t1",
            content={},
            pattern="",
        )
        t2 = track.IndexTemplate(
            name="t2",
            content={},
            pattern="",
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.CreateComposableTemplateParamSource(
                track=track.Track(name="unit-test", composable_templates=[t1, t2]),
                params={
                    "template": "t3",
                },
            )
        assert exc.value.args[0] == ("Unknown template: t3. Available templates: t1, t2.")

    def test_create_composable_index_template_from_track_no_template(self):
        tpl = track.IndexTemplate(
            name="default",
            pattern="*",
            content={
                "index_patterns": ["my*"],
                "composed_of": ["ct1", "ct2"],
            },
        )

        source = params.CreateComposableTemplateParamSource(
            track=track.Track(name="unit-test", composable_templates=[tpl]),
            params={
                "settings": {"index.number_of_replicas": 1},
            },
        )

        assert source.params() == {
            "request-params": {},
            "templates": [
                (
                    "default",
                    {
                        "index_patterns": ["my*"],
                        "template": {
                            "settings": {
                                "index.number_of_replicas": 1,
                            }
                        },
                        "composed_of": ["ct1", "ct2"],
                    },
                )
            ],
        }

    def test_create_or_merge(self):
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {"parent": {}},
            ["parent", "child", "grandchild"],
            {"name": "Mike"},
        )
        assert content["parent"]["child"]["grandchild"]["name"] == "Mike"
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {"parent": {"child": {}}},
            ["parent", "child", "grandchild"],
            {"name": "Mike"},
        )
        assert content["parent"]["child"]["grandchild"]["name"] == "Mike"
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {"parent": {"child": {"grandchild": {}}}},
            ["parent", "child", "grandchild"],
            {"name": "Mike"},
        )
        assert content["parent"]["child"]["grandchild"]["name"] == "Mike"
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {
                "parent": {
                    "child": {
                        "name": "Mary",
                        "grandchild": {"name": "Dale", "age": 38},
                    },
                },
            },
            ["parent", "child", "grandchild"],
            {"name": "Mike"},
        )
        assert content["parent"]["child"]["name"] == "Mary"
        assert content["parent"]["child"]["grandchild"]["name"] == "Mike"
        assert content["parent"]["child"]["grandchild"]["age"] == 38
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {
                "parent": {
                    "child": {
                        "name": "Mary",
                        "grandchild": {"name": {"first": "Dale", "last": "Smith"}, "age": 38},
                    },
                },
            },
            ["parent", "child", "grandchild"],
            {"name": "Mike"},
        )
        assert content["parent"]["child"]["grandchild"]["name"] == "Mike"
        assert content["parent"]["child"]["grandchild"]["age"] == 38
        content = params.CreateComposableTemplateParamSource._create_or_merge(
            {
                "parent": {
                    "child": {
                        "name": "Mary",
                        "grandchild": {"name": {"first": "Dale", "last": "Smith"}, "age": 38},
                    },
                },
            },
            ["parent", "child", "grandchild"],
            {"name": {"first": "Mike"}},
        )
        assert content["parent"]["child"]["grandchild"]["name"]["first"] == "Mike"
        assert content["parent"]["child"]["grandchild"]["name"]["last"] == "Smith"

    def test_no_templates_specified(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.CreateComposableTemplateParamSource(
                track=track.Track(name="unit-test"),
                params={
                    "settings": {
                        "index.number_of_shards": 1,
                        "index.number_of_replicas": 1,
                    },
                    "operation-type": "create-composable-template",
                },
            )
        assert exc.value.args[0] == (
            "Please set the properties 'template' and 'body' for the create-composable-template operation "
            "or declare composable and/or component templates in the track"
        )


class TestCreateComponentTemplateParamSource:
    def test_create_component_index_template_from_track(self):
        tpl = track.ComponentTemplate(
            name="default",
            content={
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {"type": "date"},
                        },
                    },
                },
            },
        )

        source = params.CreateComponentTemplateParamSource(
            track=track.Track(name="unit-test", component_templates=[tpl]),
            params={
                "settings": {
                    "index.number_of_shards": 1,
                    "index.number_of_replicas": 1,
                },
            },
        )

        assert source.params() == {
            "request-params": {},
            "templates": [
                (
                    "default",
                    {
                        "template": {
                            "settings": {
                                "index.number_of_shards": 1,
                                "index.number_of_replicas": 1,
                            },
                            "mappings": {
                                "properties": {
                                    "@timestamp": {"type": "date"},
                                },
                            },
                        }
                    },
                )
            ],
        }

    def test_create_component_index_template_from_track_wrong_filter(self):
        t1 = track.ComponentTemplate(
            name="t1",
            content={},
        )
        t2 = track.ComponentTemplate(
            name="t2",
            content={},
        )

        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.CreateComponentTemplateParamSource(
                track=track.Track(name="unit-test", component_templates=[t1, t2]),
                params={
                    "template": "t3",
                },
            )
        assert exc.value.args[0] == ("Unknown template: t3. Available templates: t1, t2.")


class TestDeleteComponentTemplateParamSource:
    def test_delete_component_template_by_name(self):
        source = params.DeleteComponentTemplateParamSource(
            track.Track(name="unit-test"),
            params={"template": "default"},
        )

        assert source.params() == {
            "templates": ["default"],
            "only-if-exists": True,
            "request-params": {},
        }

    def test_no_component_templates(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.DeleteComponentTemplateParamSource(
                track.Track(name="unit-test"),
                params={"operation-type": "delete-component-template"},
            )
        assert exc.value.args[0] == "Please set the property 'template' for the delete-component-template operation."

    def test_delete_component_template_from_track(self):
        tpl1 = track.ComponentTemplate(
            name="logs",
            content={
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {"type": "date"},
                        },
                    },
                },
            },
        )
        tpl2 = track.ComponentTemplate(
            name="metrics",
            content={
                "template": {
                    "settings": {
                        "index.number_of_shards": 1,
                        "index.number_of_replicas": 1,
                    },
                },
            },
        )
        source = params.DeleteComponentTemplateParamSource(
            track.Track(name="unit-test", component_templates=[tpl1, tpl2]),
            params={"request-params": {"master_timeout": 20}, "only-if-exists": False},
        )

        p = source.params()

        assert len(p["templates"]) == 2
        assert p["templates"][0] == "logs"
        assert p["templates"][1] == "metrics"
        assert not p["only-if-exists"]
        assert p["request-params"] == {"master_timeout": 20}

        # test filtering
        source = params.DeleteComponentTemplateParamSource(
            track.Track(name="unit-test", component_templates=[tpl1, tpl2]),
            params={"template": "logs"},
        )

        p = source.params()

        assert len(p["templates"]) == 1
        assert p["templates"][0] == "logs"


class TestDeleteComposableTemplateParamSource:
    def test_delete_composable_template_by_name(self):
        source = params.DeleteComposableTemplateParamSource(
            track.Track(name="unit-test"),
            params={"template": "default"},
        )
        assert source.params() == {
            "template": "default",
            "templates": [("default", False, None)],
            "only-if-exists": True,
            "request-params": {},
        }

    def test_no_composable_templates(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.DeleteComponentTemplateParamSource(
                track.Track(name="unit-test"),
                params={"operation-type": "delete-composable-template"},
            )
        assert exc.value.args[0] == "Please set the property 'template' for the delete-composable-template operation."

    def test_delete_composable_template_from_track(self):
        tpl1 = track.IndexTemplate(
            name="logs",
            pattern="logs-*",
            content={
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {"type": "date"},
                        },
                    },
                },
            },
        )
        tpl2 = track.IndexTemplate(
            name="metrics",
            pattern="metrics-*",
            content={
                "template": {
                    "settings": {
                        "index.number_of_shards": 1,
                        "index.number_of_replicas": 1,
                    },
                },
            },
        )
        source = params.DeleteComposableTemplateParamSource(
            track.Track(name="unit-test", composable_templates=[tpl1, tpl2]),
            params={"request-params": {"master_timeout": 20}, "only-if-exists": False},
        )

        p = source.params()

        assert len(p["templates"]) == 2
        assert p["templates"][0][0] == "logs"
        assert p["templates"][1][0] == "metrics"
        assert not p["only-if-exists"]
        assert p["request-params"] == {"master_timeout": 20}

        # test filtering
        source = params.DeleteComposableTemplateParamSource(
            track.Track(name="unit-test", composable_templates=[tpl1, tpl2]),
            params={"template": "logs"},
        )

        p = source.params()

        assert len(p["templates"]) == 1
        assert p["templates"][0][0] == "logs"


class TestSearchParamSource:
    def test_passes_cache(self):
        index1 = track.Index(name="index1", types=["type1"])

        source = params.SearchParamSource(
            track=track.Track(name="unit-test", indices=[index1]),
            params={
                "body": {
                    "query": {
                        "match_all": {},
                    },
                },
                "headers": {"header1": "value1"},
                "cache": True,
            },
        )

        assert source.params() == {
            "index": "index1",
            "type": None,
            "request-timeout": None,
            "opaque-id": None,
            "headers": {"header1": "value1"},
            "request-params": {},
            "cache": True,
            "response-compression-enabled": True,
            "detailed-results": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

    def test_uses_data_stream(self):
        ds1 = track.DataStream(name="data-stream-1")

        source = params.SearchParamSource(
            track=track.Track(name="unit-test", data_streams=[ds1]),
            params={
                "body": {
                    "query": {
                        "match_all": {},
                    },
                },
                "request-timeout": 1.0,
                "headers": {"header1": "value1", "header2": "value2"},
                "opaque-id": "12345abcde",
                "cache": True,
            },
        )

        assert source.params() == {
            "index": "data-stream-1",
            "type": None,
            "request-timeout": 1.0,
            "headers": {"header1": "value1", "header2": "value2"},
            "opaque-id": "12345abcde",
            "request-params": {},
            "cache": True,
            "response-compression-enabled": True,
            "detailed-results": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

    def test_create_without_index(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            params.SearchParamSource(
                track=track.Track(name="unit-test"),
                params={
                    "type": "type1",
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
                operation_name="test_operation",
            )

        assert exc.value.args[0] == "'index' or 'data-stream' is mandatory and is missing for operation 'test_operation'"

    def test_passes_request_parameters(self):
        index1 = track.Index(name="index1", types=["type1"])

        source = params.SearchParamSource(
            track=track.Track(name="unit-test", indices=[index1]),
            params={
                "request-params": {"_source_include": "some_field"},
                "body": {
                    "query": {
                        "match_all": {},
                    },
                },
            },
        )

        assert source.params() == {
            "index": "index1",
            "type": None,
            "request-timeout": None,
            "opaque-id": None,
            "headers": None,
            "request-params": {"_source_include": "some_field"},
            "cache": None,
            "response-compression-enabled": True,
            "detailed-results": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

    def test_user_specified_index_overrides_defaults(self):
        index1 = track.Index(name="index1", types=["type1"])

        source = params.SearchParamSource(
            track=track.Track(name="unit-test", indices=[index1]),
            params={
                "index": "_all",
                "type": "type1",
                "cache": False,
                "response-compression-enabled": False,
                "detailed-results": True,
                "opaque-id": "12345abcde",
                "body": {
                    "query": {
                        "match_all": {},
                    },
                },
            },
        )

        assert source.params() == {
            "index": "_all",
            "type": "type1",
            "request-timeout": None,
            "opaque-id": "12345abcde",
            "headers": None,
            "request-params": {},
            "cache": False,
            "response-compression-enabled": False,
            "detailed-results": True,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

    def test_user_specified_data_stream_overrides_defaults(self):
        ds1 = track.DataStream(name="data-stream-1")

        source = params.SearchParamSource(
            track=track.Track(name="unit-test", data_streams=[ds1]),
            params={
                "data-stream": "data-stream-2",
                "cache": False,
                "response-compression-enabled": False,
                "request-timeout": 1.0,
                "body": {
                    "query": {
                        "match_all": {},
                    },
                },
            },
        )

        assert source.params() == {
            "index": "data-stream-2",
            "type": None,
            "request-timeout": 1.0,
            "opaque-id": None,
            "headers": None,
            "request-params": {},
            "cache": False,
            "response-compression-enabled": False,
            "detailed-results": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

    def test_invalid_data_stream_with_type(self):
        with pytest.raises(exceptions.InvalidSyntax) as exc:
            ds1 = track.DataStream(name="data-stream-1")

            params.SearchParamSource(
                track=track.Track(name="unit-test", data_streams=[ds1]),
                params={
                    "data-stream": "data-stream-2",
                    "type": "_doc",
                    "cache": False,
                    "response-compression-enabled": False,
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
                operation_name="test_operation",
            )

        assert exc.value.args[0] == "'type' not supported with 'data-stream' for operation 'test_operation'"

    def test_assertions_without_detailed_results_are_invalid(self):
        index1 = track.Index(name="index1", types=["type1"])
        with pytest.raises(exceptions.InvalidSyntax, match=r"The property \[detailed-results\] must be \[true\] if assertions are defined"):
            params.SearchParamSource(
                track=track.Track(name="unit-test", indices=[index1]),
                params={
                    "index": "_all",
                    # unset!
                    # "detailed-results": True,
                    "assertions": [{"property": "hits", "condition": ">", "value": 0}],
                    "body": {
                        "query": {
                            "match_all": {},
                        },
                    },
                },
            )


class TestForceMergeParamSource:
    def test_force_merge_index_from_track(self):
        source = params.ForceMergeParamSource(
            track.Track(
                name="unit-test",
                indices=[
                    track.Index(name="index1"),
                    track.Index(name="index2"),
                    track.Index(name="index3"),
                ],
            ),
            params={},
        )

        p = source.params()
        assert p["index"] == "index1,index2,index3"
        assert p["mode"] == "blocking"

    def test_force_merge_data_stream_from_track(self):
        source = params.ForceMergeParamSource(
            track.Track(
                name="unit-test",
                data_streams=[
                    track.DataStream(name="data-stream-1"),
                    track.DataStream(name="data-stream-2"),
                    track.DataStream(name="data-stream-3"),
                ],
            ),
            params={},
        )

        p = source.params()
        assert p["index"] == "data-stream-1,data-stream-2,data-stream-3"
        assert p["mode"] == "blocking"

    def test_force_merge_index_by_name(self):
        source = params.ForceMergeParamSource(
            track.Track(name="unit-test"),
            params={"index": "index2"},
        )

        p = source.params()
        assert p["index"] == "index2"
        assert p["mode"] == "blocking"

    def test_force_merge_by_data_stream_name(self):
        source = params.ForceMergeParamSource(
            track.Track(name="unit-test"),
            params={"data-stream": "data-stream-2"},
        )

        p = source.params()
        assert p["index"] == "data-stream-2"
        assert p["mode"] == "blocking"

    def test_default_force_merge_index(self):
        source = params.ForceMergeParamSource(
            track.Track(name="unit-test"),
            params={},
        )

        p = source.params()
        assert p["index"] == "_all"
        assert p["mode"] == "blocking"

    def test_force_merge_all_params(self):
        source = params.ForceMergeParamSource(
            track.Track(name="unit-test"),
            params={
                "index": "index2",
                "request-timeout": 30,
                "max-num-segments": 1,
                "polling-period": 20,
                "mode": "polling",
            },
        )

        p = source.params()
        assert p["index"] == "index2"
        assert p["request-timeout"] == 30
        assert p["max-num-segments"] == 1
        assert p["mode"] == "polling"


class TestDownsampleParamSource:
    def test_downsample_all_params(self):
        source = params.DownsampleParamSource(
            track.Track(name="unit-test"),
            params={
                "source-index": "test-source-index",
                "target-index": "test-target-index",
                "fixed-interval": "1m",
            },
        )

        p = source.params()
        assert p["fixed-interval"] == "1m"
        assert p["source-index"] == "test-source-index"
        assert p["target-index"] == "test-target-index"

    def test_downsample_default_index_param(self):
        source = params.DownsampleParamSource(
            track.Track(
                name="unit-test",
                indices=[track.Index(name="test-source-index", body="index.json")],
            ),
            params={
                "fixed-interval": "1m",
                "target-index": "test-target-index",
            },
        )

        p = source.params()
        assert p["fixed-interval"] == "1m"
        assert p["source-index"] == "test-source-index"
        assert p["target-index"] == "test-target-index"

    def test_downsample_source_index_override_default_index_param(self):
        source = params.DownsampleParamSource(
            track.Track(
                name="unit-test",
                indices=[track.Index(name="test-source-index", body="index.json")],
            ),
            params={
                "source-index": "another-index",
                "fixed-interval": "1m",
                "target-index": "test-target-index",
            },
        )

        p = source.params()
        assert p["fixed-interval"] == "1m"
        assert p["source-index"] == "another-index"
        assert p["target-index"] == "test-target-index"

    def test_downsample_empty_params(self):
        source = params.DownsampleParamSource(
            track.Track(name="unit-test"),
            params={},
        )

        p = source.params()
        assert p["fixed-interval"] == "1h"
        assert p["target-index"] == f"{p['source-index']}-{p['fixed-interval']}"
