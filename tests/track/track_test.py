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

from unittest import TestCase

from esrally import exceptions
from esrally.track import track


class TrackTests(TestCase):
    def test_finds_default_challenge(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertEqual(default_challenge,
                         track.Track(name="unittest",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .default_challenge)

    def test_default_challenge_none_if_no_challenges(self):
        self.assertIsNone(track.Track(name="unittest",
                                      description="unittest track",
                                      challenges=[])
                          .default_challenge)

    def test_finds_challenge_by_name(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertEqual(another_challenge,
                         track.Track(name="unittest",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .find_challenge_or_default("other"))

    def test_uses_default_challenge_if_no_name_given(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        self.assertEqual(default_challenge,
                         track.Track(name="unittest",
                                     description="unittest track",
                                     challenges=[another_challenge, default_challenge])
                         .find_challenge_or_default(""))

    def test_does_not_find_unknown_challenge(self):
        default_challenge = track.Challenge("default", description="default challenge", default=True)
        another_challenge = track.Challenge("other", description="non-default challenge", default=False)

        with self.assertRaises(exceptions.InvalidName) as ctx:
            track.Track(name="unittest",
                        description="unittest track",
                        challenges=[another_challenge, default_challenge]).find_challenge_or_default("unknown-name")

        self.assertEqual("Unknown challenge [unknown-name] for track [unittest]", ctx.exception.args[0])


class IndexTests(TestCase):
    def test_matches_exactly(self):
        self.assertTrue(track.Index("test").matches("test"))
        self.assertFalse(track.Index("test").matches(" test"))

    def test_matches_if_no_pattern_is_defined(self):
        self.assertTrue(track.Index("test").matches(pattern=None))

    def test_matches_if_catch_all_pattern_is_defined(self):
        self.assertTrue(track.Index("test").matches(pattern="*"))
        self.assertTrue(track.Index("test").matches(pattern="_all"))

    def test_str(self):
        self.assertEqual("test", str(track.Index("test")))


class DataStreamTests(TestCase):
    def test_matches_exactly(self):
        self.assertTrue(track.DataStream("test").matches("test"))
        self.assertFalse(track.DataStream("test").matches(" test"))

    def test_matches_if_no_pattern_is_defined(self):
        self.assertTrue(track.DataStream("test").matches(pattern=None))

    def test_matches_if_catch_all_pattern_is_defined(self):
        self.assertTrue(track.DataStream("test").matches(pattern="*"))
        self.assertTrue(track.DataStream("test").matches(pattern="_all"))

    def test_str(self):
        self.assertEqual("test", str(track.DataStream("test")))


class DocumentCorpusTests(TestCase):
    def test_do_not_filter(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
            track.Documents(source_format="other", number_of_documents=6, target_index="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7, target_index="logs-03"),
            track.Documents(source_format=None, number_of_documents=8, target_index=None)
        ], meta_data={
            "average-document-size-in-bytes": 12
        })

        filtered_corpus = corpus.filter()

        self.assertEqual(corpus.name, filtered_corpus.name)
        self.assertListEqual(corpus.documents, filtered_corpus.documents)
        self.assertDictEqual(corpus.meta_data, filtered_corpus.meta_data)

    def test_filter_documents_by_format(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
            track.Documents(source_format="other", number_of_documents=6, target_index="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7, target_index="logs-03"),
            track.Documents(source_format=None, number_of_documents=8, target_index=None)
        ])

        filtered_corpus = corpus.filter(source_format=track.Documents.SOURCE_FORMAT_BULK)

        self.assertEqual("test", filtered_corpus.name)
        self.assertEqual(2, len(filtered_corpus.documents))
        self.assertEqual("logs-01", filtered_corpus.documents[0].target_index)
        self.assertEqual("logs-03", filtered_corpus.documents[1].target_index)

    def test_filter_documents_by_indices(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
            track.Documents(source_format="other", number_of_documents=6, target_index="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7, target_index="logs-03"),
            track.Documents(source_format=None, number_of_documents=8, target_index=None)
        ])

        filtered_corpus = corpus.filter(target_indices=["logs-02"])

        self.assertEqual("test", filtered_corpus.name)
        self.assertEqual(1, len(filtered_corpus.documents))
        self.assertEqual("logs-02", filtered_corpus.documents[0].target_index)

    def test_filter_documents_by_data_streams(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5,
                            target_data_stream="logs-01"),
            track.Documents(source_format="other", number_of_documents=6, target_data_stream="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7,
                            target_data_stream="logs-03"),
            track.Documents(source_format=None, number_of_documents=8, target_data_stream=None)
        ])

        filtered_corpus = corpus.filter(target_data_streams=["logs-02"])
        self.assertEqual("test", filtered_corpus.name)
        self.assertEqual(1, len(filtered_corpus.documents))
        self.assertEqual("logs-02", filtered_corpus.documents[0].target_data_stream)

    def test_filter_documents_by_format_and_indices(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=6, target_index="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7, target_index="logs-03"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=8, target_index=None)
        ])

        filtered_corpus = corpus.filter(source_format=track.Documents.SOURCE_FORMAT_BULK, target_indices=["logs-01", "logs-02"])

        self.assertEqual("test", filtered_corpus.name)
        self.assertEqual(2, len(filtered_corpus.documents))
        self.assertEqual("logs-01", filtered_corpus.documents[0].target_index)
        self.assertEqual("logs-02", filtered_corpus.documents[1].target_index)

    def test_union_document_corpus_is_reflexive(self):
        corpus = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=6, target_index="logs-02"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=7, target_index="logs-03"),
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=8, target_index=None)
        ])
        self.assertTrue(corpus.union(corpus) is corpus)

    def test_union_document_corpora_is_symmetric(self):
        a = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
        ])
        b = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-02"),
        ])
        self.assertEqual(b.union(a), a.union(b))
        self.assertEqual(2, len(a.union(b).documents))

    def test_cannot_union_mixed_document_corpora_by_name(self):
        a = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
        ])
        b = track.DocumentCorpus("other", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-02"),
        ])
        with self.assertRaises(exceptions.RallyAssertionError) as ae:
            a.union(b)
        self.assertEqual(ae.exception.message, "Corpora names differ: [test] and [other].")

    def test_cannot_union_mixed_document_corpora_by_meta_data(self):
        a = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-01"),
        ], meta_data={
            "with-metadata": False
        })
        b = track.DocumentCorpus("test", documents=[
            track.Documents(source_format=track.Documents.SOURCE_FORMAT_BULK, number_of_documents=5, target_index="logs-02"),
        ], meta_data={
            "with-metadata": True
        })
        with self.assertRaises(exceptions.RallyAssertionError) as ae:
            a.union(b)
        self.assertEqual(ae.exception.message,
                         "Corpora meta-data differ: [{'with-metadata': False}] and [{'with-metadata': True}].")


class OperationTypeTests(TestCase):
    def test_string_hyphenation_is_symmetric(self):
        for op_type in track.OperationType:
            self.assertEqual(op_type, track.OperationType.from_hyphenated_string(op_type.to_hyphenated_string()))


class TaskFilterTests(TestCase):
    def create_index_task(self):
        return track.Task("create-index-task",
                          track.Operation("create-index-op",
                                          operation_type=track.OperationType.CreateIndex.to_hyphenated_string()),
                          tags=["write-op", "admin-op"])

    def search_task(self):
        return track.Task("search-task",
                          track.Operation("search-op",
                                          operation_type=track.OperationType.Search.to_hyphenated_string()),
                          tags="read-op")

    def test_task_name_filter(self):
        f = track.TaskNameFilter("create-index-task")
        self.assertTrue(f.matches(self.create_index_task()))
        self.assertFalse(f.matches(self.search_task()))

    def test_task_op_type_filter(self):
        f = track.TaskOpTypeFilter(track.OperationType.CreateIndex.to_hyphenated_string())
        self.assertTrue(f.matches(self.create_index_task()))
        self.assertFalse(f.matches(self.search_task()))

    def test_task_tag_filter(self):
        f = track.TaskTagFilter(tag_name="write-op")
        self.assertTrue(f.matches(self.create_index_task()))
        self.assertFalse(f.matches(self.search_task()))


class TaskTests(TestCase):
    def task(self, schedule=None, target_throughput=None, target_interval=None, ignore_response_error_level=None):
        op = track.Operation("bulk-index", track.OperationType.Bulk.to_hyphenated_string())
        params = {}
        if target_throughput is not None:
            params["target-throughput"] = target_throughput
        if target_interval is not None:
            params["target-interval"] = target_interval
        if ignore_response_error_level is not None:
            params["ignore-response-error-level"] = ignore_response_error_level
        return track.Task("test", op, schedule=schedule, params=params)

    def test_unthrottled_task(self):
        task = self.task()
        self.assertIsNone(task.target_throughput)

    def test_target_interval_zero_treated_as_unthrottled(self):
        task = self.task(target_interval=0)
        self.assertIsNone(task.target_throughput)

    def test_valid_throughput_with_unit(self):
        task = self.task(target_throughput="5 MB/s")
        self.assertEqual(track.Throughput(5.0, "MB/s"), task.target_throughput)

    def test_valid_throughput_numeric(self):
        task = self.task(target_throughput=3.2)
        self.assertEqual(track.Throughput(3.2, "ops/s"), task.target_throughput)

    def test_invalid_throughput_format_is_rejected(self):
        task = self.task(target_throughput="3.2 docs")
        with self.assertRaises(exceptions.InvalidSyntax) as e:
            # pylint: disable=pointless-statement
            task.target_throughput
        self.assertEqual("Task [test] specifies invalid target throughput [3.2 docs].", e.exception.args[0])

    def test_invalid_throughput_type_is_rejected(self):
        task = self.task(target_throughput=True)
        with self.assertRaises(exceptions.InvalidSyntax) as e:
            # pylint: disable=pointless-statement
            task.target_throughput
        self.assertEqual("Target throughput [True] for task [test] must be string or numeric.", e.exception.args[0])

    def test_interval_and_throughput_is_rejected(self):
        task = self.task(target_throughput=1, target_interval=1)
        with self.assertRaises(exceptions.InvalidSyntax) as e:
            # pylint: disable=pointless-statement
            task.target_throughput
        self.assertEqual("Task [test] specifies target-interval [1] and target-throughput [1] but only one "
                         "of them is allowed.", e.exception.args[0])

    def test_invalid_ignore_response_error_level_is_rejected(self):
        task = self.task(ignore_response_error_level="invalid-value")
        with self.assertRaises(exceptions.InvalidSyntax) as e:
            # pylint: disable=pointless-statement
            task.ignore_response_error_level
        self.assertEqual("Task [test] specifies ignore-response-error-level to [invalid-value] but "
                         "the only allowed values are [non-fatal].", e.exception.args[0])

    def test_task_continues_with_global_continue(self):
        task = self.task()
        effective_on_error = task.error_behavior(default_error_behavior="continue")
        self.assertEqual(effective_on_error, "continue")

    def test_task_continues_with_global_abort_and_task_override(self):
        task = self.task(ignore_response_error_level="non-fatal")
        effective_on_error = task.error_behavior(default_error_behavior="abort")
        self.assertEqual(effective_on_error, "continue")

    def test_task_aborts_with_global_abort(self):
        task = self.task()
        effective_on_error = task.error_behavior(default_error_behavior="abort")
        self.assertEqual(effective_on_error, "abort")
