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

import io
import random
import unittest.mock as mock
from unittest import TestCase

import elasticsearch
import pytest

from esrally import exceptions
from esrally.driver import runner


class BaseUnitTestContextManagerRunner:
    def __enter__(self):
        self.fp = io.StringIO("many\nlines\nin\na\nfile")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.fp.close()
        return False


class RegisterRunnerTests(TestCase):
    def tearDown(self):
        runner.remove_runner("unit_test")

    def test_runner_function_should_be_wrapped(self):
        def runner_function(*args):
            return args

        runner.register_runner(operation_type="unit_test", runner=runner_function)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [runner_function]", repr(returned_runner))
        self.assertEqual(("default_client", "param"), returned_runner({"default": "default_client", "other": "other_client"}, "param"))

    def test_single_cluster_runner_class_with_context_manager_should_be_wrapped_with_context_manager_enabled(self):
        class UnitTestSingleClusterContextManagerRunner(BaseUnitTestContextManagerRunner):
            def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterContextManagerRunner"

        test_runner = UnitTestSingleClusterContextManagerRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined context-manager enabled runner for [UnitTestSingleClusterContextManagerRunner]",
                         repr(returned_runner))
        # test that context_manager functionality gets preserved after wrapping
        with returned_runner:
            self.assertEqual(("default_client", "param"), returned_runner({"default": "default_client", "other": "other_client"}, "param"))
        # check that the context manager interface of our inner runner has been respected.
        self.assertTrue(test_runner.fp.closed)

    def test_multi_cluster_runner_class_with_context_manager_should_be_wrapped_with_context_manager_enabled(self):
        class UnitTestMultiClusterContextManagerRunner(BaseUnitTestContextManagerRunner):
            multi_cluster = True

            def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestMultiClusterContextManagerRunner"

        test_runner = UnitTestMultiClusterContextManagerRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined context-manager enabled runner for [UnitTestMultiClusterContextManagerRunner]",
                         repr(returned_runner))

        # test that context_manager functionality gets preserved after wrapping
        all_clients = {"default": "default_client", "other": "other_client"}
        with returned_runner:
            self.assertEqual((all_clients, "param1", "param2"), returned_runner(all_clients, "param1", "param2"))
        # check that the context manager interface of our inner runner has been respected.
        self.assertTrue(test_runner.fp.closed)

    def test_single_cluster_runner_class_should_be_wrapped(self):
        class UnitTestSingleClusterRunner:
            def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterRunner"

        test_runner = UnitTestSingleClusterRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [UnitTestSingleClusterRunner]", repr(returned_runner))
        self.assertEqual(("default_client", "param"), returned_runner({"default": "default_client", "other": "other_client"}, "param"))

    def test_multi_cluster_runner_class_should_be_wrapped(self):
        class UnitTestMultiClusterRunner:
            multi_cluster = True

            def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestMultiClusterRunner"

        test_runner = UnitTestMultiClusterRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [UnitTestMultiClusterRunner]", repr(returned_runner))
        all_clients = {"default": "default_client", "other": "other_client"}
        self.assertEqual((all_clients, "some_param"), returned_runner(all_clients, "some_param"))


class BulkIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_missing_params(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n"
        }

        with self.assertRaises(exceptions.DataError) as ctx:
            bulk(es, bulk_params)
        self.assertEqual("Parameter source for operation 'bulk-index' did not provide the mandatory parameter 'action-metadata-present'. "
                         "Please add it to your parameter source.", ctx.exception.args[0])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_success_with_metadata(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n",
            "action-metadata-present": True,
            "bulk-size": 3
        }

        result = bulk(es, bulk_params)

        self.assertIsNone(result["took"])
        self.assertIsNone(result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_success_without_metadata_with_doc_type(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "index_line\n" +
                    "index_line\n" +
                    "index_line\n",
            "action-metadata-present": False,
            "bulk-size": 3,
            "index": "test-index",
            "type": "_doc"
        }

        result = bulk(es, bulk_params)

        self.assertIsNone(result["took"])
        self.assertEqual("test-index", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], index="test-index", doc_type="_doc", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_success_without_metadata_and_without_doc_type(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "index_line\n" +
                    "index_line\n" +
                    "index_line\n",
            "action-metadata-present": False,
            "bulk-size": 3,
            "index": "test-index"
        }

        result = bulk(es, bulk_params)

        self.assertIsNone(result["took"])
        self.assertEqual("test-index", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], index="test-index", doc_type=None, params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_error(self, es):
        es.bulk.return_value = {
            "took": 5,
            "errors": True,
            "items": [
                {
                    "index": {
                        "status": 201,
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        }
                    }
                },
                {
                    "index": {
                        "status": 500,
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        }
                    }
                },
                {
                    "index": {
                        "status": 404,
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        }
                    }
                },
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n",
            "action-metadata-present": True,
            "bulk-size": 3,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(5, result["took"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(2, result["error-count"])
        self.assertEqual("bulk", result["error-type"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_error_no_shards(self, es):
        es.bulk.return_value = {
            "took": 20,
            "errors": True,
            "items": [
                {
                    "create": {
                        "_index": "test",
                        "_type": "doc",
                        "_id": "1",
                        "status": 429,
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@1]" # pylint: disable=line-too-long
                    }
                },
                {
                    "create": {
                        "_index": "test",
                        "_type": "doc",
                        "_id": "2",
                        "status": 429,
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@2]" # pylint: disable=line-too-long
                    }
                },
                {
                    "create": {
                        "_index": "test",
                        "_type": "doc",
                        "_id": "3",
                        "status": 429,
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@3]" # pylint: disable=line-too-long
                    }
                }
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "index_line\n",
            "action-metadata-present": True,
            "detailed-results": False,
            "bulk-size": 3,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(20, result["took"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(3, result["error-count"])
        self.assertEqual("bulk", result["error-type"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_mixed_bulk_with_simple_stats(self, es):
        es.bulk.return_value = {
            "took": 30,
            "ingest_took": 20,
            "errors": True,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": True,
                        "status": 201,
                        "_seq_no": 0
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "2",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "status": 200,
                        "_seq_no": 1
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "3",
                        "_version": 1,
                        "result": "noop",
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        },
                        "created": False,
                        "status": 500,
                        "_seq_no": -2
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "6",
                        "_version": 2,
                        "result": "noop",
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        },
                        "status": 404,
                        "_seq_no": 5
                    }
                }
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "update_line\n" +
                    "action_meta_data\n" +
                    "index_line\n" +
                    "action_meta_data\n" +
                    "update_line\n",
            "action-metadata-present": True,
            "detailed-results": False,
            "bulk-size": 4,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(30, result["took"])
        self.assertEqual(20, result["ingest_took"])
        self.assertEqual(4, result["weight"])
        self.assertEqual(4, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(2, result["error-count"])
        self.assertEqual("bulk", result["error-type"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = bulk(es, bulk_params)
        self.assertNotIn("ingest_took", result)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_mixed_bulk_with_detailed_stats_body_as_string(self, es):
        es.bulk.return_value = {
            "took": 30,
            "ingest_took": 20,
            "errors": True,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": True,
                        "status": 201,
                        "_seq_no": 0
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "2",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "status": 200,
                        "_seq_no": 1
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "3",
                        "_version": 1,
                        "result": "noop",
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        },
                        "created": False,
                        "status": 500,
                        "_seq_no": -2
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "4",
                        "_version": 1,
                        "result": "noop",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 1
                        },
                        "created": False,
                        "status": 500,
                        "_seq_no": -2
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "5",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": True,
                        "status": 201,
                        "_seq_no": 4
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "6",
                        "_version": 2,
                        "result": "noop",
                        "_shards": {
                            "total": 2,
                            "successful": 0,
                            "failed": 2
                        },
                        "status": 404,
                        "_seq_no": 5
                    }
                }
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1485188, 51.5250666]}\n' +
                    '{ "update" : { "_index" : "test", "_type" : "type1", "_id: "2" } }\n' +
                    '{"location" : [-0.1479949, 51.5252071]}\n' +
                    '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1458559, 51.5289059]}\n' +
                    '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1498551, 51.5282564]}\n' +
                    '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1487043, 51.5254843]}\n' +
                    '{ "update" : { "_index" : "test", "_type" : "type1", "_id: "3" } }\n' +
                    '{"location" : [-0.1533367, 51.5261779]}\n',
            "action-metadata-present": True,
            "bulk-size": 6,
            "detailed-results": True,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(30, result["took"])
        self.assertEqual(20, result["ingest_took"])
        self.assertEqual(6, result["weight"])
        self.assertEqual(6, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(3, result["error-count"])
        self.assertEqual("bulk", result["error-type"])
        self.assertEqual(
            {
                "index": {
                    "item-count": 4,
                    "created": 2,
                    "noop": 2
                },

                "update": {
                    "item-count": 2,
                    "updated": 1,
                    "noop": 1
                }
            }, result["ops"])
        self.assertEqual(
            [
                {
                    "item-count": 3,
                    "shards": {
                        "total": 2,
                        "successful": 1,
                        "failed": 0
                    }
                },
                {
                    "item-count": 2,
                    "shards": {
                        "total": 2,
                        "successful": 0,
                        "failed": 2
                    }
                },
                {
                    "item-count": 1,
                    "shards": {
                        "total": 2,
                        "successful": 1,
                        "failed": 1
                    }
                }
            ], result["shards_histogram"])
        self.assertEqual(582, result["bulk-request-size-bytes"])
        self.assertEqual(234, result["total-document-size-bytes"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = bulk(es, bulk_params)
        self.assertNotIn("ingest_took", result)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_simple_bulk_with_detailed_stats_body_as_list(self, es):
        es.bulk.return_value = {
            "took": 30,
            "ingest_took": 20,
            "errors": False,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": True,
                        "status": 201,
                        "_seq_no": 0
                    }
                }
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1485188, 51.5250666]}\n',
            "action-metadata-present": True,
            "bulk-size": 1,
            "detailed-results": True,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(30, result["took"])
        self.assertEqual(20, result["ingest_took"])
        self.assertEqual(1, result["weight"])
        self.assertEqual(1, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertEqual(
            {
                "index": {
                    "item-count": 1,
                    "created": 1
                },
            }, result["ops"])
        self.assertEqual(
            [
                {
                    "item-count": 1,
                    "shards": {
                        "total": 2,
                        "successful": 1,
                        "failed": 0
                    }
                }
            ], result["shards_histogram"])
        self.assertEqual(93, result["bulk-request-size-bytes"])
        self.assertEqual(39, result["total-document-size-bytes"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = bulk(es, bulk_params)
        self.assertNotIn("ingest_took", result)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_simple_bulk_with_detailed_stats_body_as_unrecognized_type(self, es):
        es.bulk.return_value = {
            "took": 30,
            "ingest_took": 20,
            "errors": False,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 2,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": True,
                        "status": 201,
                        "_seq_no": 0
                    }
                }
            ]
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": {
                "items": '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                         '{"location" : [-0.1485188, 51.5250666]}\n',
            },
            "action-metadata-present": True,
            "bulk-size": 1,
            "detailed-results": True,
            "index": "test"
        }

        with self.assertRaisesRegex(exceptions.DataError, "bulk body is neither string nor list"):
            bulk(es, bulk_params)

        es.bulk.assert_called_with(body=bulk_params["body"], params={})


class ForceMergeRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_force_merge_with_defaults(self, es):
        force_merge = runner.ForceMerge()
        force_merge(es, params={"index" : "_all"})

        es.indices.forcemerge.assert_called_once_with(index="_all", request_timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_force_merge_override_request_timeout(self, es):
        force_merge = runner.ForceMerge()
        force_merge(es, params={"index" : "_all", "request-timeout": 50000})

        es.indices.forcemerge.assert_called_once_with(index="_all", request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_force_merge_with_params(self, es):
        force_merge = runner.ForceMerge()
        force_merge(es, params={"index" : "_all", "max-num-segments": 1, "request-timeout": 50000})

        es.indices.forcemerge.assert_called_once_with(index="_all", max_num_segments=1, request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_optimize_with_defaults(self, es):
        es.indices.forcemerge.side_effect = elasticsearch.TransportError(400, "Bad Request")

        force_merge = runner.ForceMerge()
        force_merge(es, params={})

        es.transport.perform_request.assert_called_once_with("POST", "/_optimize", params={"request_timeout": None})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_optimize_with_params(self, es):
        es.indices.forcemerge.side_effect = elasticsearch.TransportError(400, "Bad Request")
        force_merge = runner.ForceMerge()
        force_merge(es, params={"max-num-segments": 3, "request-timeout": 17000})

        es.transport.perform_request.assert_called_once_with("POST", "/_optimize?max_num_segments=3",
                                                             params={"request_timeout": 17000})


class QueryRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_match_only_request_body_defined(self, es):
        es.search.return_value = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 1,
                    "relation": "gte"
                },
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }

        query_runner = runner.Query()

        params = {
            "cache": True,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            result = query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(1, result["hits"])
        self.assertEqual("gte", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.search.assert_called_once_with(
            index="_all",
            body=params["body"],
            params={"request_cache": "true"}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_match_using_request_params(self, es):
        es.search.return_value = {
            "timed_out": False,
            "took": 62,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }

                ]
            }
        }

        query_runner = runner.Query()
        params = {
            "cache": False,
            "body": None,
            "request-params": {
                "q": "user:kimchy"
            }
        }

        with query_runner:
            result = query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(62, result["took"])
        self.assertFalse("error-type" in result)

        es.search.assert_called_once_with(
            index="_all",
            body=params["body"],
            params={
                "request_cache": "false",
                "q": "user:kimchy"
            }
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_hits_total_as_number(self, es):
        es.search.return_value = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": 2,
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }

        query_runner = runner.Query()

        params = {
            "cache": True,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            result = query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.search.assert_called_once_with(
            index="_all",
            body=params["body"],
            params={
                "request_cache": "true"
            }
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_match_all(self, es):
        es.search.return_value = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }

        query_runner = runner.Query()

        params = {
            "index": "unittest",
            "cache": None,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            result = query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.search.assert_called_once_with(
            index="unittest",
            body=params["body"],
            params={}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_match_all_doc_type_fallback(self, es):
        es.transport.perform_request.return_value = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }

        query_runner = runner.Query()

        params = {
            "index": "unittest",
            "type": "type",
            "cache": None,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            result = query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            'GET', '/unittest/type/_search',
            body=params["body"],
            params={}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_only_one_page(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.side_effect = [
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "cache": True,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.search.assert_called_once_with(
            index="unittest",
            body=params["body"],
            scroll="10s",
            size=100,
            sort='_doc',
            params={
                "request_cache": "true"
            }
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_no_request_cache(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.side_effect = [
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.search.assert_called_once_with(
            index="unittest",
            body=params["body"],
            scroll="10s",
            size=100,
            sort='_doc',
            params={}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_only_one_page_only_request_body_defined(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.side_effect = [
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": 1,
            "results-per-page": 100,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_with_explicit_number_of_pages(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 54,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    }
                ]
            }
        }
        es.scroll.side_effect = [
            # page 2
            {
                "_scroll_id": "some-scroll-id",
                "timed_out": True,
                "took": 25,
                "hits": {
                    "hits": [
                        {
                            "some-doc-3"
                        }
                    ]
                }
            },
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": 2,
            "results-per-page": 100,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(3, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(79, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertTrue(results["timed_out"])
        self.assertFalse("error-type" in results)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_early_termination(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 53,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    }
                ]
            }
        }
        es.scroll.side_effect = [
            # page 2 has no results
            {
                "_scroll_id": "some-scroll-id",
                "timed_out": False,
                "took": 2,
                "hits": {
                    "hits": []
                }
            },
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": 5,
            "results-per-page": 100,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(1, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual("pages", results["unit"])
        self.assertEqual(55, results["took"])
        self.assertFalse("error-type" in results)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_cannot_clear_scroll(self, es):
        import elasticsearch
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 53,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    }
                ]
            }
        }
        es.scroll.side_effect = [
            # page 2 has no results
            {
                "_scroll_id": "some-scroll-id",
                "timed_out": False,
                "took": 2,
                "hits": {
                    "hits": []
                }
            },
            # delete scroll id raises an exception
            elasticsearch.ConnectionTimeout()
        ]

        query_runner = runner.Query()

        params = {
            "pages": 5,
            "results-per-page": 100,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(1, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual("pages", results["unit"])
        self.assertEqual(55, results["took"])
        self.assertFalse("error-type" in results)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_request_all_pages(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 876,
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    },
                    {
                        "some-doc-2"
                    },
                    {
                        "some-doc-3"
                    },
                    {
                        "some-doc-4"
                    }
                ]
            }
        }
        es.scroll.side_effect = [
            # page 2 has no results
            {
                "_scroll_id": "some-scroll-id",
                "took": 24,
                "timed_out": False,
                "hits": {
                    "hits": []
                }
            },
            # delete scroll id response
            {
                "acknowledged": True
            }
        ]

        query_runner = runner.Query()

        params = {
            "pages": "all",
            "results-per-page": 100,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        with query_runner:
            results = query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(4, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(900, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)


class PutPipelineRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_pipeline(self, es):
        r = runner.PutPipeline()

        params = {
            "id": "rename",
            "body": {
                "description": "describe pipeline",
                "processors": [
                    {
                        "set": {
                            "field": "foo",
                            "value": "bar"
                        }
                    }
                ]
            }
        }

        r(es, params)

        es.ingest.put_pipeline.assert_called_once_with(id="rename", body=params["body"], master_timeout=None, timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_param_body_mandatory(self, es):
        r = runner.PutPipeline()

        params = {
            "id": "rename"
        }
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'body'. "
                                    "Please add it to your parameter source."):
            r(es, params)

        self.assertEqual(0, es.ingest.put_pipeline.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_param_id_mandatory(self, es):
        r = runner.PutPipeline()

        params = {
            "body": {}
        }
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'id'. "
                                    "Please add it to your parameter source."):
            r(es, params)

        self.assertEqual(0, es.ingest.put_pipeline.call_count)


class ClusterHealthRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_waits_for_expected_cluster_status(self, es):
        es.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0
        }
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "green"
            }
        }

        result = r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0
        }, result)

        es.cluster.health.assert_called_once_with(index=None, params={"wait_for_status": "green"})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_accepts_better_cluster_status(self, es):
        es.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0
        }
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "yellow"
            }
        }

        result = r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0
        }, result)

        es.cluster.health.assert_called_once_with(index=None, params={"wait_for_status": "yellow"})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_rejects_relocating_shards(self, es):
        es.cluster.health.return_value = {
            "status": "yellow",
            "relocating_shards": 3
        }
        r = runner.ClusterHealth()

        params = {
            "index": "logs-*",
            "request-params": {
                "wait_for_status": "red",
                "wait_for_no_relocating_shards": True
            }
        }

        result = r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": False,
            "cluster-status": "yellow",
            "relocating-shards": 3
        }, result)

        es.cluster.health.assert_called_once_with(index="logs-*",
                                                  params={"wait_for_status": "red", "wait_for_no_relocating_shards": True})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_rejects_unknown_cluster_status(self, es):
        es.cluster.health.return_value = {
            "status": None,
            "relocating_shards": 0
        }
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "green"
            }
        }

        result = r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": False,
            "cluster-status": None,
            "relocating-shards": 0
        }, result)

        es.cluster.health.assert_called_once_with(index=None, params={"wait_for_status": "green"})


class CreateIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_creates_multiple_indices(self, es):
        r = runner.CreateIndex()

        request_params = {
            "wait_for_active_shards": "true"
        }

        params = {
            "indices": [
                ("indexA", {"settings": {}}),
                ("indexB", {"settings": {}}),
            ],
            "request-params": request_params
        }

        result = r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.create.assert_has_calls([
            mock.call(index="indexA", body={"settings": {}}, params=request_params),
            mock.call(index="indexB", body={"settings": {}}, params=request_params)
        ])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_param_indices_mandatory(self, es):
        r = runner.CreateIndex()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'create-index' did not provide the mandatory parameter 'indices'. "
                                    "Please add it to your parameter source."):
            r(es, params)

        self.assertEqual(0, es.indices.create.call_count)


class DeleteIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_deletes_existing_indices(self, es):
        es.indices.exists.side_effect = [False, True]

        r = runner.DeleteIndex()

        params = {
            "indices": ["indexA", "indexB"],
            "only-if-exists": True
        }

        result = r(es, params)

        self.assertEqual((1, "ops"), result)

        es.indices.delete.assert_called_once_with(index="indexB", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_deletes_all_indices(self, es):
        r = runner.DeleteIndex()

        params = {
            "indices": ["indexA", "indexB"],
            "only-if-exists": False,
            "request-params": {
                "ignore_unavailable": "true",
                "expand_wildcards": "none"
            }
        }

        result = r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.delete.assert_has_calls([
            mock.call(index="indexA", params=params["request-params"]),
            mock.call(index="indexB", params=params["request-params"])
        ])
        self.assertEqual(0, es.indices.exists.call_count)


class CreateIndexTemplateRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_index_templates(self, es):
        r = runner.CreateIndexTemplate()

        params = {
            "templates": [
                ("templateA", {"settings": {}}),
                ("templateB", {"settings": {}}),
            ],
            "request-params": {
                "timeout": 50,
                "create": "true"
            }
        }

        result = r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.put_template.assert_has_calls([
            mock.call(name="templateA", body={"settings": {}}, params=params["request-params"]),
            mock.call(name="templateB", body={"settings": {}}, params=params["request-params"])
        ])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_param_templates_mandatory(self, es):
        r = runner.CreateIndexTemplate()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'create-index-template' did not provide the mandatory parameter "
                                    "'templates'. Please add it to your parameter source."):
            r(es, params)

        self.assertEqual(0, es.indices.put_template.call_count)


class DeleteIndexTemplateRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_deletes_all_index_templates(self, es):
        r = runner.DeleteIndexTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                ("templateB", True, "logs-*"),
            ],
            "request-params": {
                "timeout": 60
            }
        }
        result = r(es, params)

        # 2 times delete index template, one time delete matching indices
        self.assertEqual((3, "ops"), result)

        es.indices.delete_template.assert_has_calls([
            mock.call(name="templateA", params=params["request-params"]),
            mock.call(name="templateB", params=params["request-params"])
        ])
        es.indices.delete.assert_called_once_with(index="logs-*")

    @mock.patch("elasticsearch.Elasticsearch")
    def test_deletes_only_existing_index_templates(self, es):
        es.indices.exists_template.side_effect = [False, True]

        r = runner.DeleteIndexTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                # will not accidentally delete all indices
                ("templateB", True, ""),
            ],
            "request-params": {
                "timeout": 60
            },
            "only-if-exists": True
        }
        result = r(es, params)

        # 2 times delete index template, one time delete matching indices
        self.assertEqual((1, "ops"), result)

        es.indices.delete_template.assert_called_once_with(name="templateB", params=params["request-params"])
        # not called because the matching index is empty.
        self.assertEqual(0, es.indices.delete.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_param_templates_mandatory(self, es):
        r = runner.DeleteIndexTemplate()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'delete-index-template' did not provide the mandatory parameter "
                                    "'templates'. Please add it to your parameter source."):
            r(es, params)

        self.assertEqual(0, es.indices.delete_template.call_count)


class CreateMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_ml_datafeed(self, es):
        params = {
            "datafeed-id": "some-data-feed",
            "body": {
                "job_id": "total-requests",
                "indices": ["server-metrics"]
            }
        }

        r = runner.CreateMlDatafeed()
        r(es, params)

        es.xpack.ml.put_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_ml_datafeed_fallback(self, es):
        es.xpack.ml.put_datafeed.side_effect = elasticsearch.TransportError(400, "Bad Request")
        datafeed_id = "some-data-feed"
        body = {
                "job_id": "total-requests",
                "indices": ["server-metrics"]
            }
        params = {
            "datafeed-id": datafeed_id,
            "body": body
        }

        r = runner.CreateMlDatafeed()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("PUT",
                                                             "/_xpack/ml/datafeeds/%s" % datafeed_id,
                                                             body=body,
                                                             params=params)


class DeleteMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_delete_ml_datafeed(self, es):
        datafeed_id = "some-data-feed"
        params = {
            "datafeed-id": datafeed_id
        }

        r = runner.DeleteMlDatafeed()
        r(es, params)

        es.xpack.ml.delete_datafeed.assert_called_once_with(datafeed_id=datafeed_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_delete_ml_datafeed_fallback(self, es):
        es.xpack.ml.delete_datafeed.side_effect = elasticsearch.TransportError(400, "Bad Request")
        datafeed_id = "some-data-feed"
        params = {
            "datafeed-id": datafeed_id,
        }

        r = runner.DeleteMlDatafeed()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("DELETE",
                                                             "/_xpack/ml/datafeeds/%s" % datafeed_id,
                                                             params=params)


class StartMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_start_ml_datafeed_with_body(self, es):
        params = {
            "datafeed-id": "some-data-feed",
            "body": {
                "end": "now"
            }
        }

        r = runner.StartMlDatafeed()
        r(es, params)

        es.xpack.ml.start_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                           body=params["body"],
                                                           start=None,
                                                           end=None,
                                                           timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_start_ml_datafeed_with_body_fallback(self, es):
        es.xpack.ml.start_datafeed.side_effect = elasticsearch.TransportError(400, "Bad Request")
        body = {
                "end": "now"
            }
        params = {
            "datafeed-id": "some-data-feed",
            "body": body
        }

        r = runner.StartMlDatafeed()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             "/_xpack/ml/datafeeds/%s/_start" % params["datafeed-id"],
                                                             body=body,
                                                             params=params)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_start_ml_datafeed_with_params(self, es):
        params = {
            "datafeed-id": "some-data-feed",
            "start": "2017-01-01T01:00:00Z",
            "end": "now",
            "timeout": "10s"
        }

        r = runner.StartMlDatafeed()
        r(es, params)

        es.xpack.ml.start_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                           body=None,
                                                           start=params["start"],
                                                           end=params["end"],
                                                           timeout=params["timeout"])


class StopMlDatafeedTests:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.parametrize("seed", range(20))
    def test_stop_ml_datafeed(self, es, seed):
        random.seed(seed)
        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.StopMlDatafeed()
        r(es, params)

        es.xpack.ml.stop_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                          force=params["force"],
                                                          timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.parametrize("seed", range(20))
    def test_stop_ml_datafeed_fallback(self, es, seed):
        random.seed(seed)
        es.xpack.ml.stop_datafeed.side_effect = elasticsearch.TransportError(400, "Bad Request")
        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.StopMlDatafeed()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             "/_xpack/ml/datafeeds/%s/_stop" % params["datafeed-id"],
                                                             params=params)


class CreateMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_ml_job(self, es):
        params = {
            "job-id": "an-ml-job",
            "body": {
                "description": "Total sum of requests",
                "analysis_config": {
                    "bucket_span": "10m",
                    "detectors": [
                        {
                            "detector_description": "Sum of total",
                            "function": "sum",
                            "field_name": "total"
                        }
                    ]
                },
                "data_description": {
                    "time_field": "timestamp",
                    "time_format": "epoch_ms"
                }
            }
        }

        r = runner.CreateMlJob()
        r(es, params)

        es.xpack.ml.put_job.assert_called_once_with(job_id=params["job-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_ml_job_fallback(self, es):
        es.xpack.ml.put_job.side_effect = elasticsearch.TransportError(400, "Bad Request")
        body = {
                "description": "Total sum of requests",
                "analysis_config": {
                    "bucket_span": "10m",
                    "detectors": [
                        {
                            "detector_description": "Sum of total",
                            "function": "sum",
                            "field_name": "total"
                        }
                    ]
                },
                "data_description": {
                    "time_field": "timestamp",
                    "time_format": "epoch_ms"
                }
            }
        params = {
            "job-id": "an-ml-job",
            "body": body
        }

        r = runner.CreateMlJob()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("PUT",
                                                             "/_xpack/ml/anomaly_detectors/%s" % params["job-id"],
                                                             params=params,
                                                             body=body)


class DeleteMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_delete_ml_job(self, es):
        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.DeleteMlJob()
        r(es, params)

        es.xpack.ml.delete_job.assert_called_once_with(job_id=job_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_delete_ml_job_fallback(self, es):
        es.xpack.ml.delete_job.side_effect = elasticsearch.TransportError(400, "Bad Request")

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.DeleteMlJob()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("DELETE",
                                                             "/_xpack/ml/anomaly_detectors/%s" % params["job-id"],
                                                             params=params)


class OpenMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_open_ml_job(self, es):
        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.OpenMlJob()
        r(es, params)

        es.xpack.ml.open_job.assert_called_once_with(job_id=job_id)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_open_ml_job_fallback(self, es):
        es.xpack.ml.open_job.side_effect = elasticsearch.TransportError(400, "Bad Request")

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.OpenMlJob()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             "/_xpack/ml/anomaly_detectors/%s/_open" % params["job-id"],
                                                             params=params)


class CloseMlJobTests:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.parametrize("seed", range(20))
    def test_close_ml_job(self, es, seed):
        random.seed(seed)
        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.CloseMlJob()
        r(es, params)

        es.xpack.ml.close_job.assert_called_once_with(job_id=params["job-id"], force=params["force"], timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.parametrize("seed", range(20))
    def test_close_ml_job_fallback(self, es, seed):
        random.seed(seed)
        es.xpack.ml.close_job.side_effect = elasticsearch.TransportError(400, "Bad Request")

        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.CloseMlJob()
        r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             "/_xpack/ml/anomaly_detectors/%s/_close" % params["job-id"],
                                                             params=params)


class RawRequestRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_issue_request_with_defaults(self, es):
        r = runner.RawRequest()

        params = {
            "path": "/_cat/count"
        }
        r(es, params)

        es.transport.perform_request.assert_called_once_with(method="GET",
                                                             url="/_cat/count",
                                                             headers=None,
                                                             body=None,
                                                             params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_issue_delete_index(self, es):
        r = runner.RawRequest()

        params = {
            "method": "DELETE",
            "path": "/twitter",
            "ignore": [400, 404],
            "request-params": {
                "pretty": "true"
            }
        }
        r(es, params)

        es.transport.perform_request.assert_called_once_with(method="DELETE",
                                                             url="/twitter",
                                                             headers=None,
                                                             body=None,
                                                             params={"ignore": [400, 404], "pretty": "true"})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_issue_create_index(self, es):
        r = runner.RawRequest()

        params = {
            "method": "POST",
            "path": "/twitter",
            "body": {
                "settings": {
                    "index": {
                        "number_of_replicas": 0
                    }
                }
            }
        }
        r(es, params)

        es.transport.perform_request.assert_called_once_with(method="POST",
                                                             url="/twitter",
                                                             headers=None,
                                                             body={
                                                                 "settings": {
                                                                     "index": {
                                                                         "number_of_replicas": 0
                                                                     }
                                                                 }
                                                             },
                                                             params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_issue_msearch(self, es):
        r = runner.RawRequest()

        params = {
            "path": "/_msearch",
            "headers": {
                "Content-Type": "application/x-ndjson"
            },
            "body": [
                {"index": "test"},
                {"query": {"match_all": {}}, "from": 0, "size": 10},
                {"index": "test", "search_type": "dfs_query_then_fetch"},
                {"query": {"match_all": {}}}
            ]
        }
        r(es, params)

        es.transport.perform_request.assert_called_once_with(method="GET",
                                                             url="/_msearch",
                                                             headers={"Content-Type": "application/x-ndjson"},
                                                             body=[
                                                                 {"index": "test"},
                                                                 {"query": {"match_all": {}}, "from": 0, "size": 10},
                                                                 {"index": "test", "search_type": "dfs_query_then_fetch"},
                                                                 {"query": {"match_all": {}}}
                                                             ],
                                                             params={})


class SleepTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("time.sleep")
    def test_missing_parameter(self, sleep, es):
        r = runner.Sleep()
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'sleep' did not provide the mandatory parameter "
                                    "'duration'. Please add it to your parameter source."):
            r(es, params={})

        self.assertEqual(0, es.call_count)
        self.assertEqual(0, sleep.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("time.sleep")
    def test_sleep(self, sleep, es):
        r = runner.Sleep()
        r(es, params={"duration": 4.3})

        self.assertEqual(0, es.call_count)
        sleep.assert_called_once_with(4.3)


class DeleteSnapshotRepositoryTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_delete_snapshot_repository(self, es):
        params = {
            "repository": "backups"
        }

        r = runner.DeleteSnapshotRepository()
        r(es, params)

        es.snapshot.delete_repository.assert_called_once_with(repository="backups")


class CreateSnapshotRepositoryTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_create_snapshot_repository(self, es):
        params = {
            "repository": "backups",
            "body": {
                "type": "fs",
                "settings": {
                    "location": "/var/backups"
                }
            }
        }

        r = runner.CreateSnapshotRepository()
        r(es, params)

        es.snapshot.create_repository.assert_called_once_with(repository="backups",
                                                              body={
                                                                  "type": "fs",
                                                                  "settings": {
                                                                      "location": "/var/backups"
                                                                  }
                                                              },
                                                              params={})


class RestoreSnapshotTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_restore_snapshot(self, es):
        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "wait-for-completion": True,
            "request-params": {
                "request_timeout": 7200
            }
        }

        r = runner.RestoreSnapshot()
        r(es, params)

        es.snapshot.restore.assert_called_once_with(repository="backups",
                                                    snapshot="snapshot-001",
                                                    body=None,
                                                    wait_for_completion=True,
                                                    params={"request_timeout": 7200})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_restore_snapshot_with_body(self, es):
        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {
                "indices": "index1,index2",
                "include_global_state": False,
                "index_settings": {
                    "index.number_of_replicas": 0
                }
            },
            "wait-for-completion": True,
            "request-params": {
                "request_timeout": 7200
            }
        }

        r = runner.RestoreSnapshot()
        r(es, params)

        es.snapshot.restore.assert_called_once_with(repository="backups",
                                                    snapshot="snapshot-001",
                                                    body={
                                                        "indices": "index1,index2",
                                                        "include_global_state": False,
                                                        "index_settings": {
                                                            "index.number_of_replicas": 0
                                                        }
                                                    },
                                                    wait_for_completion=True,
                                                    params={"request_timeout": 7200})


class IndicesRecoveryTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_indices_recovery_already_finished(self, es):
        # empty response
        es.indices.recovery.return_value = {}

        r = runner.IndicesRecovery()
        self.assertFalse(r.completed)
        self.assertEqual(r.percent_completed, 0.0)

        r(es, {
            "completion-recheck-wait-period": 0
        })

        self.assertTrue(r.completed)
        self.assertEqual(r.percent_completed, 1.0)

        es.indices.recovery.assert_called_with(active_only=True)
        # retries three times
        self.assertEqual(3, es.indices.recovery.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    def test_waits_for_ongoing_indices_recovery(self, es):
        # empty response
        es.indices.recovery.side_effect = [
            # active recovery
            {
                "index1": {
                    "shards": [
                        {
                            "id": 0,
                            "index": {
                                "size": {
                                    "total": "75.4mb",
                                    "total_in_bytes": 79063092,
                                    "recovered": "65.7mb",
                                    "recovered_in_bytes": 68891939,
                                }
                            }
                        },
                        {
                            "id": 1,
                            "index": {
                                "size": {
                                    "total": "175.4mb",
                                    "total_in_bytes": 179063092,
                                    "recovered": "165.7mb",
                                    "recovered_in_bytes": 168891939,
                                }
                            }
                        }
                    ]
                }
            },
            # completed - will be called three times
            {},
            {},
            {},
        ]

        r = runner.IndicesRecovery()
        self.assertFalse(r.completed)
        self.assertEqual(r.percent_completed, 0.0)

        while not r.completed:
            recovered_bytes, unit = r(es, {
                "completion-recheck-wait-period": 0
            })
            if r.completed:
                # no additional bytes recovered since the last call
                self.assertEqual(recovered_bytes, 0)
                self.assertEqual(r.percent_completed, 1.0)
            else:
                # sum of both shards
                self.assertEqual(recovered_bytes, 237783878)
                self.assertAlmostEqual(r.percent_completed, 0.9211, places=3)
            self.assertEqual(unit, "bytes")


class ShrinkIndexTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("time.sleep")
    def test_shrink_index_with_shrink_node(self, sleep, es):
        # cluster health API
        es.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0
        }

        r = runner.ShrinkIndex()
        params = {
            "source-index": "src",
            "target-index": "target",
            "target-body": {
                "settings": {
                    "index.number_of_replicas": 2,
                    "index.number_of_shards": 0
                }
            },
            "shrink-node": "rally-node-0"
        }

        r(es, params)

        es.indices.put_settings.assert_called_once_with(index="src",
                                                        body={
                                                            "settings": {
                                                                "index.routing.allocation.require._name": "rally-node-0",
                                                                "index.blocks.write": "true"
                                                            }
                                                        },
                                                        preserve_existing=True)

        es.cluster.health.assert_has_calls([
            mock.call(index="src", params={"wait_for_no_relocating_shards": "true"}),
            mock.call(index="target", params={"wait_for_no_relocating_shards": "true"}),
        ])

        es.indices.shrink.assert_called_once_with(index="src", target="target", body={
            "settings": {
                "index.number_of_replicas": 2,
                "index.number_of_shards": 0,
                "index.routing.allocation.require._name": None,
                "index.blocks.write": None
            }
        })

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("time.sleep")
    def test_shrink_index_derives_shrink_node(self, sleep, es):
        # cluster health API
        es.cluster.health.return_value = {
            "status": "green",
            "relocating_shards": 0
        }
        es.nodes.info.return_value = {
            "_nodes": {
                "total": 3,
                "successful": 3,
                "failed": 0
            },
            "cluster_name": "elasticsearch",
            "nodes": {
                "lsM0-tKnQqKEGVw-OZU5og": {
                    "name": "node0",
                    "roles": [
                        "master",
                        "data",
                        "ingest"
                    ]
                },
                "kxM0-tKnQqKEGVw-OZU5og": {
                    "name": "node1",
                    "roles": [
                        "master"
                    ]
                },
                "yyM0-tKnQqKEGVw-OZU5og": {
                    "name": "node0",
                    "roles": [
                        "ingest"
                    ]
                }
            }
        }

        r = runner.ShrinkIndex()
        params = {
            "source-index": "src",
            "target-index": "target",
            "target-body": {
                "settings": {
                    "index.number_of_replicas": 2,
                    "index.number_of_shards": 0
                }
            }
        }

        r(es, params)

        es.indices.put_settings.assert_called_once_with(index="src",
                                                        body={
                                                            "settings": {
                                                                # the only data node in the cluster was chosen
                                                                "index.routing.allocation.require._name": "node0",
                                                                "index.blocks.write": "true"
                                                            }
                                                        },
                                                        preserve_existing=True)

        es.cluster.health.assert_has_calls([
            mock.call(index="src", params={"wait_for_no_relocating_shards": "true"}),
            mock.call(index="target", params={"wait_for_no_relocating_shards": "true"}),
        ])

        es.indices.shrink.assert_called_once_with(index="src", target="target", body={
            "settings": {
                "index.number_of_replicas": 2,
                "index.number_of_shards": 0,
                "index.routing.allocation.require._name": None,
                "index.blocks.write": None
            }
        })


class PutSettingsTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_put_settings(self, es):
        params = {
            "body": {
                "transient": {
                    "indices.recovery.max_bytes_per_sec": "20mb"
                }
            }
        }

        r = runner.PutSettings()
        r(es, params)

        es.cluster.put_settings.assert_called_once_with(body={
            "transient": {
                "indices.recovery.max_bytes_per_sec": "20mb"
            }
        })


class RetryTests(TestCase):
    def test_is_transparent_on_success_when_no_retries(self):
        delegate = mock.Mock()
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        retrier(es, params)

        delegate.assert_called_once_with(es, params)

    def test_is_transparent_on_exception_when_no_retries(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=elasticsearch.ConnectionError("N/A", "no route to host"))
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionError):
            retrier(es, params)

        delegate.assert_called_once_with(es, params)

    def test_is_transparent_on_application_error_when_no_retries(self):
        original_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(return_value=original_return_value)
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)

        self.assertEqual(original_return_value, result)
        delegate.assert_called_once_with(es, params)

    def test_is_does_not_retry_on_success(self):
        delegate = mock.Mock()
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.1,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        retrier(es, params)

        delegate.assert_called_once_with(es, params)

    def test_retries_on_timeout_if_wanted_and_raises_if_no_recovery(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=elasticsearch.ConnectionError("N/A", "no route to host"))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionError):
            retrier(es, params)

        delegate.assert_has_calls([
            mock.call(es, params),
            mock.call(es, params),
            mock.call(es, params)
        ])

    def test_retries_on_timeout_if_wanted_and_returns_first_call(self):
        import elasticsearch
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(side_effect=[elasticsearch.ConnectionError("N/A", "no route to host"), failed_return_value])
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": False
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)
        self.assertEqual(failed_return_value, result)

        delegate.assert_has_calls([
            # has returned a connection error
            mock.call(es, params),
            # has returned normally
            mock.call(es, params)
        ])

    def test_retries_mixed_timeout_and_application_errors(self):
        import elasticsearch
        connection_error = elasticsearch.ConnectionError("N/A", "no route to host")
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(side_effect=[
            connection_error,
            failed_return_value,
            connection_error,
            connection_error,
            failed_return_value,
            success_return_value]
        )
        es = None
        params = {
            # we try exactly as often as there are errors to also test the semantics of "retry".
            "retries": 5,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)
        self.assertEqual(success_return_value, result)

        delegate.assert_has_calls([
            # connection error
            mock.call(es, params),
            # application error
            mock.call(es, params),
            # connection error
            mock.call(es, params),
            # connection error
            mock.call(es, params),
            # application error
            mock.call(es, params),
            # success
            mock.call(es, params)
        ])

    def test_does_not_retry_on_timeout_if_not_wanted(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=elasticsearch.ConnectionTimeout(408, "timed out"))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": False,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionTimeout):
            retrier(es, params)

        delegate.assert_called_once_with(es, params)

    def test_retries_on_application_error_if_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        delegate = mock.Mock(side_effect=[failed_return_value, success_return_value])
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": False,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)

        self.assertEqual(success_return_value, result)

        delegate.assert_has_calls([
            mock.call(es, params),
            # one retry
            mock.call(es, params)
        ])

    def test_does_not_retry_on_application_error_if_not_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(return_value=failed_return_value)
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": False
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)

        self.assertEqual(failed_return_value, result)

        delegate.assert_called_once_with(es, params)

    def test_assumes_success_if_runner_returns_non_dict(self):
        delegate = mock.Mock(return_value=(1, "ops"))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)

        self.assertEqual((1, "ops"), result)

        delegate.assert_called_once_with(es, params)

    def test_retries_until_success(self):
        failure_count = 5

        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        responses = []
        responses += failure_count * [failed_return_value]
        responses += [success_return_value]

        delegate = mock.Mock(side_effect=responses)
        es = None
        params = {
            "retry-until-success": True,
            "retry-wait-period": 0.01
        }
        retrier = runner.Retry(delegate)

        result = retrier(es, params)

        self.assertEqual(success_return_value, result)

        delegate.assert_has_calls([mock.call(es, params) for _ in range(failure_count + 1)])
