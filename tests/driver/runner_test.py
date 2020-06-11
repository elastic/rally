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
import json
import random
import unittest.mock as mock
from unittest import TestCase

import elasticsearch

from esrally import exceptions
from esrally.driver import runner
from tests import run_async, as_future


class BaseUnitTestContextManagerRunner:
    async def __aenter__(self):
        self.fp = io.StringIO("many\nlines\nin\na\nfile")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.fp.close()
        return False


class RegisterRunnerTests(TestCase):
    def tearDown(self):
        runner.remove_runner("unit_test")

    @run_async
    async def test_runner_function_should_be_wrapped(self):
        async def runner_function(*args):
            return args

        runner.register_runner(operation_type="unit_test", runner=runner_function, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [runner_function]", repr(returned_runner))
        self.assertEqual(("default_client", "param"),
                         await returned_runner({"default": "default_client", "other": "other_client"}, "param"))

    @run_async
    async def test_single_cluster_runner_class_with_context_manager_should_be_wrapped_with_context_manager_enabled(self):
        class UnitTestSingleClusterContextManagerRunner(BaseUnitTestContextManagerRunner):
            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterContextManagerRunner"

        test_runner = UnitTestSingleClusterContextManagerRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined context-manager enabled runner for [UnitTestSingleClusterContextManagerRunner]",
                         repr(returned_runner))
        # test that context_manager functionality gets preserved after wrapping
        async with returned_runner:
            self.assertEqual(("default_client", "param"),
                             await returned_runner({"default": "default_client", "other": "other_client"}, "param"))
        # check that the context manager interface of our inner runner has been respected.
        self.assertTrue(test_runner.fp.closed)

    @run_async
    async def test_multi_cluster_runner_class_with_context_manager_should_be_wrapped_with_context_manager_enabled(self):
        class UnitTestMultiClusterContextManagerRunner(BaseUnitTestContextManagerRunner):
            multi_cluster = True

            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestMultiClusterContextManagerRunner"

        test_runner = UnitTestMultiClusterContextManagerRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined context-manager enabled runner for [UnitTestMultiClusterContextManagerRunner]",
                         repr(returned_runner))

        # test that context_manager functionality gets preserved after wrapping
        all_clients = {"default": "default_client", "other": "other_client"}
        async with returned_runner:
            self.assertEqual((all_clients, "param1", "param2"), await returned_runner(all_clients, "param1", "param2"))
        # check that the context manager interface of our inner runner has been respected.
        self.assertTrue(test_runner.fp.closed)

    @run_async
    async def test_single_cluster_runner_class_should_be_wrapped(self):
        class UnitTestSingleClusterRunner:
            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterRunner"

        test_runner = UnitTestSingleClusterRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [UnitTestSingleClusterRunner]", repr(returned_runner))
        self.assertEqual(("default_client", "param"),
                         await returned_runner({"default": "default_client", "other": "other_client"}, "param"))

    @run_async
    async def test_multi_cluster_runner_class_should_be_wrapped(self):
        class UnitTestMultiClusterRunner:
            multi_cluster = True

            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestMultiClusterRunner"

        test_runner = UnitTestMultiClusterRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.NoCompletion)
        self.assertEqual("user-defined runner for [UnitTestMultiClusterRunner]", repr(returned_runner))
        all_clients = {"default": "default_client", "other": "other_client"}
        self.assertEqual((all_clients, "some_param"), await returned_runner(all_clients, "some_param"))


class SelectiveJsonParserTests(TestCase):
    def doc_as_text(self, doc):
        return io.StringIO(json.dumps(doc))

    def test_parse_all_expected(self):
        doc = self.doc_as_text({
            "title": "Hello",
            "meta": {
                "length": 100,
                "date": {
                    "year": 2000
                }
            }
        })

        parsed = runner.parse(doc, [
            # simple property
            "title",
            # a nested property
            "meta.date.year",
            # ignores unknown properties
            "meta.date.month"
        ])

        self.assertEqual("Hello", parsed.get("title"))
        self.assertEqual(2000, parsed.get("meta.date.year"))
        self.assertNotIn("meta.date.month", parsed)

    def test_list_length(self):
        doc = self.doc_as_text({
            "title": "Hello",
            "meta": {
                "length": 100,
                "date": {
                    "year": 2000
                }
            },
            "authors": ["George", "Harry"],
            "readers": [
                {
                    "name": "Tom",
                    "age": 14
                },
                {
                    "name": "Bob",
                    "age": 17
                },
                {
                    "name": "Alice",
                    "age": 22
                }
            ],
            "supporters": []
        })

        parsed = runner.parse(doc, [
            # simple property
            "title",
            # a nested property
            "meta.date.year",
            # ignores unknown properties
            "meta.date.month"
        ], ["authors", "readers", "supporters"])

        self.assertEqual("Hello", parsed.get("title"))
        self.assertEqual(2000, parsed.get("meta.date.year"))
        self.assertNotIn("meta.date.month", parsed)

        # lists
        self.assertFalse(parsed.get("authors"))
        self.assertFalse(parsed.get("readers"))
        self.assertTrue(parsed.get("supporters"))


class BulkIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_bulk_index_missing_params(self, es):
        bulk_response = {
            "errors": False,
            "took": 8
        }
        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))

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
            await bulk(es, bulk_params)
        self.assertEqual("Parameter source for operation 'bulk-index' did not provide the mandatory parameter 'action-metadata-present'. "
                         "Please add it to your parameter source.", ctx.exception.args[0])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_bulk_index_success_with_metadata(self, es):
        bulk_response = {
            "errors": False,
            "took": 8
        }
        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))

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

        result = await bulk(es, bulk_params)

        self.assertEqual(8, result["took"])
        self.assertIsNone(result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_bulk_index_success_without_metadata_with_doc_type(self, es):
        bulk_response = {
            "errors": False,
            "took": 8
        }
        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))
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

        result = await bulk(es, bulk_params)

        self.assertEqual(8, result["took"])
        self.assertEqual("test-index", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], index="test-index", doc_type="_doc", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_bulk_index_success_without_metadata_and_without_doc_type(self, es):
        bulk_response = {
            "errors": False,
            "took": 8
        }
        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": "index_line\n" +
                    "index_line\n" +
                    "index_line\n",
            "action-metadata-present": False,
            "bulk-size": 3,
            "index": "test-index"
        }

        result = await bulk(es, bulk_params)

        self.assertEqual(8, result["took"])
        self.assertEqual("test-index", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])
        self.assertFalse("error-type" in result)

        es.bulk.assert_called_with(body=bulk_params["body"], index="test-index", doc_type=None, params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_bulk_index_error(self, es):
        bulk_response = {
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

        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))

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

        result = await bulk(es, bulk_params)

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
    @run_async
    async def test_bulk_index_error_no_shards(self, es):
        bulk_response = {
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

        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))

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

        result = await bulk(es, bulk_params)

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
    @run_async
    async def test_mixed_bulk_with_simple_stats(self, es):
        bulk_response = {
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
        es.bulk.return_value = as_future(io.StringIO(json.dumps(bulk_response)))
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

        result = await bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(30, result["took"])
        self.assertNotIn("ingest_took", result, "ingest_took is not extracted with simple stats")
        self.assertEqual(4, result["weight"])
        self.assertEqual(4, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(2, result["error-count"])
        self.assertEqual("bulk", result["error-type"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_mixed_bulk_with_detailed_stats_body_as_string(self, es):
        es.bulk.return_value = as_future({
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
        })
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

        result = await bulk(es, bulk_params)

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

        es.bulk.return_value.result().pop("ingest_took")
        result = await bulk(es, bulk_params)
        self.assertNotIn("ingest_took", result)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_simple_bulk_with_detailed_stats_body_as_list(self, es):
        es.bulk.return_value = as_future({
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
        })
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": '{ "index" : { "_index" : "test", "_type" : "type1" } }\n' +
                    '{"location" : [-0.1485188, 51.5250666]}\n',
            "action-metadata-present": True,
            "bulk-size": 1,
            "detailed-results": True,
            "index": "test"
        }

        result = await bulk(es, bulk_params)

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

        es.bulk.return_value.result().pop("ingest_took")
        result = await bulk(es, bulk_params)
        self.assertNotIn("ingest_took", result)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_simple_bulk_with_detailed_stats_body_as_unrecognized_type(self, es):
        es.bulk.return_value = as_future({
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
        })
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
            await bulk(es, bulk_params)

        es.bulk.assert_called_with(body=bulk_params["body"], params={})


class ForceMergeRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_force_merge_with_defaults(self, es):
        es.indices.forcemerge.return_value = as_future()
        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index" : "_all"})

        es.indices.forcemerge.assert_called_once_with(index="_all", request_timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_force_merge_override_request_timeout(self, es):
        es.indices.forcemerge.return_value = as_future()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "request-timeout": 50000})

        es.indices.forcemerge.assert_called_once_with(index="_all", request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_force_merge_with_params(self, es):
        es.indices.forcemerge.return_value = as_future()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index" : "_all", "max-num-segments": 1, "request-timeout": 50000})

        es.indices.forcemerge.assert_called_once_with(index="_all", max_num_segments=1, request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_optimize_with_defaults(self, es):
        es.indices.forcemerge.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={})

        es.transport.perform_request.assert_called_once_with("POST", "/_optimize", timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_optimize_with_params(self, es):
        es.indices.forcemerge.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()
        force_merge = runner.ForceMerge()
        await force_merge(es, params={"max-num-segments": 3, "request-timeout": 17000})

        es.transport.perform_request.assert_called_once_with("POST", "/_optimize?max_num_segments=3", timeout=17000)


class IndicesStatsRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_indices_stats_without_parameters(self, es):
        es.indices.stats.return_value = as_future({})
        indices_stats = runner.IndicesStats()
        result = await indices_stats(es, params={})
        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])

        es.indices.stats.assert_called_once_with(index="_all", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_indices_stats_with_failed_condition(self, es):
        es.indices.stats.return_value = as_future({
            "_all": {
                "total": {
                    "merges": {
                        "current": 2,
                        "current_docs": 292698,
                    }
                }
            }
        })

        indices_stats = runner.IndicesStats()

        result = await indices_stats(es, params={
            "index": "logs-*",
            "condition": {
                "path": "_all.total.merges.current",
                "expected-value": 0
            }
        })
        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertFalse(result["success"])
        self.assertDictEqual({
            "path": "_all.total.merges.current",
            "actual-value": "2",
            "expected-value": "0"
        }, result["condition"])

        es.indices.stats.assert_called_once_with(index="logs-*", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_indices_stats_with_successful_condition(self, es):
        es.indices.stats.return_value = as_future({
            "_all": {
                "total": {
                    "merges": {
                        "current": 0,
                        "current_docs": 292698,
                    }
                }
            }
        })

        indices_stats = runner.IndicesStats()

        result = await indices_stats(es, params={
            "index": "logs-*",
            "condition": {
                "path": "_all.total.merges.current",
                "expected-value": 0
            }
        })
        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertTrue(result["success"])
        self.assertDictEqual({
            "path": "_all.total.merges.current",
            "actual-value": "0",
            "expected-value": "0"
        }, result["condition"])

        es.indices.stats.assert_called_once_with(index="logs-*", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_indices_stats_with_non_existing_path(self, es):
        es.indices.stats.return_value = as_future({
            "indices": {
                "total": {
                    "docs": {
                        "current": 0
                    }
                }
            }
        })

        indices_stats = runner.IndicesStats()

        result = await indices_stats(es, params={
            "index": "logs-*",
            "condition": {
                # non-existing path
                "path": "indices.my_index.total.docs.count",
                "expected-value": 0
            }
        })
        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertFalse(result["success"])
        self.assertDictEqual({
            "path": "indices.my_index.total.docs.count",
            "actual-value": None,
            "expected-value": "0"
        }, result["condition"])

        es.indices.stats.assert_called_once_with(index="logs-*", metric="_all")


class QueryRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_match_only_request_body_defined(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 1,
                    "relation": "gte"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "detailed-results": True,
            "cache": True,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(1, result["hits"])
        self.assertEqual("gte", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/_all/_search",
            params={"request_cache": "true"},
            body=params["body"],
            headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_match_using_request_params(self, es):
        response = {
            "timed_out": False,
            "took": 62,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }

                ]
            }
        }
        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(response)))

        query_runner = runner.Query()
        params = {
            "cache": False,
            "detailed-results": True,
            "body": None,
            "request-params": {
                "q": "user:kimchy"
            }
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(62, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/_all/_search",
            params={
                "request_cache": "false",
                "q": "user:kimchy"
            },
            body=params["body"],
            headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_no_detailed_results(self, es):
        response = {
            "timed_out": False,
            "took": 62,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }

                ]
            }
        }
        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(response)))

        query_runner = runner.Query()
        params = {
            "body": None,
            "request-params": {
                "q": "user:kimchy"
            },
            "detailed-results": False
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertNotIn("hits", result)
        self.assertNotIn("hits_relation", result)
        self.assertNotIn("timed_out", result)
        self.assertNotIn("took", result)
        self.assertNotIn("error-type", result)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/_all/_search",
            params={"q": "user:kimchy"},
            body=params["body"],
            headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_hits_total_as_number(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": 2,
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "cache": True,
            "detailed-results": True,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/_all/_search",
            params={
                "request_cache": "true"
            },
            body=params["body"],
            headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_match_all(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }
        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "index": "unittest",
            "detailed-results": True,
            "response-compression-enabled": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/unittest/_search",
            params={},
            body=params["body"],
            headers={"Accept-Encoding": "identity"}
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_query_match_all_doc_type_fallback(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }

        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "index": "unittest",
            "type": "type",
            "detailed-results": True,
            "cache": None,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            result = await query_runner(es, params)

        self.assertEqual(1, result["weight"])
        self.assertEqual("ops", result["unit"])
        self.assertEqual(2, result["hits"])
        self.assertEqual("eq", result["hits_relation"])
        self.assertFalse(result["timed_out"])
        self.assertEqual(5, result["took"])
        self.assertFalse("error-type" in result)

        es.transport.perform_request.assert_called_once_with(
            "GET", "/unittest/type/_search",
            body=params["body"],
            params={},
            headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_only_one_page(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }

        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))
        es.clear_scroll.return_value = as_future(io.StringIO('{"acknowledged": true}'))

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

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/unittest/_search",
            params={"request_cache": "true", "sort": "_doc", "scroll": "10s", "size": 100},
            body=params["body"],
            headers=None
        )
        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_no_request_cache(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }

        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))
        es.clear_scroll.return_value = as_future(io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "response-compression-enabled": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/unittest/_search",
            params={"sort": "_doc", "scroll": "10s", "size": 100},
            body=params["body"],
            headers={"Accept-Encoding": "identity"}
        )
        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_only_one_page_only_request_body_defined(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }

        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))
        es.clear_scroll.return_value = as_future(io.StringIO('{"acknowledged": true}'))

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

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(2, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(4, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.transport.perform_request.assert_called_once_with(
            "GET",
            "/_all/_search",
            params={"sort": "_doc", "scroll": "10s", "size": 100},
            body=params["body"],
            headers=None
        )

        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_with_explicit_number_of_pages(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 54,
            "hits": {
                "total": {
                    # includes all hits across all pages
                    "value": 3,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    }
                ]
            }
        }

        # page 2
        scroll_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": True,
            "took": 25,
            "hits": {
                "hits": [
                    {
                        "title": "some-doc-3"
                    }
                ]
            }
        }

        es.transport.perform_request.side_effect = [
            as_future(io.StringIO(json.dumps(search_response))),
            as_future(io.StringIO(json.dumps(scroll_response)))
        ]

        es.clear_scroll.return_value = as_future(io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "pages": 2,
            "results-per-page": 2,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(3, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual(79, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertTrue(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_cannot_clear_scroll(self, es):
        import elasticsearch
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 53,
            "hits": {
                "total": {
                    "value": 1,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    }
                ]
            }
        }

        es.transport.perform_request.return_value = as_future(io.StringIO(json.dumps(search_response)))
        es.clear_scroll.return_value = as_future(exception=elasticsearch.ConnectionTimeout())

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

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(1, results["weight"])
        self.assertEqual(1, results["pages"])
        self.assertEqual(1, results["hits"])
        self.assertEqual("eq", results["hits_relation"])
        self.assertEqual("pages", results["unit"])
        self.assertEqual(53, results["took"])
        self.assertFalse("error-type" in results)

        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_scroll_query_request_all_pages(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 876,
            "hits": {
                "total": {
                    "value": 4,
                    "relation": "gte"
                },
                "hits": [
                    {
                        "title": "some-doc-1"
                    },
                    {
                        "title": "some-doc-2"
                    },
                    {
                        "title": "some-doc-3"
                    },
                    {
                        "title": "some-doc-4"
                    }
                ]
            }
        }

        # page 2 has no results
        scroll_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 2,
            "hits": {
                "hits": []
            }
        }

        es.transport.perform_request.side_effect = [
            as_future(io.StringIO(json.dumps(search_response))),
            as_future(io.StringIO(json.dumps(scroll_response)))
        ]
        es.clear_scroll.return_value = as_future(io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "pages": "all",
            "results-per-page": 4,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {}
                }
            }
        }

        async with query_runner:
            results = await query_runner(es, params)

        self.assertEqual(2, results["weight"])
        self.assertEqual(2, results["pages"])
        self.assertEqual(4, results["hits"])
        self.assertEqual("gte", results["hits_relation"])
        self.assertEqual(878, results["took"])
        self.assertEqual("pages", results["unit"])
        self.assertFalse(results["timed_out"])
        self.assertFalse("error-type" in results)

        es.clear_scroll.assert_called_once_with(body={"scroll_id": ["some-scroll-id"]})


class PutPipelineRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_pipeline(self, es):
        es.ingest.put_pipeline.return_value = as_future()

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

        await r(es, params)

        es.ingest.put_pipeline.assert_called_once_with(id="rename", body=params["body"], master_timeout=None, timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_param_body_mandatory(self, es):
        es.ingest.put_pipeline.return_value = as_future()

        r = runner.PutPipeline()

        params = {
            "id": "rename"
        }
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'body'. "
                                    "Please add it to your parameter source."):
            await r(es, params)

        self.assertEqual(0, es.ingest.put_pipeline.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_param_id_mandatory(self, es):
        es.ingest.put_pipeline.return_value = as_future()

        r = runner.PutPipeline()

        params = {
            "body": {}
        }
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'id'. "
                                    "Please add it to your parameter source."):
            await r(es, params)

        self.assertEqual(0, es.ingest.put_pipeline.call_count)


class ClusterHealthRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_waits_for_expected_cluster_status(self, es):
        es.cluster.health.return_value = as_future({
            "status": "green",
            "relocating_shards": 0
        })
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "green"
            }
        }

        result = await r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0
        }, result)

        es.cluster.health.assert_called_once_with(index=None, params={"wait_for_status": "green"})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_accepts_better_cluster_status(self, es):
        es.cluster.health.return_value = as_future({
            "status": "green",
            "relocating_shards": 0
        })
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "yellow"
            }
        }

        result = await r(es, params)

        self.assertDictEqual({
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0
        }, result)

        es.cluster.health.assert_called_once_with(index=None, params={"wait_for_status": "yellow"})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_rejects_relocating_shards(self, es):
        es.cluster.health.return_value = as_future({
            "status": "yellow",
            "relocating_shards": 3
        })
        r = runner.ClusterHealth()

        params = {
            "index": "logs-*",
            "request-params": {
                "wait_for_status": "red",
                "wait_for_no_relocating_shards": True
            }
        }

        result = await r(es, params)

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
    @run_async
    async def test_rejects_unknown_cluster_status(self, es):
        es.cluster.health.return_value = as_future({
            "status": None,
            "relocating_shards": 0
        })
        r = runner.ClusterHealth()

        params = {
            "request-params": {
                "wait_for_status": "green"
            }
        }

        result = await r(es, params)

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
    @run_async
    async def test_creates_multiple_indices(self, es):
        es.indices.create.return_value = as_future()

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

        result = await r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.create.assert_has_calls([
            mock.call(index="indexA", body={"settings": {}}, params=request_params),
            mock.call(index="indexB", body={"settings": {}}, params=request_params)
        ])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_param_indices_mandatory(self, es):
        es.indices.create.return_value = as_future()

        r = runner.CreateIndex()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'create-index' did not provide the mandatory parameter 'indices'. "
                                    "Please add it to your parameter source."):
            await r(es, params)

        self.assertEqual(0, es.indices.create.call_count)


class DeleteIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_deletes_existing_indices(self, es):
        es.indices.exists.side_effect = [as_future(False), as_future(True)]
        es.indices.delete.return_value = as_future()

        r = runner.DeleteIndex()

        params = {
            "indices": ["indexA", "indexB"],
            "only-if-exists": True
        }

        result = await r(es, params)

        self.assertEqual((1, "ops"), result)

        es.indices.delete.assert_called_once_with(index="indexB", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_deletes_all_indices(self, es):
        es.indices.delete.return_value = as_future()

        r = runner.DeleteIndex()

        params = {
            "indices": ["indexA", "indexB"],
            "only-if-exists": False,
            "request-params": {
                "ignore_unavailable": "true",
                "expand_wildcards": "none"
            }
        }

        result = await r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.delete.assert_has_calls([
            mock.call(index="indexA", params=params["request-params"]),
            mock.call(index="indexB", params=params["request-params"])
        ])
        self.assertEqual(0, es.indices.exists.call_count)


class CreateIndexTemplateRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_index_templates(self, es):
        es.indices.put_template.return_value = as_future()

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

        result = await r(es, params)

        self.assertEqual((2, "ops"), result)

        es.indices.put_template.assert_has_calls([
            mock.call(name="templateA", body={"settings": {}}, params=params["request-params"]),
            mock.call(name="templateB", body={"settings": {}}, params=params["request-params"])
        ])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_param_templates_mandatory(self, es):
        es.indices.put_template.return_value = as_future()

        r = runner.CreateIndexTemplate()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'create-index-template' did not provide the mandatory parameter "
                                    "'templates'. Please add it to your parameter source."):
            await r(es, params)

        self.assertEqual(0, es.indices.put_template.call_count)


class DeleteIndexTemplateRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_deletes_all_index_templates(self, es):
        es.indices.delete_template.return_value = as_future()
        es.indices.delete.return_value = as_future()

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
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        self.assertEqual((3, "ops"), result)

        es.indices.delete_template.assert_has_calls([
            mock.call(name="templateA", params=params["request-params"]),
            mock.call(name="templateB", params=params["request-params"])
        ])
        es.indices.delete.assert_called_once_with(index="logs-*")

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_deletes_only_existing_index_templates(self, es):
        es.indices.exists_template.side_effect = [as_future(False), as_future(True)]
        es.indices.delete_template.return_value = as_future()

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
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        self.assertEqual((1, "ops"), result)

        es.indices.delete_template.assert_called_once_with(name="templateB", params=params["request-params"])
        # not called because the matching index is empty.
        self.assertEqual(0, es.indices.delete.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_param_templates_mandatory(self, es):
        r = runner.DeleteIndexTemplate()

        params = {}
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'delete-index-template' did not provide the mandatory parameter "
                                    "'templates'. Please add it to your parameter source."):
            await r(es, params)

        self.assertEqual(0, es.indices.delete_template.call_count)


class CreateMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_ml_datafeed(self, es):
        es.xpack.ml.put_datafeed.return_value = as_future()

        params = {
            "datafeed-id": "some-data-feed",
            "body": {
                "job_id": "total-requests",
                "indices": ["server-metrics"]
            }
        }

        r = runner.CreateMlDatafeed()
        await r(es, params)

        es.xpack.ml.put_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_ml_datafeed_fallback(self, es):
        es.xpack.ml.put_datafeed.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()
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
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("PUT", f"/_xpack/ml/datafeeds/{datafeed_id}", body=body)


class DeleteMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_delete_ml_datafeed(self, es):
        es.xpack.ml.delete_datafeed.return_value = as_future()

        datafeed_id = "some-data-feed"
        params = {
            "datafeed-id": datafeed_id
        }

        r = runner.DeleteMlDatafeed()
        await r(es, params)

        es.xpack.ml.delete_datafeed.assert_called_once_with(datafeed_id=datafeed_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_delete_ml_datafeed_fallback(self, es):
        es.xpack.ml.delete_datafeed.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()
        datafeed_id = "some-data-feed"
        params = {
            "datafeed-id": datafeed_id,
        }

        r = runner.DeleteMlDatafeed()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("DELETE",
                                                             f"/_xpack/ml/datafeeds/{datafeed_id}",
                                                             params={
                                                                 "force": "false",
                                                                 "ignore": 404
                                                             })


class StartMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_start_ml_datafeed_with_body(self, es):
        es.xpack.ml.start_datafeed.return_value = as_future()
        params = {
            "datafeed-id": "some-data-feed",
            "body": {
                "end": "now"
            }
        }

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.xpack.ml.start_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                           body=params["body"],
                                                           start=None,
                                                           end=None,
                                                           timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_start_ml_datafeed_with_body_fallback(self, es):
        es.xpack.ml.start_datafeed.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()
        body = {
                "end": "now"
            }
        params = {
            "datafeed-id": "some-data-feed",
            "body": body
        }

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             f"/_xpack/ml/datafeeds/{params['datafeed-id']}/_start",
                                                             body=body)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_start_ml_datafeed_with_params(self, es):
        es.xpack.ml.start_datafeed.return_value = as_future()
        params = {
            "datafeed-id": "some-data-feed",
            "start": "2017-01-01T01:00:00Z",
            "end": "now",
            "timeout": "10s"
        }

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.xpack.ml.start_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                           body=None,
                                                           start=params["start"],
                                                           end=params["end"],
                                                           timeout=params["timeout"])


class StopMlDatafeedTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_stop_ml_datafeed(self, es):
        es.xpack.ml.stop_datafeed.return_value = as_future()
        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.StopMlDatafeed()
        await r(es, params)

        es.xpack.ml.stop_datafeed.assert_called_once_with(datafeed_id=params["datafeed-id"],
                                                          force=params["force"],
                                                          timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_stop_ml_datafeed_fallback(self, es):
        es.xpack.ml.stop_datafeed.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.StopMlDatafeed()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             f"/_xpack/ml/datafeeds/{params['datafeed-id']}/_stop",
                                                             params={
                                                                 "force": str(params["force"]).lower(),
                                                                 "timeout": params["timeout"]
                                                             })


class CreateMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_ml_job(self, es):
        es.xpack.ml.put_job.return_value = as_future()

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
        await r(es, params)

        es.xpack.ml.put_job.assert_called_once_with(job_id=params["job-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_ml_job_fallback(self, es):
        es.xpack.ml.put_job.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

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
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("PUT",
                                                             f"/_xpack/ml/anomaly_detectors/{params['job-id']}",
                                                             body=body)


class DeleteMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_delete_ml_job(self, es):
        es.xpack.ml.delete_job.return_value = as_future()

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.DeleteMlJob()
        await r(es, params)

        es.xpack.ml.delete_job.assert_called_once_with(job_id=job_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_delete_ml_job_fallback(self, es):
        es.xpack.ml.delete_job.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.DeleteMlJob()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("DELETE",
                                                             f"/_xpack/ml/anomaly_detectors/{params['job-id']}",
                                                             params={
                                                                 "force": "false",
                                                                 "ignore": 404
                                                             })


class OpenMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_open_ml_job(self, es):
        es.xpack.ml.open_job.return_value = as_future()

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.OpenMlJob()
        await r(es, params)

        es.xpack.ml.open_job.assert_called_once_with(job_id=job_id)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_open_ml_job_fallback(self, es):
        es.xpack.ml.open_job.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

        job_id = "an-ml-job"
        params = {
            "job-id": job_id
        }

        r = runner.OpenMlJob()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             f"/_xpack/ml/anomaly_detectors/{params['job-id']}/_open")


class CloseMlJobTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_close_ml_job(self, es):
        es.xpack.ml.close_job.return_value = as_future()
        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.CloseMlJob()
        await r(es, params)

        es.xpack.ml.close_job.assert_called_once_with(job_id=params["job-id"], force=params["force"], timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_close_ml_job_fallback(self, es):
        es.xpack.ml.close_job.side_effect = as_future(exception=elasticsearch.TransportError(400, "Bad Request"))
        es.transport.perform_request.return_value = as_future()

        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s"
        }

        r = runner.CloseMlJob()
        await r(es, params)

        es.transport.perform_request.assert_called_once_with("POST",
                                                             f"/_xpack/ml/anomaly_detectors/{params['job-id']}/_close",
                                                             params={
                                                                 "force": str(params["force"]).lower(),
                                                                 "timeout": params["timeout"]
                                                             })


class RawRequestRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_issue_request_with_defaults(self, es):
        es.transport.perform_request.return_value = as_future()
        r = runner.RawRequest()

        params = {
            "path": "/_cat/count"
        }
        await r(es, params)

        es.transport.perform_request.assert_called_once_with(method="GET",
                                                             url="/_cat/count",
                                                             headers=None,
                                                             body=None,
                                                             params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_issue_delete_index(self, es):
        es.transport.perform_request.return_value = as_future()
        r = runner.RawRequest()

        params = {
            "method": "DELETE",
            "path": "/twitter",
            "ignore": [400, 404],
            "request-params": {
                "pretty": "true"
            }
        }
        await r(es, params)

        es.transport.perform_request.assert_called_once_with(method="DELETE",
                                                             url="/twitter",
                                                             headers=None,
                                                             body=None,
                                                             params={"ignore": [400, 404], "pretty": "true"})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_issue_create_index(self, es):
        es.transport.perform_request.return_value = as_future()
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
        await r(es, params)

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
    @run_async
    async def test_issue_msearch(self, es):
        es.transport.perform_request.return_value = as_future()
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
        await r(es, params)

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
    @mock.patch("asyncio.sleep", return_value=as_future())
    @run_async
    async def test_missing_parameter(self, sleep, es):
        r = runner.Sleep()
        with self.assertRaisesRegex(exceptions.DataError,
                                    "Parameter source for operation 'sleep' did not provide the mandatory parameter "
                                    "'duration'. Please add it to your parameter source."):
            await r(es, params={})

        self.assertEqual(0, es.call_count)
        self.assertEqual(0, sleep.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep", return_value=as_future())
    @run_async
    async def test_sleep(self, sleep, es):
        r = runner.Sleep()
        await r(es, params={"duration": 4.3})

        self.assertEqual(0, es.call_count)
        sleep.assert_called_once_with(4.3)


class DeleteSnapshotRepositoryTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_delete_snapshot_repository(self, es):
        es.snapshot.delete_repository.return_value = as_future()
        params = {
            "repository": "backups"
        }

        r = runner.DeleteSnapshotRepository()
        await r(es, params)

        es.snapshot.delete_repository.assert_called_once_with(repository="backups")


class CreateSnapshotRepositoryTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_snapshot_repository(self, es):
        es.snapshot.create_repository.return_value = as_future()
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
        await r(es, params)

        es.snapshot.create_repository.assert_called_once_with(repository="backups",
                                                              body={
                                                                  "type": "fs",
                                                                  "settings": {
                                                                      "location": "/var/backups"
                                                                  }
                                                              },
                                                              params={})


class CreateSnapshotTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_snapshot_no_wait(self, es):
        es.snapshot.create.return_value = as_future({})
        # should not be called
        es.snapshot.status.return_value = as_future({})

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {
                "indices": "logs-*"
            },
            "wait-for-completion": False,
            "request-params": {
                "request_timeout": 7200
            }
        }

        r = runner.CreateSnapshot()
        await r(es, params)

        es.snapshot.create.assert_called_once_with(repository="backups",
                                                   snapshot="snapshot-001",
                                                   body={
                                                       "indices": "logs-*"
                                                   },
                                                   params={"request_timeout": 7200},
                                                   wait_for_completion=False)
        self.assertEqual(0, es.snapshot.status.call_count)

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_create_snapshot_wait_for_completion(self, es):
        es.snapshot.create.return_value = as_future({
            "snapshot": {
                "snapshot": "snapshot-001",
                "uuid": "wjt6zFEIRua_-jutT5vrAw",
                "version_id": 7070099,
                "version": "7.7.0",
                "indices": [
                    "logs-2020-01-01"
                ],
                "include_global_state": False,
                "state": "SUCCESS",
                "start_time": "2020-06-10T07:38:53.811Z",
                "start_time_in_millis": 1591774733811,
                "end_time": "2020-06-10T07:38:55.015Z",
                "end_time_in_millis": 1591774735015,
                "duration_in_millis": 1204,
                "failures": [],
                "shards": {
                    "total": 5,
                    "failed": 0,
                    "successful": 5
                }
            }
        })

        es.snapshot.status.return_value = as_future({
            "snapshots": [
                {
                    "snapshot": "snapshot-001",
                    "repository": "backups",
                    "uuid": "5uZiG1bhRri2DsBpZxj91A",
                    "state": "SUCCESS",
                    "include_global_state": False,
                    "stats": {
                        "total": {
                            "file_count": 70,
                            "size_in_bytes": 9399505
                        },
                        "start_time_in_millis": 1591776481060,
                        "time_in_millis": 200
                    }
                }
            ]
        })

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {
                "indices": "logs-*"
            },
            "wait-for-completion": True,
            "request-params": {
                "request_timeout": 7200
            }
        }

        r = runner.CreateSnapshot()
        result = await r(es, params)

        self.assertDictEqual({
            "weight": 9399505,
            "unit": "byte",
            "success": True,
            "service_time": 0.2,
            "time_period": 0.2
        }, result)

        es.snapshot.create.assert_called_once_with(repository="backups",
                                                   snapshot="snapshot-001",
                                                   body={
                                                       "indices": "logs-*"
                                                   },
                                                   params={"request_timeout": 7200},
                                                   wait_for_completion=True)

        es.snapshot.status.assert_called_once_with(repository="backups",
                                                   snapshot="snapshot-001")


class RestoreSnapshotTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_restore_snapshot(self, es):
        es.snapshot.restore.return_value = as_future()

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "wait-for-completion": True,
            "request-params": {
                "request_timeout": 7200
            }
        }

        r = runner.RestoreSnapshot()
        await r(es, params)

        es.snapshot.restore.assert_called_once_with(repository="backups",
                                                    snapshot="snapshot-001",
                                                    body=None,
                                                    wait_for_completion=True,
                                                    params={"request_timeout": 7200})

    @mock.patch("elasticsearch.Elasticsearch")
    @run_async
    async def test_restore_snapshot_with_body(self, es):
        es.snapshot.restore.return_value = as_future()
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
        await r(es, params)

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
    @run_async
    async def test_waits_for_ongoing_indices_recovery(self, es):
        # empty response
        es.indices.recovery.side_effect = [
            # recovery did not yet start
            as_future({}),
            # active recovery - one shard is not yet finished
            as_future({
                "index1": {
                    "shards": [
                        {
                            "id": 0,
                            "type": "SNAPSHOT",
                            "stage": "INDEX",
                            "primary": True,
                            "start_time_in_millis": 1393244159716,
                            "stop_time_in_millis": 0,
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
                            "type": "SNAPSHOT",
                            "stage": "DONE",
                            "primary": True,
                            "start_time_in_millis": 1393244155000,
                            "stop_time_in_millis": 1393244158000,
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
            }),
            # completed
            as_future({
                "index1": {
                    "shards": [
                        {
                            "id": 0,
                            "type": "SNAPSHOT",
                            "stage": "DONE",
                            "primary": True,
                            "start_time_in_millis": 1393244159716,
                            "stop_time_in_millis": 1393244160000,
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
                            "type": "SNAPSHOT",
                            "stage": "DONE",
                            "primary": True,
                            "start_time_in_millis": 1393244155000,
                            "stop_time_in_millis": 1393244158000,
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
            }),
        ]

        r = runner.IndicesRecovery()

        result = await r(es, {
            "completion-recheck-wait-period": 0,
            "index": "index1"
        })

        # sum of both shards
        self.assertEqual(237783878, result["weight"])
        self.assertEqual("byte", result["unit"])
        self.assertTrue(result["success"])
        # 5 seconds
        self.assertEqual(5, result["service_time"])
        self.assertEqual(5, result["time_period"])

        es.indices.recovery.assert_called_with(index="index1")
        # retries three times
        self.assertEqual(3, es.indices.recovery.call_count)


class ShrinkIndexTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep", return_value=as_future())
    @run_async
    async def test_shrink_index_with_shrink_node(self, sleep, es):
        # cluster health API
        es.cluster.health.return_value = as_future({
            "status": "green",
            "relocating_shards": 0
        })
        es.indices.put_settings.return_value = as_future()
        es.indices.shrink.return_value = as_future()

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

        await r(es, params)

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
    @mock.patch("asyncio.sleep", return_value=as_future())
    @run_async
    async def test_shrink_index_derives_shrink_node(self, sleep, es):
        # cluster health API
        es.cluster.health.return_value = as_future({
            "status": "green",
            "relocating_shards": 0
        })
        es.nodes.info.return_value = as_future({
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
        })
        es.indices.put_settings.return_value = as_future()
        es.indices.shrink.return_value = as_future()

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

        await r(es, params)

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
    @run_async
    async def test_put_settings(self, es):
        es.cluster.put_settings.return_value = as_future()
        params = {
            "body": {
                "transient": {
                    "indices.recovery.max_bytes_per_sec": "20mb"
                }
            }
        }

        r = runner.PutSettings()
        await r(es, params)

        es.cluster.put_settings.assert_called_once_with(body={
            "transient": {
                "indices.recovery.max_bytes_per_sec": "20mb"
            }
        })


class RetryTests(TestCase):
    @run_async
    async def test_is_transparent_on_success_when_no_retries(self):
        delegate = mock.Mock(return_value=as_future())
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_is_transparent_on_exception_when_no_retries(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host")))
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionError):
            await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_is_transparent_on_application_error_when_no_retries(self):
        original_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(return_value=as_future(original_return_value))
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        self.assertEqual(original_return_value, result)
        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_is_does_not_retry_on_success(self):
        delegate = mock.Mock(return_value=as_future())
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.1,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_retries_on_timeout_if_wanted_and_raises_if_no_recovery(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=[
            as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host")),
            as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host")),
            as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host")),
            as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host"))
        ])
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionError):
            await retrier(es, params)

        delegate.assert_has_calls([
            mock.call(es, params),
            mock.call(es, params),
            mock.call(es, params)
        ])

    @run_async
    async def test_retries_on_timeout_if_wanted_and_returns_first_call(self):
        import elasticsearch
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(side_effect=[
            as_future(exception=elasticsearch.ConnectionError("N/A", "no route to host")),
            as_future(failed_return_value)
        ])
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": False
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)
        self.assertEqual(failed_return_value, result)

        delegate.assert_has_calls([
            # has returned a connection error
            mock.call(es, params),
            # has returned normally
            mock.call(es, params)
        ])

    @run_async
    async def test_retries_mixed_timeout_and_application_errors(self):
        import elasticsearch
        connection_error = elasticsearch.ConnectionError("N/A", "no route to host")
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(side_effect=[
            as_future(exception=connection_error),
            as_future(failed_return_value),
            as_future(exception=connection_error),
            as_future(exception=connection_error),
            as_future(failed_return_value),
            as_future(success_return_value)
        ])
        es = None
        params = {
            # we try exactly as often as there are errors to also test the semantics of "retry".
            "retries": 5,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)
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

    @run_async
    async def test_does_not_retry_on_timeout_if_not_wanted(self):
        import elasticsearch

        delegate = mock.Mock(side_effect=as_future(exception=elasticsearch.ConnectionTimeout(408, "timed out")))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": False,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        with self.assertRaises(elasticsearch.ConnectionTimeout):
            await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_retries_on_application_error_if_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        delegate = mock.Mock(side_effect=[
            as_future(failed_return_value),
            as_future(success_return_value)
        ])
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": False,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        self.assertEqual(success_return_value, result)

        delegate.assert_has_calls([
            mock.call(es, params),
            # one retry
            mock.call(es, params)
        ])

    @run_async
    async def test_does_not_retry_on_application_error_if_not_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.Mock(return_value=as_future(failed_return_value))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": False
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        self.assertEqual(failed_return_value, result)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_assumes_success_if_runner_returns_non_dict(self):
        delegate = mock.Mock(return_value=as_future(result=(1, "ops")))
        es = None
        params = {
            "retries": 3,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        self.assertEqual((1, "ops"), result)

        delegate.assert_called_once_with(es, params)

    @run_async
    async def test_retries_until_success(self):
        failure_count = 5

        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        responses = []
        responses += failure_count * [as_future(failed_return_value)]
        responses += [as_future(success_return_value)]

        delegate = mock.Mock(side_effect=responses)
        es = None
        params = {
            "retry-until-success": True,
            "retry-wait-period": 0.01
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        self.assertEqual(success_return_value, result)

        delegate.assert_has_calls([mock.call(es, params) for _ in range(failure_count + 1)])
