import unittest.mock as mock
from unittest import TestCase

from esrally.driver import runner


class RegisterRunnerTests(TestCase):
    def tearDown(self):
        runner.remove_runner("unit_test")

    def test_runner_function_should_be_wrapped(self):
        def runner_function(es, params):
            pass
        runner.register_runner(operation_type="unit_test", runner=runner_function)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.DelegatingRunner)
        self.assertEqual("user-defined runner for [runner_function]", repr(returned_runner))

    def test_runner_class_with_context_manager_should_be_registered_as_is(self):
        class UnitTestRunner:
            def __enter__(self):
                return self

            def __call__(self, *args):
                pass

            def __exit__(self, exc_type, exc_val, exc_tb):
                return False

        test_runner = UnitTestRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertTrue(test_runner == returned_runner)

    def test_runner_class_should_be_wrapped(self):
        class UnitTestRunner:
            def __call__(self, *args):
                pass

            def __str__(self):
                return "UnitTestRunner"

        test_runner = UnitTestRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner)
        returned_runner = runner.runner_for("unit_test")
        self.assertIsInstance(returned_runner, runner.DelegatingRunner)
        self.assertEqual("user-defined runner for [UnitTestRunner]", repr(returned_runner))


class BulkIndexRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_success_with_metadata(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": [
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line"
            ],
            "action_metadata_present": True,
            "bulk-size": 3
        }

        result = bulk(es, bulk_params)

        self.assertIsNone(result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_success_without_metadata(self, es):
        es.bulk.return_value = {
            "errors": False
        }
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": [
                "index_line",
                "index_line",
                "index_line"
            ],
            "action_metadata_present": False,
            "bulk-size": 3,
            "index": "test-index",
            "type": "test-type"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test-index", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(True, result["success"])
        self.assertEqual(0, result["error-count"])

        es.bulk.assert_called_with(body=bulk_params["body"], index="test-index", doc_type="test-type", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_bulk_index_error(self, es):
        es.bulk.return_value = {
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
            "body": [
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line"
            ],
            "action_metadata_present": True,
            "bulk-size": 3,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(3, result["weight"])
        self.assertEqual(3, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(2, result["error-count"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_mixed_bulk_with_simple_stats(self, es):
        es.bulk.return_value = {
            "took": 30,
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
            "body": [
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line"
            ],
            "action_metadata_present": True,
            "detailed-results": False,
            "bulk-size": 4,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(4, result["weight"])
        self.assertEqual(4, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(2, result["error-count"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    def test_mixed_bulk_with_detailed_stats(self, es):
        es.bulk.return_value = {
            "took": 30,
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
            "body": [
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line"
            ],
            "action_metadata_present": True,
            "bulk-size": 6,
            "detailed-results": True,
            "index": "test"
        }

        result = bulk(es, bulk_params)

        self.assertEqual("test", result["index"])
        self.assertEqual(6, result["weight"])
        self.assertEqual(6, result["bulk-size"])
        self.assertEqual("docs", result["unit"])
        self.assertEqual(False, result["success"])
        self.assertEqual(3, result["error-count"])
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
        self.assertEqual(158, result["bulk-request-size-bytes"])
        self.assertEqual(62, result["total-document-size-bytes"])

        es.bulk.assert_called_with(body=bulk_params["body"], params={})


class QueryRunnerTests(TestCase):
    @mock.patch("elasticsearch.Elasticsearch")
    def test_query_match_all(self, es):
        es.search.return_value = {
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
            "index": "unittest",
            "type": "type",
            "use_request_cache": False,
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

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_only_one_page(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
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
            "items_per_page": 100,
            "index": "unittest",
            "type": "type",
            "use_request_cache": False,
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
        self.assertEqual("ops", results["unit"])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_with_explicit_number_of_pages(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
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
            # page 2
            {
                "_scroll_id": "some-scroll-id",
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
            "items_per_page": 100,
            "index": "unittest",
            "type": "type",
            "use_request_cache": False,
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
        self.assertEqual("ops", results["unit"])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_early_termination(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
            "hits": {
                "hits": [
                    {
                        "some-doc-1"
                    }
                ]
            }
        }
        es.transport.perform_request.side_effect = [
            # page 2 has no results
            {
                "_scroll_id": "some-scroll-id",
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
            "items_per_page": 100,
            "index": "unittest",
            "type": "type",
            "use_request_cache": False,
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
        self.assertEqual("ops", results["unit"])

    @mock.patch("elasticsearch.Elasticsearch")
    def test_scroll_query_request_all_pages(self, es):
        # page 1
        es.search.return_value = {
            "_scroll_id": "some-scroll-id",
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
        es.transport.perform_request.side_effect = [
            # page 2 has no results
            {
                "_scroll_id": "some-scroll-id",
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
            "items_per_page": 100,
            "index": "unittest",
            "type": "type",
            "use_request_cache": False,
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
        self.assertEqual("ops", results["unit"])
