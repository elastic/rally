import unittest.mock as mock
from unittest import TestCase

from esrally.driver import runner


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
            "action_metadata_present": True
        }

        result = bulk(es, bulk_params)

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
            "index": "test-index",
            "type": "test-type"
        }

        result = bulk(es, bulk_params)

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
            "action_metadata_present": True
        }

        result = bulk(es, bulk_params)

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
            "detailed-results": False
        }

        result = bulk(es, bulk_params)

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
            "detailed-results": True
        }

        result = bulk(es, bulk_params)

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

        es.bulk.assert_called_with(body=bulk_params["body"], params={})
