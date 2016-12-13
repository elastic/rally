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
                        "status": 201
                    }
                },
                {
                    "index": {
                        "status": 500
                    }
                },
                {
                    "index": {
                        "status": 404
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
