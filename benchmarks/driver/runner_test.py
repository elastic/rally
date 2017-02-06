import pytest

from esrally.driver import runner

bulk_index = runner.BulkIndex()


class ElasticsearchMock:
    def __init__(self, bulk_size):
        self.no_errors = {
            "took": 500,
            "errors": False,
            "items": []
        }
        for idx in range(0, bulk_size):
            self.no_errors["items"].append({
                "index": {
                    "_index": "test",
                    "_type": "type1",
                    "_id": str(idx),
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
            })

    def bulk(self, body=None, index=None, doc_type=None, params=None):
        return self.no_errors


es = ElasticsearchMock(bulk_size=5000)


@pytest.mark.benchmark(
    group="bulk-runner",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_bulk_runner_without_errors_no_detailed_results(benchmark):
    benchmark(bulk_index, es, {
        "action_metadata_present": True,
        "body": "bulk API body"
    })


@pytest.mark.benchmark(
    group="bulk-runner",
    warmup="on",
    warmup_iterations=1000,
    disable_gc=True
)
def test_bulk_runner_without_errors_with_detailed_results(benchmark):
    benchmark(bulk_index, es, {
        "action_metadata_present": True,
        "body": "bulk API body",
        "detailed-results": True
    })
