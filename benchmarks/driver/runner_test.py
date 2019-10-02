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

import pytest

from esrally.driver import runner

bulk_index = runner.BulkIndex()

BULK_SIZE = 5000


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


es = ElasticsearchMock(bulk_size=BULK_SIZE)


@pytest.mark.benchmark(
    group="bulk-runner",
    warmup="on",
    warmup_iterations=10000,
    disable_gc=True
)
def test_bulk_runner_without_errors_no_detailed_results(benchmark):
    benchmark(bulk_index, es, {
        "action-metadata-present": True,
        "body": "bulk API body",
        "bulk-size": BULK_SIZE
    })


@pytest.mark.benchmark(
    group="bulk-runner",
    warmup="on",
    warmup_iterations=1000,
    disable_gc=True
)
def test_bulk_runner_without_errors_with_detailed_results(benchmark):
    benchmark(bulk_index, es, {
        "action-metadata-present": True,
        "body": "bulk API body",
        "bulk-size": BULK_SIZE,
        "detailed-results": True
    })
