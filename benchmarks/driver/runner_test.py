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

import asyncio
import io
import json

import pytest
from elastic_transport import ApiResponse, ApiResponseMeta, HttpHeaders, NodeConfig

from esrally.driver import runner

bulk_index = runner.BulkIndex()

BULK_SIZE = 5000

BULK_BODY = (
    b'{"index": {"_index": "test"}}\n{"geonameid": 2986043, "name": "Pic de Font Blanca", "asciiname": "Pic de Font Blanca"}\n' * BULK_SIZE
)


class ElasticsearchMock:
    def __init__(self, bulk_size):
        self.meta = ApiResponseMeta(
            status=200,
            http_version="1.1",
            headers=HttpHeaders(),
            duration=0.1,
            node=NodeConfig("http", "localhost", 9200),
        )
        self.no_errors = {"took": 500, "errors": False, "items": []}
        for idx in range(0, bulk_size):
            self.no_errors["items"].append(
                {
                    "index": {
                        "_index": "test",
                        "_id": str(idx),
                        "_version": 1,
                        "result": "created",
                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                        "created": True,
                        "status": 201,
                        "_seq_no": 0,
                    }
                }
            )
        self.no_errors_raw = json.dumps(self.no_errors).encode()
        self.raw_response = False

    def return_raw_response(self):
        self.raw_response = True

    async def bulk(self, body=None, index=None, params=None):
        if self.raw_response:
            self.raw_response = False
            # the runner consumes the stream so it must be fresh on every call
            return ApiResponse(body=io.BytesIO(self.no_errors_raw), meta=self.meta)
        return ApiResponse(body=self.no_errors, meta=self.meta)


es = ElasticsearchMock(bulk_size=BULK_SIZE)


@pytest.fixture
def run():
    loop = asyncio.new_event_loop()
    yield lambda coro_fn, *args: loop.run_until_complete(coro_fn(*args))
    loop.close()


@pytest.mark.benchmark(
    group="bulk-runner",
    warmup="on",
    warmup_iterations=100,
    disable_gc=True,
)
@pytest.mark.parametrize("detailed_results", [False, True], ids=["no_detailed_results", "with_detailed_results"])
def test_bulk_runner_without_errors(benchmark, run, detailed_results):
    params = {
        "action-metadata-present": True,
        "body": BULK_BODY,
        "bulk-size": BULK_SIZE,
        "unit": "docs",
        "detailed-results": detailed_results,
    }
    result = benchmark(run, bulk_index, es, params)
    assert result["success"] is True
    assert result["success-count"] == BULK_SIZE
