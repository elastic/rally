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

"""
Throughput benchmarks for Rally runner implementations.

Measures runner processing overhead by running runners against mocked
Elasticsearch responses.  Uses pytest-benchmark for timing, statistics,
and structured JSON output (--benchmark-json).

Usage:
    pytest benchmarks/driver/runner_test.py -v -s
    pytest benchmarks/driver/runner_test.py --benchmark-json=results.json
"""

import asyncio
import io
import json

import pytest
from elastic_transport import ApiResponse, ApiResponseMeta, HttpHeaders, NodeConfig

from esrally.driver import runner

_HEADERS = HttpHeaders()
_HEADERS["content-type"] = "application/json"
_NODE = NodeConfig(scheme="http", host="localhost", port=9200)
BULK_META_200 = ApiResponseMeta(status=200, http_version="1.1", headers=_HEADERS, duration=0.1, node=_NODE)


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def make_simple_bulk_response():
    """ApiResponse with raw BytesIO body for the simple (non-detailed) stats path.

    The parse() function calls seek(0) before each read, so the same BytesIO
    is safely reusable across calls.
    """
    body_bytes = json.dumps({"errors": False, "took": 5}).encode()
    return ApiResponse(body=io.BytesIO(body_bytes), meta=BULK_META_200)


def make_detailed_bulk_response(bulk_size):
    """ApiResponse with dict body for the detailed stats path."""
    items = [
        {
            "index": {
                "_index": "test",
                "_id": str(i),
                "_version": 1,
                "result": "created",
                "_shards": {"total": 2, "successful": 1, "failed": 0},
                "status": 201,
                "_seq_no": i,
            }
        }
        for i in range(bulk_size)
    ]
    return ApiResponse(body={"took": 5, "errors": False, "items": items}, meta=BULK_META_200)


def make_bulk_params(bulk_size, detailed=False):
    """Build the params dict that BulkIndex.__call__ expects."""
    body_lines = []
    for i in range(bulk_size):
        body_lines.append(json.dumps({"index": {"_index": "test"}}))
        body_lines.append(json.dumps({"field": f"value_{i}", "ts": 1609780186}))
    return {
        "body": body_lines,
        "bulk-size": bulk_size,
        "unit": "docs",
        "action-metadata-present": True,
        "detailed-results": detailed,
    }


# ---------------------------------------------------------------------------
# Async ES mock
# ---------------------------------------------------------------------------


class AsyncESMock:
    """Minimal async-compatible Elasticsearch client mock for benchmarking.

    Returns a pre-built ApiResponse from bulk() without any I/O.
    The overhead is negligible, so timings reflect runner processing cost.
    """

    def __init__(self, response):
        self._response = response

    def return_raw_response(self):
        pass

    async def bulk(self, **kwargs):
        return self._response


class RetryAsyncESMock:
    """Mock that returns 429 errors on the initial call and success on retries.

    Distinguishes initial vs retry calls by comparing the body length against
    the known full body size.  This is stateless, so it works correctly across
    repeated benchmark iterations.
    """

    def __init__(self, initial_response, retry_response, full_body_len):
        self._initial = initial_response
        self._retry = retry_response
        self._full_body_len = full_body_len

    def return_raw_response(self):
        pass

    async def bulk(self, **kwargs):
        body = kwargs.get("body", [])
        if len(body) == self._full_body_len:
            return self._initial
        return self._retry


# ---------------------------------------------------------------------------
# Event loop fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def loop():
    _loop = asyncio.new_event_loop()
    yield _loop
    _loop.close()


# ---------------------------------------------------------------------------
# Response builders — 429 retry scenarios
# ---------------------------------------------------------------------------


def make_429_detailed_response(bulk_size, num_429):
    """Detailed (dict) bulk response where the first *num_429* items are 429s."""
    items = []
    for i in range(bulk_size):
        if i < num_429:
            items.append(
                {
                    "index": {
                        "_index": "test",
                        "_id": str(i),
                        "status": 429,
                        "error": {
                            "type": "es_rejected_execution_exception",
                            "reason": "rejected execution",
                        },
                    }
                }
            )
        else:
            items.append(
                {
                    "index": {
                        "_index": "test",
                        "_id": str(i),
                        "_version": 1,
                        "result": "created",
                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                        "status": 201,
                        "_seq_no": i,
                    }
                }
            )
    return ApiResponse(body={"took": 5, "errors": True, "items": items}, meta=BULK_META_200)


def make_429_simple_response(bulk_size, num_429):
    """Simple (BytesIO) bulk response where the first *num_429* items are 429s."""
    items = []
    for i in range(bulk_size):
        if i < num_429:
            items.append(
                {
                    "index": {
                        "_id": str(i),
                        "status": 429,
                        "error": {
                            "type": "es_rejected_execution_exception",
                            "reason": "rejected execution",
                        },
                    }
                }
            )
        else:
            items.append(
                {
                    "index": {
                        "_id": str(i),
                        "status": 201,
                        "result": "created",
                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                    }
                }
            )
    body = json.dumps({"took": 5, "errors": True, "items": items}).encode()
    return ApiResponse(body=io.BytesIO(body), meta=BULK_META_200)


# ---------------------------------------------------------------------------
# BulkIndex benchmarks
# ---------------------------------------------------------------------------

BULK_SIZES = [100, 500, 1000, 5000]


class TestBulkIndexThroughput:
    """Throughput benchmarks for the BulkIndex runner.

    Simple stats path:  parses only "errors" and "took" via streaming ijson.
    Detailed stats path: iterates every item in the response plus every bulk
    body line to compute per-op metrics, shard histograms, and byte sizes.
    """

    @pytest.mark.parametrize("bulk_size", BULK_SIZES)
    @pytest.mark.benchmark(
        group="bulk-index-simple",
        warmup=True,
        warmup_iterations=200,
        disable_gc=True,
    )
    def test_simple_stats(self, benchmark, loop, bulk_size):
        response = make_simple_bulk_response()
        es = AsyncESMock(response)
        params = make_bulk_params(bulk_size, detailed=False)
        bulk = runner.BulkIndex()

        benchmark.extra_info["bulk_size"] = bulk_size

        def run():
            loop.run_until_complete(bulk(es, params))

        benchmark(run)

    @pytest.mark.parametrize("bulk_size", BULK_SIZES)
    @pytest.mark.benchmark(
        group="bulk-index-detailed",
        warmup=True,
        warmup_iterations=200,
        disable_gc=True,
    )
    def test_detailed_stats(self, benchmark, loop, bulk_size):
        response = make_detailed_bulk_response(bulk_size)
        es = AsyncESMock(response)
        params = make_bulk_params(bulk_size, detailed=True)
        bulk = runner.BulkIndex()

        benchmark.extra_info["bulk_size"] = bulk_size

        def run():
            loop.run_until_complete(bulk(es, params))

        benchmark(run)


# ---------------------------------------------------------------------------
# BulkIndex retry benchmarks
# ---------------------------------------------------------------------------

RETRY_BULK_SIZES = [500, 5000]
RETRY_FRACTION = 0.1


class TestBulkIndexRetryThroughput:
    """Benchmarks for the BulkIndex retry-on-429 code path.

    Measures the overhead of detecting per-item 429 errors, building the
    retry body, re-issuing the bulk request, and accumulating stats.
    The initial response has RETRY_FRACTION of items rejected with 429;
    the retry always succeeds.
    """

    @pytest.mark.parametrize("bulk_size", RETRY_BULK_SIZES)
    @pytest.mark.benchmark(
        group="bulk-index-retry",
        warmup=True,
        warmup_iterations=200,
        disable_gc=True,
    )
    def test_retry_simple(self, benchmark, loop, bulk_size):
        num_429 = int(bulk_size * RETRY_FRACTION)
        initial = make_429_simple_response(bulk_size, num_429)
        retry_success = make_simple_bulk_response()
        es = RetryAsyncESMock(initial, retry_success, full_body_len=bulk_size * 2)
        params = make_bulk_params(bulk_size, detailed=False)
        params["retries_on_429"] = 1
        bulk = runner.BulkIndex()

        benchmark.extra_info.update({"bulk_size": bulk_size, "retry_fraction": RETRY_FRACTION})

        def run():
            loop.run_until_complete(bulk(es, params))

        benchmark(run)

    @pytest.mark.parametrize("bulk_size", RETRY_BULK_SIZES)
    @pytest.mark.benchmark(
        group="bulk-index-retry",
        warmup=True,
        warmup_iterations=200,
        disable_gc=True,
    )
    def test_retry_detailed(self, benchmark, loop, bulk_size):
        num_429 = int(bulk_size * RETRY_FRACTION)
        initial = make_429_detailed_response(bulk_size, num_429)
        retry_success = make_detailed_bulk_response(num_429)
        es = RetryAsyncESMock(initial, retry_success, full_body_len=bulk_size * 2)
        params = make_bulk_params(bulk_size, detailed=True)
        params["retries_on_429"] = 1
        bulk = runner.BulkIndex()

        benchmark.extra_info.update({"bulk_size": bulk_size, "retry_fraction": RETRY_FRACTION})

        def run():
            loop.run_until_complete(bulk(es, params))

        benchmark(run)
