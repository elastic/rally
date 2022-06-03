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
import collections
import copy
import io
import json
import math
import random
import unittest.mock as mock

import elastic_transport
import elasticsearch
import pytest

from esrally import client, exceptions
from esrally.driver import runner


class BaseUnitTestContextManagerRunner:
    async def __aenter__(self):
        self.fp = io.StringIO("many\nlines\nin\na\nfile")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.fp.close()
        return False


class TestRegisterRunner:
    def teardown_method(self, method):
        runner.remove_runner("unit_test")

    @pytest.mark.asyncio
    async def test_runner_function_should_be_wrapped(self):
        async def runner_function(*args):
            return args

        runner.register_runner(operation_type="unit_test", runner=runner_function, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        assert isinstance(returned_runner, runner.NoCompletion)
        assert repr(returned_runner) == "user-defined runner for [runner_function]"
        assert await returned_runner({"default": "default_client", "other": "other_client"}, "param") == ("default_client", "param")

    @pytest.mark.asyncio
    async def test_single_cluster_runner_class_with_context_manager_should_be_wrapped_with_context_manager_enabled(self):
        class UnitTestSingleClusterContextManagerRunner(BaseUnitTestContextManagerRunner):
            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterContextManagerRunner"

        test_runner = UnitTestSingleClusterContextManagerRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        assert isinstance(returned_runner, runner.NoCompletion)
        assert repr(returned_runner) == "user-defined context-manager enabled runner for [UnitTestSingleClusterContextManagerRunner]"
        # test that context_manager functionality gets preserved after wrapping
        async with returned_runner:
            assert await returned_runner({"default": "default_client", "other": "other_client"}, "param") == ("default_client", "param")
        # check that the context manager interface of our inner runner has been respected.
        assert test_runner.fp.closed

    @pytest.mark.asyncio
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
        assert isinstance(returned_runner, runner.NoCompletion)
        assert repr(returned_runner) == "user-defined context-manager enabled runner for [UnitTestMultiClusterContextManagerRunner]"

        # test that context_manager functionality gets preserved after wrapping
        all_clients = {"default": "default_client", "other": "other_client"}
        async with returned_runner:
            assert await returned_runner(all_clients, "param1", "param2") == (all_clients, "param1", "param2")
        # check that the context manager interface of our inner runner has been respected.
        assert test_runner.fp.closed

    @pytest.mark.asyncio
    async def test_single_cluster_runner_class_should_be_wrapped(self):
        class UnitTestSingleClusterRunner:
            async def __call__(self, *args):
                return args

            def __str__(self):
                return "UnitTestSingleClusterRunner"

        test_runner = UnitTestSingleClusterRunner()
        runner.register_runner(operation_type="unit_test", runner=test_runner, async_runner=True)
        returned_runner = runner.runner_for("unit_test")
        assert isinstance(returned_runner, runner.NoCompletion)
        assert repr(returned_runner) == "user-defined runner for [UnitTestSingleClusterRunner]"
        assert await returned_runner({"default": "default_client", "other": "other_client"}, "param") == ("default_client", "param")

    @pytest.mark.asyncio
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
        assert isinstance(returned_runner, runner.NoCompletion)
        assert repr(returned_runner) == "user-defined runner for [UnitTestMultiClusterRunner]"
        all_clients = {"default": "default_client", "other": "other_client"}
        assert await returned_runner(all_clients, "some_param") == (all_clients, "some_param")


class TestAssertingRunner:
    def setup_method(self, method):
        runner.enable_assertions(True)

    def teardown_method(self, method):
        runner.enable_assertions(False)

    @pytest.mark.asyncio
    async def test_asserts_equal_succeeds(self):
        es = None
        response = {
            "hits": {
                "hits": {
                    "value": 5,
                    "relation": "eq",
                },
            },
        }
        delegate = mock.AsyncMock(return_value=response)
        r = runner.AssertingRunner(delegate)
        async with r:
            final_response = await r(
                es,
                {
                    "name": "test-task",
                    "assertions": [
                        {"property": "hits.hits.value", "condition": "==", "value": 5},
                        {"property": "hits.hits.relation", "condition": "==", "value": "eq"},
                    ],
                },
            )

        assert final_response == response

    @pytest.mark.asyncio
    async def test_asserts_equal_fails(self):
        es = None
        response = {
            "hits": {
                "hits": {
                    "value": 10000,
                    "relation": "gte",
                },
            },
        }
        delegate = mock.AsyncMock(return_value=response)
        r = runner.AssertingRunner(delegate)
        with pytest.raises(
            exceptions.RallyTaskAssertionError, match=r"Expected \[hits.hits.relation\] in \[test-task\] to be == \[eq\] but was \[gte\]."
        ):
            async with r:
                await r(
                    es,
                    {
                        "name": "test-task",
                        "assertions": [
                            {"property": "hits.hits.value", "condition": "==", "value": 10000},
                            {"property": "hits.hits.relation", "condition": "==", "value": "eq"},
                        ],
                    },
                )

    @pytest.mark.asyncio
    async def test_skips_asserts_for_non_dicts(self):
        es = None
        response = (1, "ops")
        delegate = mock.AsyncMock(return_value=response)
        r = runner.AssertingRunner(delegate)
        async with r:
            final_response = await r(
                es,
                {
                    "name": "test-task",
                    "assertions": [
                        {
                            "property": "hits.hits.value",
                            "condition": "==",
                            "value": 5,
                        },
                    ],
                },
            )
        # still passes response as is
        assert final_response == response

    def test_predicates(self):
        r = runner.AssertingRunner(delegate=None)
        assert len(r.predicates) == 5

        predicate_success = {
            # predicate: (expected, actual)
            ">": (5, 10),
            ">=": (5, 5),
            "<": (5, 4),
            "<=": (5, 5),
            "==": (5, 5),
        }

        for predicate, vals in predicate_success.items():
            expected, actual = vals
            assert r.predicates[predicate](expected, actual), f"Expected [{expected} {predicate} {actual}] to succeed."

        predicate_fail = {
            # predicate: (expected, actual)
            ">": (5, 5),
            ">=": (5, 4),
            "<": (5, 5),
            "<=": (5, 6),
            "==": (5, 6),
        }

        for predicate, vals in predicate_fail.items():
            expected, actual = vals
            assert not r.predicates[predicate](expected, actual), f"Expected [{expected} {predicate} {actual}] to fail."


class TestSelectiveJsonParser:
    def doc_as_text(self, doc):
        return io.StringIO(json.dumps(doc))

    def test_parse_all_expected(self):
        doc = self.doc_as_text(
            {
                "title": "Hello",
                "meta": {
                    "length": 100,
                    "date": {
                        "year": 2000,
                    },
                },
            },
        )

        parsed = runner.parse(
            doc,
            [
                # simple property
                "title",
                # a nested property
                "meta.date.year",
                # ignores unknown properties
                "meta.date.month",
            ],
        )

        assert parsed == {
            "title": "Hello",
            "meta.date.year": 2000,
        }

    def test_list_length(self):
        doc = self.doc_as_text(
            {
                "title": "Hello",
                "meta": {
                    "length": 100,
                    "date": {
                        "year": 2000,
                    },
                },
                "authors": ["George", "Harry"],
                "readers": [
                    {
                        "name": "Tom",
                        "age": 14,
                    },
                    {
                        "name": "Bob",
                        "age": 17,
                    },
                    {
                        "name": "Alice",
                        "age": 22,
                    },
                ],
                "supporters": [],
            }
        )

        parsed = runner.parse(
            doc,
            [
                # simple property
                "title",
                # a nested property
                "meta.date.year",
                # ignores unknown properties
                "meta.date.month",
            ],
            ["authors", "readers", "supporters"],
        )

        assert parsed == {
            "title": "Hello",
            "meta.date.year": 2000,
            "authors": False,
            "readers": False,
            "supporters": True,
        }


def _build_bulk_body(*lines):
    return "".join(line + "\n" for line in lines)


class TestBulkIndexRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_missing_params(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
            )
        }

        with pytest.raises(exceptions.DataError) as exc:
            await bulk(es, bulk_params)
        assert exc.value.args[0] == (
            "Parameter source for operation 'bulk-index' did not provide the mandatory parameter 'action-metadata-present'. "
            "Add it to your parameter source and try again."
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_success_with_timeout(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
            ),
            "action-metadata-present": True,
            "bulk-size": 3,
            "unit": "docs",
            "timeout": "1m",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 8,
            "index": None,
            "weight": 3,
            "unit": "docs",
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={"timeout": "1m"})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_success_with_metadata(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
            ),
            "action-metadata-present": True,
            "bulk-size": 3,
            "unit": "docs",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 8,
            "index": None,
            "weight": 3,
            "unit": "docs",
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_simple_bulk_with_timeout_and_headers(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "index_line",
                "index_line",
                "index_line",
            ),
            "action-metadata-present": False,
            "type": "_doc",
            "index": "test1",
            "request-timeout": 3.0,
            "headers": {"x-test-id": "1234"},
            "opaque-id": "DESIRED-OPAQUE-ID",
            "bulk-size": 3,
            "unit": "docs",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 8,
            "index": "test1",
            "weight": 3,
            "unit": "docs",
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        es.bulk.assert_awaited_with(
            doc_type="_doc",
            params={},
            body="index_line\nindex_line\nindex_line\n",
            headers={"x-test-id": "1234"},
            index="test1",
            opaque_id="DESIRED-OPAQUE-ID",
            request_timeout=3.0,
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_success_without_metadata_with_doc_type(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "index_line",
                "index_line",
                "index_line",
            ),
            "action-metadata-present": False,
            "bulk-size": 3,
            "unit": "docs",
            "index": "test-index",
            "type": "_doc",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 8,
            "index": "test-index",
            "weight": 3,
            "unit": "docs",
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], index="test-index", doc_type="_doc", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_success_without_metadata_and_without_doc_type(self, es):
        bulk_response = {
            "errors": False,
            "took": 8,
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "index_line",
                "index_line",
                "index_line",
            ),
            "action-metadata-present": False,
            "bulk-size": 3,
            "unit": "docs",
            "index": "test-index",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 8,
            "index": "test-index",
            "weight": 3,
            "unit": "docs",
            "success": True,
            "success-count": 3,
            "error-count": 0,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], index="test-index", doc_type=None, params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_error(self, es):
        bulk_response = {
            "took": 5,
            "errors": True,
            "items": [
                {"index": {"status": 201, "_shards": {"total": 2, "successful": 1, "failed": 0}}},
                {"index": {"status": 500, "_shards": {"total": 2, "successful": 0, "failed": 2}}},
                {"index": {"status": 404, "_shards": {"total": 2, "successful": 0, "failed": 2}}},
            ],
        }

        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
            ),
            "action-metadata-present": True,
            "bulk-size": 3,
            "unit": "docs",
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        result.pop("error-description")  # TODO not deterministic
        assert result == {
            "took": 5,
            "index": "test",
            "weight": 3,
            "unit": "docs",
            "success": False,
            "success-count": 1,
            "error-count": 2,
            "error-type": "bulk",
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
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
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@1]",  # pylint: disable=line-too-long
                    }
                },
                {
                    "create": {
                        "_index": "test",
                        "_type": "doc",
                        "_id": "2",
                        "status": 429,
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@2]",  # pylint: disable=line-too-long
                    }
                },
                {
                    "create": {
                        "_index": "test",
                        "_type": "doc",
                        "_id": "3",
                        "status": 429,
                        "error": "EsRejectedExecutionException[rejected execution (queue capacity 50) on org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction$PrimaryPhase$1@3]",  # pylint: disable=line-too-long
                    }
                },
            ],
        }

        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "index_line",
            ),
            "action-metadata-present": True,
            "detailed-results": False,
            "bulk-size": 3,
            "unit": "docs",
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        result.pop("error-description")  # TODO not deterministic
        assert result == {
            "took": 20,
            "index": "test",
            "weight": 3,
            "unit": "docs",
            "success": False,
            "success-count": 0,
            "error-count": 3,
            "error-type": "bulk",
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
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
                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                        "created": True,
                        "status": 201,
                        "_seq_no": 0,
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "2",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {"total": 2, "successful": 1, "failed": 0},
                        "status": 200,
                        "_seq_no": 1,
                    }
                },
                {
                    "index": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "3",
                        "_version": 1,
                        "result": "noop",
                        "_shards": {"total": 2, "successful": 0, "failed": 2},
                        "created": False,
                        "status": 500,
                        "_seq_no": -2,
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_type": "type1",
                        "_id": "6",
                        "_version": 2,
                        "result": "noop",
                        "_shards": {"total": 2, "successful": 0, "failed": 2},
                        "status": 404,
                        "_seq_no": 5,
                    }
                },
            ],
        }
        es.bulk = mock.AsyncMock(return_value=io.StringIO(json.dumps(bulk_response)))
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line",
                "action_meta_data",
                "index_line",
                "action_meta_data",
                "update_line",
            ),
            "action-metadata-present": True,
            "detailed-results": False,
            "bulk-size": 4,
            "unit": "docs",
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        result.pop("error-description")  # TODO not deterministic
        assert result == {
            "took": 30,
            "index": "test",
            "weight": 4,
            "unit": "docs",
            "success": False,
            "success-count": 2,
            "error-count": 2,
            "error-type": "bulk",
        }
        assert "ingest_took" not in result, "ingest_took is not extracted with simple stats"

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_mixed_bulk_with_detailed_stats_body_as_string(self, es):
        es.bulk = mock.AsyncMock(
            return_value={
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
                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                            "created": True,
                            "status": 201,
                            "_seq_no": 0,
                        }
                    },
                    {
                        "update": {
                            "_index": "test",
                            "_type": "type1",
                            "_id": "2",
                            "_version": 2,
                            "result": "updated",
                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                            "status": 200,
                            "_seq_no": 1,
                        }
                    },
                    {
                        "index": {
                            "_index": "test",
                            "_type": "type1",
                            "_id": "3",
                            "_version": 1,
                            "result": "noop",
                            "_shards": {"total": 2, "successful": 0, "failed": 2},
                            "created": False,
                            "status": 500,
                            "_seq_no": -2,
                        }
                    },
                    {
                        "index": {
                            "_index": "test",
                            "_type": "type1",
                            "_id": "4",
                            "_version": 1,
                            "result": "noop",
                            "_shards": {"total": 2, "successful": 1, "failed": 1},
                            "created": False,
                            "status": 500,
                            "_seq_no": -2,
                        }
                    },
                    {
                        "index": {
                            "_index": "test",
                            "_type": "type1",
                            "_id": "5",
                            "_version": 1,
                            "result": "created",
                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                            "created": True,
                            "status": 201,
                            "_seq_no": 4,
                        }
                    },
                    {
                        "update": {
                            "_index": "test",
                            "_type": "type1",
                            "_id": "6",
                            "_version": 2,
                            "result": "noop",
                            "_shards": {"total": 2, "successful": 0, "failed": 2},
                            "status": 404,
                            "_seq_no": 5,
                        }
                    },
                ],
            }
        )
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                '{ "index" : { "_index" : "test", "_type" : "type1" } }',
                '{"location" : [-0.1485188, 51.5250666]}',
                '{ "update" : { "_index" : "test", "_type" : "type1", "_id: "2" } }',
                '{"location" : [-0.1479949, 51.5252071]}',
                '{ "index" : { "_index" : "test", "_type" : "type1" } }',
                '{"location" : [-0.1458559, 51.5289059]}',
                '{ "index" : { "_index" : "test", "_type" : "type1" } }',
                '{"location" : [-0.1498551, 51.5282564]}',
                '{ "index" : { "_index" : "test", "_type" : "type1" } }',
                '{"location" : [-0.1487043, 51.5254843]}',
                '{ "update" : { "_index" : "test", "_type" : "type1", "_id: "3" } }',
                '{"location" : [-0.1533367, 51.5261779]}',
            ),
            "action-metadata-present": True,
            "bulk-size": 6,
            "unit": "docs",
            "detailed-results": True,
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        result.pop("error-description")  # TODO not deterministic
        assert result == {
            "took": 30,
            "ingest_took": 20,
            "index": "test",
            "weight": 6,
            "unit": "docs",
            "success": False,
            "success-count": 3,
            "error-count": 3,
            "error-type": "bulk",
            "ops": {
                "index": collections.Counter({"item-count": 4, "created": 2, "noop": 2}),
                "update": collections.Counter({"item-count": 2, "updated": 1, "noop": 1}),
            },
            "shards_histogram": [
                {"item-count": 3, "shards": {"total": 2, "successful": 1, "failed": 0}},
                {"item-count": 2, "shards": {"total": 2, "successful": 0, "failed": 2}},
                {"item-count": 1, "shards": {"total": 2, "successful": 1, "failed": 1}},
            ],
            "bulk-request-size-bytes": 582,
            "total-document-size-bytes": 234,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = await bulk(es, bulk_params)
        assert "ingest_took" not in result

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_simple_bulk_with_detailed_stats_body_as_list(self, es):
        es.bulk = mock.AsyncMock(
            return_value={
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
                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                            "created": True,
                            "status": 201,
                            "_seq_no": 0,
                        }
                    }
                ],
            }
        )
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                '{ "index" : { "_index" : "test", "_type" : "type1" } }',
                '{"location" : [-0.1485188, 51.5250666]}',
            ),
            "action-metadata-present": True,
            "bulk-size": 1,
            "unit": "docs",
            "detailed-results": True,
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 30,
            "ingest_took": 20,
            "index": "test",
            "weight": 1,
            "unit": "docs",
            "success": True,
            "success-count": 1,
            "error-count": 0,
            "ops": {
                "index": collections.Counter({"item-count": 1, "created": 1}),
            },
            "shards_histogram": [
                {
                    "item-count": 1,
                    "shards": {"total": 2, "successful": 1, "failed": 0},
                }
            ],
            "bulk-request-size-bytes": 93,
            "total-document-size-bytes": 39,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = await bulk(es, bulk_params)
        assert "ingest_took" not in result

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_simple_bulk_with_detailed_stats_body_as_bytes(self, es):
        es.bulk = mock.AsyncMock(
            return_value={
                "took": 30,
                "ingest_took": 20,
                "errors": False,
                "items": [
                    {
                        "index": {
                            "_index": "bytes",
                            "_type": "bytes1",
                            "_id": "1",
                            "_version": 1,
                            "result": "created",
                            "_shards": {"total": 1, "successful": 1, "failed": 0},
                            "created": True,
                            "status": 201,
                            "_seq_no": 0,
                        }
                    }
                ],
            }
        )
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": b'{ "index" : { "_index" : "bytes", "_type" : "bytes1" } }\n{"message" : "in a bottle"}',
            "action-metadata-present": True,
            "bulk-size": 1,
            "unit": "docs",
            "detailed-results": True,
            "index": "test",
        }

        result = await bulk(es, bulk_params)

        assert result == {
            "took": 30,
            "ingest_took": 20,
            "index": "test",
            "weight": 1,
            "unit": "docs",
            "success": True,
            "success-count": 1,
            "error-count": 0,
            "ops": {
                "index": collections.Counter({"item-count": 1, "created": 1}),
            },
            "shards_histogram": [
                {
                    "item-count": 1,
                    "shards": {"total": 1, "successful": 1, "failed": 0},
                }
            ],
            "bulk-request-size-bytes": 83,
            "total-document-size-bytes": 27,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

        es.bulk.return_value.pop("ingest_took")
        result = await bulk(es, bulk_params)
        assert "ingest_took" not in result

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_simple_bulk_with_detailed_stats_body_as_unrecognized_type(self, es):
        es.bulk = mock.AsyncMock(
            return_value={
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
                            "_shards": {"total": 2, "successful": 1, "failed": 0},
                            "created": True,
                            "status": 201,
                            "_seq_no": 0,
                        }
                    }
                ],
            }
        )
        bulk = runner.BulkIndex()

        bulk_params = {
            "body": {
                "items": 1,
            },
            "action-metadata-present": True,
            "bulk-size": 1,
            "unit": "docs",
            "detailed-results": True,
            "index": "test",
        }

        with pytest.raises(exceptions.DataError, match="bulk body is not of type bytes, string, or list"):
            await bulk(es, bulk_params)

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_bulk_index_error_logs_warning_with_detailed_stats_body(self, es):
        es.bulk = mock.AsyncMock(
            return_value={
                "took": 5,
                "errors": True,
                "items": [
                    {
                        "create": {
                            "_index": "test",
                            "_type": "_doc",
                            "_id": "6UNLsn0BfMD3e6iftbdV",
                            "status": 429,
                            "error": {
                                "type": "cluster_block_exception",
                                "reason": "index [test] blocked by: [TOO_MANY_REQUESTS/12/disk usage exceeded "
                                "flood-stage watermark, index has read-only-allow-delete block];",
                            },
                        }
                    }
                ],
            }
        )

        bulk = runner.BulkIndex()

        bulk_params = {
            "body": _build_bulk_body(
                '{ "index" : { "_index" : "test", "_type" : "_doc" } }',
                '{"message" : "in a bottle"}',
            ),
            "action-metadata-present": True,
            "bulk-size": 1,
            "unit": "docs",
            "detailed-results": True,
            "index": "test",
        }

        with mock.patch.object(bulk.logger, "warning") as mocked_warning_logger:
            result = await bulk(es, bulk_params)
            mocked_warning_logger.assert_has_calls([mock.call("Bulk request failed: [%s]", result["error-description"])])

        assert result == {
            "took": 5,
            "index": "test",
            "weight": 1,
            "unit": "docs",
            "success": False,
            "success-count": 0,
            "error-count": 1,
            "error-type": "bulk",
            "error-description": (
                "HTTP status: 429, message: index [test] blocked by: [TOO_MANY_REQUESTS/12/disk usage "
                "exceeded flood-stage watermark, index has read-only-allow-delete block];"
            ),
            "ops": {"create": collections.Counter({"item-count": 1})},
            "shards_histogram": [],
            "total-document-size-bytes": 27,
            "bulk-request-size-bytes": 80,
        }

        es.bulk.assert_awaited_with(body=bulk_params["body"], params={})


class TestForceMergeRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_defaults(self, es):
        es.indices.forcemerge = mock.AsyncMock()
        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all"})

        es.indices.forcemerge.assert_awaited_once_with(index="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_timeout_and_headers(self, es):
        es.indices.forcemerge = mock.AsyncMock()
        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "opaque-id": "test-id", "request-timeout": 3.0, "headers": {"header1": "value1"}})

        es.indices.forcemerge.assert_awaited_once_with(
            headers={"header1": "value1"}, index="_all", opaque_id="test-id", request_timeout=3.0
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_override_request_timeout(self, es):
        es.indices.forcemerge = mock.AsyncMock()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "request-timeout": 50000})

        es.indices.forcemerge.assert_awaited_once_with(index="_all", request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_params(self, es):
        es.indices.forcemerge = mock.AsyncMock()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "max-num-segments": 1, "request-timeout": 50000})

        es.indices.forcemerge.assert_awaited_with(index="_all", max_num_segments=1, request_timeout=50000)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_polling_no_timeout(self, es):
        es.indices.forcemerge = mock.AsyncMock()

        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "mode": "polling", "poll-period": 0})
        es.indices.forcemerge.assert_awaited_once_with(index="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_polling(self, es):
        es.indices.forcemerge = mock.AsyncMock(side_effect=elasticsearch.ConnectionTimeout(message="connection timeout"))
        es.tasks.list = mock.AsyncMock(
            side_effect=[
                {
                    "nodes": {
                        "Ap3OfntPT7qL4CBeKvamxg": {
                            "name": "instance-0000000001",
                            "transport_address": "10.46.79.231:19693",
                            "host": "10.46.79.231",
                            "ip": "10.46.79.231:19693",
                            "roles": ["data", "ingest", "master", "remote_cluster_client", "transform"],
                            "attributes": {
                                "logical_availability_zone": "zone-1",
                                "server_name": "instance-0000000001.64cb4c66f4f24d85b41f120ef2df5526",
                                "availability_zone": "us-east4-a",
                                "xpack.installed": "true",
                                "instance_configuration": "gcp.data.highio.1",
                                "transform.node": "true",
                                "region": "unknown-region",
                            },
                            "tasks": {
                                "Ap3OfntPT7qL4CBeKvamxg:417009036": {
                                    "node": "Ap3OfntPT7qL4CBeKvamxg",
                                    "id": 417009036,
                                    "type": "transport",
                                    "action": "indices:admin/forcemerge",
                                    "start_time_in_millis": 1598018980850,
                                    "running_time_in_nanos": 3659821411,
                                    "cancellable": False,
                                    "headers": {},
                                }
                            },
                        }
                    }
                },
                {
                    "nodes": {},
                },
            ]
        )
        force_merge = runner.ForceMerge()
        await force_merge(es, params={"index": "_all", "mode": "polling", "poll-period": 0})
        es.indices.forcemerge.assert_awaited_once_with(index="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_force_merge_with_polling_and_params(self, es):
        es.indices.forcemerge = mock.AsyncMock(return_value=elasticsearch.ConnectionTimeout("connection timeout"))
        es.tasks.list = mock.AsyncMock(
            side_effect=[
                {
                    "nodes": {
                        "Ap3OfntPT7qL4CBeKvamxg": {
                            "name": "instance-0000000001",
                            "transport_address": "10.46.79.231:19693",
                            "host": "10.46.79.231",
                            "ip": "10.46.79.231:19693",
                            "roles": ["data", "ingest", "master", "remote_cluster_client", "transform"],
                            "attributes": {
                                "logical_availability_zone": "zone-1",
                                "server_name": "instance-0000000001.64cb4c66f4f24d85b41f120ef2df5526",
                                "availability_zone": "us-east4-a",
                                "xpack.installed": "true",
                                "instance_configuration": "gcp.data.highio.1",
                                "transform.node": "true",
                                "region": "unknown-region",
                            },
                            "tasks": {
                                "Ap3OfntPT7qL4CBeKvamxg:417009036": {
                                    "node": "Ap3OfntPT7qL4CBeKvamxg",
                                    "id": 417009036,
                                    "type": "transport",
                                    "action": "indices:admin/forcemerge",
                                    "start_time_in_millis": 1598018980850,
                                    "running_time_in_nanos": 3659821411,
                                    "cancellable": False,
                                    "headers": {},
                                }
                            },
                        }
                    }
                },
                {
                    "nodes": {},
                },
            ]
        )
        force_merge = runner.ForceMerge()
        # request-timeout should be ignored as mode:polling
        await force_merge(
            es,
            params={
                "index": "_all",
                "mode": "polling",
                "max-num-segments": 1,
                "request-timeout": 50000,
                "poll-period": 0,
            },
        )
        es.indices.forcemerge.assert_awaited_once_with(index="_all", max_num_segments=1, request_timeout=50000)


class TestIndicesStatsRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_indices_stats_without_parameters(self, es):
        es.indices.stats = mock.AsyncMock(return_value={})
        indices_stats = runner.IndicesStats()
        result = await indices_stats(es, params={})
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.stats.assert_awaited_once_with(index="_all", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_indices_stats_with_timeout_and_headers(self, es):
        es.indices.stats = mock.AsyncMock(return_value={})
        indices_stats = runner.IndicesStats()
        result = await indices_stats(
            es,
            params={
                "request-timeout": 3.0,
                "headers": {"header1": "value1"},
                "opaque-id": "test-id1",
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.stats.assert_awaited_once_with(
            index="_all", metric="_all", headers={"header1": "value1"}, opaque_id="test-id1", request_timeout=3.0
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_indices_stats_with_failed_condition(self, es):
        es.indices.stats = mock.AsyncMock(
            return_value={
                "_all": {
                    "total": {
                        "merges": {
                            "current": 2,
                            "current_docs": 292698,
                        }
                    }
                }
            }
        )

        indices_stats = runner.IndicesStats()

        result = await indices_stats(
            es, params={"index": "logs-*", "condition": {"path": "_all.total.merges.current", "expected-value": 0}}
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "condition": {
                "path": "_all.total.merges.current",
                "actual-value": "2",
                "expected-value": "0",
            },
        }

        es.indices.stats.assert_awaited_once_with(index="logs-*", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_indices_stats_with_successful_condition(self, es):
        es.indices.stats = mock.AsyncMock(
            return_value={
                "_all": {
                    "total": {
                        "merges": {
                            "current": 0,
                            "current_docs": 292698,
                        }
                    }
                }
            }
        )

        indices_stats = runner.IndicesStats()

        result = await indices_stats(
            es,
            params={
                "index": "logs-*",
                "condition": {
                    "path": "_all.total.merges.current",
                    "expected-value": 0,
                },
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "condition": {
                "path": "_all.total.merges.current",
                "actual-value": "0",
                "expected-value": "0",
            },
        }

        es.indices.stats.assert_awaited_once_with(index="logs-*", metric="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_indices_stats_with_non_existing_path(self, es):
        es.indices.stats = mock.AsyncMock(return_value={"indices": {"total": {"docs": {"current": 0}}}})

        indices_stats = runner.IndicesStats()

        result = await indices_stats(
            es,
            params={
                "index": "logs-*",
                "condition": {
                    # non-existing path
                    "path": "indices.my_index.total.docs.count",
                    "expected-value": 0,
                },
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "condition": {
                "path": "indices.my_index.total.docs.count",
                "actual-value": None,
                "expected-value": "0",
            },
        }

        es.indices.stats.assert_awaited_once_with(index="logs-*", metric="_all")


class TestQueryRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_match_only_request_body_defined(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 1,
                    "relation": "gte",
                },
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "index": "_all",
            "detailed-results": True,
            "cache": True,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 1,
            "hits_relation": "gte",
            "timed_out": False,
            "took": 5,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET", path="/_all/_search", params={"request_cache": "true"}, body=params["body"], headers=None
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_with_timeout_and_headers(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {"value": 1, "relation": "gte"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "index": "_all",
            "detailed-results": True,
            "cache": True,
            "request-timeout": 3.0,
            "headers": {"header1": "value1"},
            "opaque-id": "test-id1",
            "body": {"query": {"match_all": {}}},
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 1,
            "hits_relation": "gte",
            "timed_out": False,
            "took": 5,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/_all/_search",
            # params={"request_timeout": 3.0, "request_cache": "true"},
            params={"request_cache": "true"},
            body=params["body"],
            headers={"header1": "value1", "x-opaque-id": "test-id1"},
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_match_using_request_params(self, es):
        response = {
            "timed_out": False,
            "took": 62,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq",
                },
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(response)))

        query_runner = runner.Query()
        params = {
            "operation-type": "search",
            "index": "_all",
            "cache": False,
            "detailed-results": True,
            "body": None,
            "request-params": {
                "q": "user:kimchy",
            },
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 62,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/_all/_search",
            params={
                "request_cache": "false",
                "q": "user:kimchy",
            },
            body=params["body"],
            headers=None,
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_no_detailed_results(self, es):
        response = {
            "timed_out": False,
            "took": 62,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq",
                },
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(response)))

        query_runner = runner.Query()
        params = {
            "operation-type": "search",
            "index": "_all",
            "body": None,
            "request-params": {"q": "user:kimchy"},
            "detailed-results": False,
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }
        assert "hits" not in result
        assert "hits_relation" not in result
        assert "timed_out" not in result
        assert "took" not in result
        assert "error-type" not in result

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/_all/_search",
            params={
                "q": "user:kimchy",
            },
            body=params["body"],
            headers=None,
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_hits_total_as_number(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": 2,
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "index": "_all",
            "cache": True,
            "detailed-results": True,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 5,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/_all/_search",
            params={
                "request_cache": "true",
            },
            body=params["body"],
            headers=None,
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_match_all(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {
                    "value": 2,
                    "relation": "eq",
                },
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "index": "unittest",
            "detailed-results": True,
            "response-compression-enabled": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 5,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/unittest/_search",
            params={},
            body=params["body"],
            headers={
                "Accept-Encoding": "identity",
            },
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_match_all_doc_type_fallback(self, es):
        search_response = {
            "timed_out": False,
            "took": 5,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "index": "unittest",
            "type": "type",
            "detailed-results": True,
            "cache": None,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            result = await query_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 5,
        }

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/unittest/type/_search",
            body=params["body"],
            params={},
            headers=None,
        )
        es.clear_scroll.assert_not_called()

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_scroll_query_only_one_page(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))
        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "cache": True,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 1,
            "pages": 1,
            "unit": "pages",
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 4,
        }
        assert "error-type" not in results

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/unittest/_search",
            params={
                "request_cache": "true",
                "sort": "_doc",
                "scroll": "10s",
                "size": 100,
            },
            body=params["body"],
            headers=None,
        )
        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_scroll_query_no_request_cache(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))
        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "response-compression-enabled": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 1,
            "pages": 1,
            "unit": "pages",
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 4,
        }
        assert "error-type" not in results

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/unittest/_search",
            params={"sort": "_doc", "scroll": "10s", "size": 100},
            body=params["body"],
            headers={"Accept-Encoding": "identity"},
        )
        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_scroll_query_only_one_page_only_request_body_defined(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))
        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "index": "_all",
            "pages": 1,
            "results-per-page": 100,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 1,
            "pages": 1,
            "unit": "pages",
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 4,
        }
        assert "error-type" not in results

        es.perform_request.assert_awaited_once_with(
            method="GET",
            path="/_all/_search",
            params={
                "sort": "_doc",
                "scroll": "10s",
                "size": 100,
            },
            body=params["body"],
            headers=None,
        )

        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
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
                    "relation": "eq",
                },
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        # page 2
        scroll_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": True,
            "took": 25,
            "hits": {
                "hits": [
                    {"title": "some-doc-3"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(
            side_effect=[
                io.StringIO(json.dumps(search_response)),
                io.StringIO(json.dumps(scroll_response)),
            ]
        )

        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "pages": 2,
            "results-per-page": 2,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 2,
            "pages": 2,
            "unit": "pages",
            "hits": 3,
            "hits_relation": "eq",
            "timed_out": True,
            "took": 79,
        }
        assert "error-type" not in results

        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_scroll_query_cannot_clear_scroll(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 53,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))
        es.clear_scroll = mock.AsyncMock(side_effect=elasticsearch.ConnectionTimeout(message="connection timeout"))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "pages": 5,
            "results-per-page": 100,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 1,
            "pages": 1,
            "unit": "pages",
            "hits": 1,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 53,
        }
        assert "error-type" not in results

        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_scroll_query_request_all_pages(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 876,
            "hits": {
                "total": {"value": 4, "relation": "gte"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                    {"title": "some-doc-3"},
                    {"title": "some-doc-4"},
                ],
            },
        }

        # page 2 has no results
        scroll_response = {
            "_scroll_id": "some-scroll-id",
            "timed_out": False,
            "took": 2,
            "hits": {
                "hits": [],
            },
        }

        es.perform_request = mock.AsyncMock(
            side_effect=[
                io.StringIO(json.dumps(search_response)),
                io.StringIO(json.dumps(scroll_response)),
            ]
        )
        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "scroll-search",
            "pages": "all",
            "results-per-page": 4,
            "index": "unittest",
            "cache": False,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        async with query_runner:
            results = await query_runner(es, params)

        assert results == {
            "weight": 2,
            "pages": 2,
            "unit": "pages",
            "hits": 4,
            "hits_relation": "gte",
            "timed_out": False,
            "took": 878,
        }
        assert "error-type" not in results

        es.clear_scroll.assert_awaited_once_with(body={"scroll_id": ["some-scroll-id"]})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_runner_search_with_pages_logs_warning_and_executes(self, es):
        # page 1
        search_response = {
            "_scroll_id": "some-scroll-id",
            "took": 4,
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"title": "some-doc-1"},
                    {"title": "some-doc-2"},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(search_response)))
        es.clear_scroll = mock.AsyncMock(return_value=io.StringIO('{"acknowledged": true}'))

        query_runner = runner.Query()

        params = {
            "operation-type": "search",
            "pages": 1,
            "results-per-page": 100,
            "index": "unittest",
            "cache": True,
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        with mock.patch.object(query_runner.logger, "warning") as mocked_warning_logger:
            results = await query_runner(es, params)
            mocked_warning_logger.assert_has_calls(
                [
                    mock.call(
                        "Invoking a scroll search with the 'search' operation is deprecated "
                        "and will be removed in a future release. Use 'scroll-search' instead."
                    )
                ]
            )

        assert results == {
            "weight": 1,
            "pages": 1,
            "unit": "pages",
            "hits": 2,
            "hits_relation": "eq",
            "timed_out": False,
            "took": 4,
        }
        assert "error-type" not in results

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_query_runner_fails_with_unknown_operation_type(self, es):
        query_runner = runner.Query()

        params = {
            "operation-type": "unknown",
            "index": "unittest",
            "body": {
                "query": {
                    "match_all": {},
                },
            },
        }

        with pytest.raises(exceptions.RallyError) as exc:
            await query_runner(es, params)
        assert exc.value.args[0] == "No runner available for operation-type: [unknown]"


class TestPutPipelineRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_pipeline(self, es):
        es.ingest.put_pipeline = mock.AsyncMock()

        r = runner.PutPipeline()

        params = {
            "id": "rename",
            "body": {
                "description": "describe pipeline",
                "processors": [
                    {
                        "set": {"field": "foo", "value": "bar"},
                    },
                ],
            },
        }

        await r(es, params)

        es.ingest.put_pipeline.assert_awaited_once_with(id="rename", body=params["body"], master_timeout=None, timeout=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_body_mandatory(self, es):
        es.ingest.put_pipeline = mock.AsyncMock()

        r = runner.PutPipeline()

        params = {"id": "rename"}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'body'. "
            "Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.ingest.put_pipeline.await_count == 0

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_id_mandatory(self, es):
        es.ingest.put_pipeline = mock.AsyncMock()

        r = runner.PutPipeline()

        params = {"body": {}}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'put-pipeline' did not provide the mandatory parameter 'id'. "
            "Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.ingest.put_pipeline.await_count == 0


class TestClusterHealthRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_waits_for_expected_cluster_status(self, es):
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        r = runner.ClusterHealth()

        params = {"request-params": {"wait_for_status": "green"}}

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0,
        }

        es.cluster.health.assert_awaited_once_with(params={"wait_for_status": "green"})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_accepts_better_cluster_status(self, es):
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        r = runner.ClusterHealth()

        params = {"request-params": {"wait_for_status": "yellow"}}

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0,
        }

        es.cluster.health.assert_awaited_once_with(params={"wait_for_status": "yellow"})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_cluster_health_with_timeout_and_headers(self, es):
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        cluster_health_runner = runner.ClusterHealth()

        params = {
            "request-params": {"wait_for_status": "yellow"},
            "request-timeout": 3.0,
            "headers": {"header1": "value1"},
            "opaque-id": "testid-1",
        }

        result = await cluster_health_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "cluster-status": "green",
            "relocating-shards": 0,
        }

        es.cluster.health.assert_awaited_once_with(
            headers={"header1": "value1"}, opaque_id="testid-1", params={"wait_for_status": "yellow"}, request_timeout=3.0
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_rejects_relocating_shards(self, es):
        es.cluster.health = mock.AsyncMock(return_value={"status": "yellow", "relocating_shards": 3})
        r = runner.ClusterHealth()

        params = {
            "index": "logs-*",
            "request-params": {
                "wait_for_status": "red",
                "wait_for_no_relocating_shards": True,
            },
        }

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "cluster-status": "yellow",
            "relocating-shards": 3,
        }

        es.cluster.health.assert_awaited_once_with(index="logs-*", params={"wait_for_status": "red", "wait_for_no_relocating_shards": True})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_rejects_unknown_cluster_status(self, es):
        es.cluster.health = mock.AsyncMock(return_value={"status": None, "relocating_shards": 0})
        r = runner.ClusterHealth()

        params = {"request-params": {"wait_for_status": "green"}}

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "cluster-status": None,
            "relocating-shards": 0,
        }

        es.cluster.health.assert_awaited_once_with(params={"wait_for_status": "green"})


class TestCreateIndexRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_creates_multiple_indices(self, es):
        es.indices.create = mock.AsyncMock()

        r = runner.CreateIndex()

        request_params = {"wait_for_active_shards": "true"}

        params = {
            "indices": [
                ("indexA", {"settings": {}}),
                ("indexB", {"settings": {}}),
            ],
            "request-params": request_params,
        }

        result = await r(es, params)

        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.indices.create.assert_has_awaits(
            [
                mock.call(index="indexA", body={"settings": {}}, params=request_params),
                mock.call(index="indexB", body={"settings": {}}, params=request_params),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_with_timeout_and_headers(self, es):
        es.indices.create = mock.AsyncMock()

        create_index_runner = runner.CreateIndex()

        request_params = {"wait_for_active_shards": "true"}

        params = {
            "indices": [
                ("indexA", {"settings": {}}),
            ],
            "request-timeout": 3.0,
            "headers": {"header1": "value1"},
            "opaque-id": "test-id1",
            "request-params": request_params,
        }

        result = await create_index_runner(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.create.assert_awaited_once_with(
            index="indexA",
            body={"settings": {}},
            headers={"header1": "value1"},
            opaque_id="test-id1",
            params={"wait_for_active_shards": "true"},
            request_timeout=3.0,
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_ignore_invalid_params(self, es):
        es.indices.create = mock.AsyncMock()

        r = runner.CreateIndex()

        request_params = {"wait_for_active_shards": "true"}

        params = {
            "indices": [
                ("indexA", {"settings": {}}),
            ],
            "index": "SHOULD-NOT-BE-PASSED",
            "body": "SHOULD-NOT-BE-PASSED",
            "request-params": request_params,
        }

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.create.assert_awaited_once_with(index="indexA", body={"settings": {}}, params={"wait_for_active_shards": "true"})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_indices_mandatory(self, es):
        es.indices.create = mock.AsyncMock()

        r = runner.CreateIndex()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'create-index' did not provide the mandatory parameter 'indices'. "
            "Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.create.await_count == 0


class TestCreateDataStreamRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_creates_multiple_data_streams(self, es):
        es.indices.create_data_stream = mock.AsyncMock()

        r = runner.CreateDataStream()

        request_params = {"wait_for_active_shards": "true"}

        params = {
            "data-streams": [
                "data-stream-A",
                "data-stream-B",
            ],
            "request-params": request_params,
        }

        result = await r(es, params)

        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.indices.create_data_stream.assert_has_awaits(
            [
                mock.call(name="data-stream-A", params=request_params),
                mock.call(name="data-stream-B", params=request_params),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_data_streams_mandatory(self, es):
        es.indices.create_data_stream = mock.AsyncMock()

        r = runner.CreateDataStream()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'create-data-stream' did not provide the "
            "mandatory parameter 'data-streams'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.create_data_stream.await_count == 0


class TestDeleteIndexRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_existing_indices(self, es):
        es.indices.exists = mock.AsyncMock(side_effect=[False, True])
        es.indices.delete = mock.AsyncMock()
        es.cluster.get_settings = mock.AsyncMock(return_value={"persistent": {}, "transient": {"action.destructive_requires_name": True}})
        es.cluster.put_settings = mock.AsyncMock()
        r = runner.DeleteIndex()

        params = {"indices": ["indexA", "indexB"], "only-if-exists": True}

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.cluster.put_settings.assert_has_awaits(
            [
                mock.call(body={"transient": {"action.destructive_requires_name": False}}),
                mock.call(body={"transient": {"action.destructive_requires_name": True}}),
            ]
        )
        es.indices.delete.assert_awaited_once_with(index="indexB", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_all_indices(self, es):
        es.indices.delete = mock.AsyncMock()
        es.cluster.get_settings = mock.AsyncMock(return_value={"persistent": {}, "transient": {}})
        es.cluster.put_settings = mock.AsyncMock()
        r = runner.DeleteIndex()

        params = {
            "indices": ["indexA", "indexB"],
            "only-if-exists": False,
            "request-params": {"ignore_unavailable": "true", "expand_wildcards": "none"},
        }

        result = await r(es, params)

        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.cluster.put_settings.assert_has_awaits(
            [
                mock.call(body={"transient": {"action.destructive_requires_name": False}}),
                mock.call(body={"transient": {"action.destructive_requires_name": None}}),
            ]
        )
        es.indices.delete.assert_has_awaits(
            [
                mock.call(index="indexA", params=params["request-params"]),
                mock.call(index="indexB", params=params["request-params"]),
            ]
        )
        assert es.indices.exists.call_count == 0


class TestDeleteDataStreamRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_existing_data_streams(self, es):
        es.indices.exists = mock.AsyncMock(side_effect=[False, True])
        es.indices.delete_data_stream = mock.AsyncMock()

        r = runner.DeleteDataStream()

        params = {"data-streams": ["data-stream-A", "data-stream-B"], "only-if-exists": True, "request-params": {}}

        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_data_stream.assert_awaited_once_with(name="data-stream-B", params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_all_data_streams(self, es):
        es.indices.delete_data_stream = mock.AsyncMock()
        es.indices.exists = mock.AsyncMock()

        r = runner.DeleteDataStream()

        params = {
            "data-streams": ["data-stream-A", "data-stream-B"],
            "only-if-exists": False,
            "request-params": {"ignore_unavailable": "true", "expand_wildcards": "none"},
        }

        result = await r(es, params)

        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_data_stream.assert_has_awaits(
            [
                mock.call(name="data-stream-A", ignore=[404], params=params["request-params"]),
                mock.call(name="data-stream-B", ignore=[404], params=params["request-params"]),
            ]
        )
        assert es.indices.exists.await_count == 0


class TestCreateIndexTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_index_templates(self, es):
        es.indices.put_template = mock.AsyncMock()

        r = runner.CreateIndexTemplate()

        params = {
            "templates": [
                ("templateA", {"settings": {}}),
                ("templateB", {"settings": {}}),
            ],
            "request-params": {"timeout": 50, "create": "true"},
        }

        result = await r(es, params)

        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.indices.put_template.assert_has_awaits(
            [
                mock.call(name="templateA", body={"settings": {}}, params=params["request-params"]),
                mock.call(name="templateB", body={"settings": {}}, params=params["request-params"]),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        es.indices.put_template = mock.AsyncMock()

        r = runner.CreateIndexTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'create-index-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.put_template.await_count == 0


class TestDeleteIndexTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_all_index_templates(self, es):
        es.indices.delete_template = mock.AsyncMock()
        es.indices.delete = mock.AsyncMock()

        r = runner.DeleteIndexTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                ("templateB", True, "logs-*"),
            ],
            "request-params": {"timeout": 60},
        }
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        assert result == {
            "weight": 3,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_template.assert_has_awaits(
            [mock.call(name="templateA", params=params["request-params"]), mock.call(name="templateB", params=params["request-params"])]
        )
        es.indices.delete.assert_awaited_once_with(index="logs-*")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_only_existing_index_templates(self, es):
        es.indices.exists_template = mock.AsyncMock(side_effect=[False, True])
        es.indices.delete_template = mock.AsyncMock()
        es.indices.delete = mock.AsyncMock()

        r = runner.DeleteIndexTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                # will not accidentally delete all indices
                ("templateB", True, ""),
            ],
            "request-params": {"timeout": 60},
            "only-if-exists": True,
        }
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_template.assert_awaited_once_with(name="templateB", params=params["request-params"])
        # not called because the matching index is empty.
        assert es.indices.delete.await_count == 0

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        es.indices.delete_template = mock.AsyncMock()
        r = runner.DeleteIndexTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'delete-index-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.delete_template.await_count == 0


class TestCreateComponentTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_index_templates(self, es):
        es.cluster.put_component_template = mock.AsyncMock()
        r = runner.CreateComponentTemplate()
        params = {
            "templates": [
                ("templateA", {"template": {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}}),
                ("templateB", {"template": {"settings": {"index.number_of_shards": 1, "index.number_of_replicas": 1}}}),
            ],
            "request-params": {"timeout": 50, "create": "true"},
        }

        result = await r(es, params)
        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }
        es.cluster.put_component_template.assert_has_awaits(
            [
                mock.call(
                    name="templateA",
                    body={"template": {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}},
                    params=params["request-params"],
                ),
                mock.call(
                    name="templateB",
                    body={"template": {"settings": {"index.number_of_shards": 1, "index.number_of_replicas": 1}}},
                    params=params["request-params"],
                ),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        es.cluster.put_component_template = mock.AsyncMock()

        r = runner.CreateComponentTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'create-component-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.cluster.put_component_template.await_count == 0


class TestDeleteComponentTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_all_index_templates(self, es):
        es.cluster.delete_component_template = mock.AsyncMock()

        r = runner.DeleteComponentTemplate()

        params = {
            "templates": [
                "templateA",
                "templateB",
            ],
            "request-params": {"timeout": 60},
            "only-if-exists": False,
        }
        result = await r(es, params)
        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }

        es.cluster.delete_component_template.assert_has_awaits(
            [
                mock.call(name="templateA", params=params["request-params"], ignore=[404]),
                mock.call(name="templateB", params=params["request-params"], ignore=[404]),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_only_existing_index_templates(self, es):
        es.cluster.exists_component_template = mock.AsyncMock(side_effect=[False, True])
        es.cluster.delete_component_template = mock.AsyncMock()

        r = runner.DeleteComponentTemplate()

        params = {
            "templates": [
                "templateA",
                "templateB",
            ],
            "request-params": {"timeout": 60},
            "only-if-exists": True,
        }
        result = await r(es, params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.cluster.delete_component_template.assert_awaited_once_with(name="templateB", params=params["request-params"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        es.indices.delete_template = mock.AsyncMock()
        r = runner.DeleteComponentTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'delete-component-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.delete_template.await_count == 0


class TestCreateComposableTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_index_templates(self, es):
        es.indices.put_index_template = mock.AsyncMock()
        r = runner.CreateComposableTemplate()
        params = {
            "templates": [
                (
                    "templateA",
                    {
                        "index_patterns": ["logs-*"],
                        "template": {"settings": {"index.number_of_shards": 3}},
                        "composed_of": ["ct1", "ct2"],
                    },
                ),
                (
                    "templateB",
                    {
                        "index_patterns": ["metrics-*"],
                        "template": {"settings": {"index.number_of_shards": 2}},
                        "composed_of": ["ct3", "ct4"],
                    },
                ),
            ],
            "request-params": {"timeout": 50},
        }

        result = await r(es, params)
        assert result == {
            "weight": 2,
            "unit": "ops",
            "success": True,
        }
        es.indices.put_index_template.assert_has_awaits(
            [
                mock.call(
                    name="templateA",
                    body={
                        "index_patterns": ["logs-*"],
                        "template": {
                            "settings": {
                                "index.number_of_shards": 3,
                            },
                        },
                        "composed_of": ["ct1", "ct2"],
                    },
                    params=params["request-params"],
                ),
                mock.call(
                    name="templateB",
                    body={
                        "index_patterns": ["metrics-*"],
                        "template": {
                            "settings": {
                                "index.number_of_shards": 2,
                            },
                        },
                        "composed_of": ["ct3", "ct4"],
                    },
                    params=params["request-params"],
                ),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        es.indices.put_index_template = mock.AsyncMock()

        r = runner.CreateComposableTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'create-composable-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.put_index_template.await_count == 0


class TestDeleteComposableTemplateRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_all_index_templates(self, es):
        es.indices.delete_index_template = mock.AsyncMock()
        es.indices.delete = mock.AsyncMock()

        r = runner.DeleteComposableTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                ("templateB", True, "logs-*"),
            ],
            "request-params": {"timeout": 60},
            "only-if-exists": False,
        }
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        assert result == {
            "weight": 3,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_index_template.assert_has_awaits(
            [
                mock.call(name="templateA", params=params["request-params"], ignore=[404]),
                mock.call(name="templateB", params=params["request-params"], ignore=[404]),
            ]
        )
        es.indices.delete.assert_awaited_once_with(index="logs-*")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_deletes_only_existing_index_templates(self, es):
        es.indices.exists_index_template = mock.AsyncMock(side_effect=[False, True])
        es.indices.delete_index_template = mock.AsyncMock()

        r = runner.DeleteComposableTemplate()

        params = {
            "templates": [
                ("templateA", False, None),
                # will not accidentally delete all indices
                ("templateB", True, ""),
            ],
            "request-params": {"timeout": 60},
            "only-if-exists": True,
        }
        result = await r(es, params)

        # 2 times delete index template, one time delete matching indices
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.indices.delete_index_template.assert_awaited_once_with(name="templateB", params=params["request-params"])
        # not called because the matching index is empty.
        assert es.indices.delete.call_count == 0

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_param_templates_mandatory(self, es):
        r = runner.DeleteComposableTemplate()

        params = {}
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'delete-composable-template' did not provide the mandatory parameter "
            "'templates'. Add it to your parameter source and try again.",
        ):
            await r(es, params)

        assert es.indices.delete_index_template.call_count == 0


class TestCreateMlDatafeed:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ml_datafeed(self, es):
        es.ml.put_datafeed = mock.AsyncMock()

        params = {"datafeed-id": "some-data-feed", "body": {"job_id": "total-requests", "indices": ["server-metrics"]}}

        r = runner.CreateMlDatafeed()
        await r(es, params)

        es.ml.put_datafeed.assert_awaited_once_with(datafeed_id=params["datafeed-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ml_datafeed_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.put_datafeed = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()
        datafeed_id = "some-data-feed"
        body = {"job_id": "total-requests", "indices": ["server-metrics"]}
        params = {"datafeed-id": datafeed_id, "body": body}

        r = runner.CreateMlDatafeed()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(method="PUT", path=f"/_xpack/ml/datafeeds/{datafeed_id}", body=body)


class TestDeleteMlDatafeed:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ml_datafeed(self, es):
        es.ml.delete_datafeed = mock.AsyncMock()

        datafeed_id = "some-data-feed"
        params = {"datafeed-id": datafeed_id}

        r = runner.DeleteMlDatafeed()
        await r(es, params)

        es.ml.delete_datafeed.assert_awaited_once_with(datafeed_id=datafeed_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ml_datafeed_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.delete_datafeed = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))

        es.perform_request = mock.AsyncMock()
        datafeed_id = "some-data-feed"
        params = {
            "datafeed-id": datafeed_id,
        }

        r = runner.DeleteMlDatafeed()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(
            method="DELETE", path=f"/_xpack/ml/datafeeds/{datafeed_id}", params={"force": "false", "ignore": 404}
        )


class TestStartMlDatafeed:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_start_ml_datafeed_with_body(self, es):
        es.ml.start_datafeed = mock.AsyncMock()
        params = {"datafeed-id": "some-data-feed", "body": {"end": "now"}}

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.ml.start_datafeed.assert_awaited_once_with(
            datafeed_id=params["datafeed-id"], body=params["body"], start=None, end=None, timeout=None
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_start_ml_datafeed_with_body_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.start_datafeed = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()
        body = {"end": "now"}
        params = {"datafeed-id": "some-data-feed", "body": body}

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(method="POST", path=f"/_xpack/ml/datafeeds/{params['datafeed-id']}/_start", body=body)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_start_ml_datafeed_with_params(self, es):
        es.ml.start_datafeed = mock.AsyncMock()
        params = {
            "datafeed-id": "some-data-feed",
            "start": "2017-01-01T01:00:00Z",
            "end": "now",
            "timeout": "10s",
        }

        r = runner.StartMlDatafeed()
        await r(es, params)

        es.ml.start_datafeed.assert_awaited_once_with(
            datafeed_id=params["datafeed-id"], body=None, start=params["start"], end=params["end"], timeout=params["timeout"]
        )


class TestStopMlDatafeed:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_stop_ml_datafeed(self, es):
        es.ml.stop_datafeed = mock.AsyncMock()
        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s",
        }

        r = runner.StopMlDatafeed()
        await r(es, params)

        es.ml.stop_datafeed.assert_awaited_once_with(datafeed_id=params["datafeed-id"], force=params["force"], timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_stop_ml_datafeed_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.stop_datafeed = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()

        params = {
            "datafeed-id": "some-data-feed",
            "force": random.choice([False, True]),
            "timeout": "5s",
        }

        r = runner.StopMlDatafeed()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(
            method="POST",
            path=f"/_xpack/ml/datafeeds/{params['datafeed-id']}/_stop",
            params={"force": str(params["force"]).lower(), "timeout": params["timeout"]},
        )


class TestCreateMlJob:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ml_job(self, es):
        es.ml.put_job = mock.AsyncMock()

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
                            "field_name": "total",
                        },
                    ],
                },
                "data_description": {"time_field": "timestamp", "time_format": "epoch_ms"},
            },
        }

        r = runner.CreateMlJob()
        await r(es, params)

        es.ml.put_job.assert_awaited_once_with(job_id=params["job-id"], body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ml_job_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.put_job = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()

        body = {
            "description": "Total sum of requests",
            "analysis_config": {
                "bucket_span": "10m",
                "detectors": [
                    {
                        "detector_description": "Sum of total",
                        "function": "sum",
                        "field_name": "total",
                    },
                ],
            },
            "data_description": {"time_field": "timestamp", "time_format": "epoch_ms"},
        }
        params = {"job-id": "an-ml-job", "body": body}

        r = runner.CreateMlJob()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(method="PUT", path=f"/_xpack/ml/anomaly_detectors/{params['job-id']}", body=body)


class TestDeleteMlJob:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ml_job(self, es):
        es.ml.delete_job = mock.AsyncMock()

        job_id = "an-ml-job"
        params = {"job-id": job_id}

        r = runner.DeleteMlJob()
        await r(es, params)

        es.ml.delete_job.assert_awaited_once_with(job_id=job_id, force=False, ignore=[404])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ml_job_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.delete_job = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()

        job_id = "an-ml-job"
        params = {"job-id": job_id}

        r = runner.DeleteMlJob()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(
            method="DELETE", path=f"/_xpack/ml/anomaly_detectors/{params['job-id']}", params={"force": "false", "ignore": 404}
        )


class TestOpenMlJob:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_open_ml_job(self, es):
        es.ml.open_job = mock.AsyncMock()

        job_id = "an-ml-job"
        params = {"job-id": job_id}

        r = runner.OpenMlJob()
        await r(es, params)

        es.ml.open_job.assert_awaited_once_with(job_id=job_id)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_open_ml_job_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.open_job = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()

        job_id = "an-ml-job"
        params = {"job-id": job_id}

        r = runner.OpenMlJob()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(method="POST", path=f"/_xpack/ml/anomaly_detectors/{params['job-id']}/_open")


class TestCloseMlJob:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_close_ml_job(self, es):
        es.ml.close_job = mock.AsyncMock()
        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s",
        }

        r = runner.CloseMlJob()
        await r(es, params)

        es.ml.close_job.assert_awaited_once_with(job_id=params["job-id"], force=params["force"], timeout=params["timeout"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_close_ml_job_fallback(self, es):
        error_meta = elastic_transport.ApiResponseMeta(status=400, http_version="1.1", headers=None, duration=0, node=None)
        es.ml.close_job = mock.AsyncMock(side_effect=elasticsearch.BadRequestError(message=400, meta=error_meta, body="Bad Request"))
        es.perform_request = mock.AsyncMock()

        params = {
            "job-id": "an-ml-job",
            "force": random.choice([False, True]),
            "timeout": "5s",
        }

        r = runner.CloseMlJob()
        await r(es, params)

        es.perform_request.assert_awaited_once_with(
            method="POST",
            path=f"/_xpack/ml/anomaly_detectors/{params['job-id']}/_close",
            params={"force": str(params["force"]).lower(), "timeout": params["timeout"]},
        )


class TestRawRequestRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_raises_missing_slash(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {"path": "_cat/count"}

        with mock.patch.object(r.logger, "error") as mocked_error_logger:
            with pytest.raises(exceptions.RallyAssertionError) as exc:
                await r(es, params)
                assert exc.value.args[0] == "RawRequest [_cat/count] failed. Path parameter must begin with a '/'."
            mocked_error_logger.assert_has_calls(
                [mock.call("RawRequest failed. Path parameter: [%s] must begin with a '/'.", params["path"])]
            )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_issue_request_with_defaults(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {"path": "/_cat/count"}
        await r(es, params)

        es.perform_request.assert_called_once_with(method="GET", path="/_cat/count", headers=None, body=None, params={})

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_issue_delete_index(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {
            "method": "DELETE",
            "path": "/twitter",
            "ignore": [400, 404],
            "request-params": {
                "pretty": "true",
            },
        }
        await r(es, params)

        es.perform_request.assert_called_once_with(
            method="DELETE", path="/twitter", headers=None, body=None, params={"ignore": [400, 404], "pretty": "true"}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_issue_create_index(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {
            "method": "POST",
            "path": "/twitter",
            "body": {
                "settings": {
                    "index": {
                        "number_of_replicas": 0,
                    },
                },
            },
        }
        await r(es, params)

        es.perform_request.assert_called_once_with(
            method="POST", path="/twitter", headers=None, body={"settings": {"index": {"number_of_replicas": 0}}}, params={}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_issue_msearch(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {
            "path": "/_msearch",
            "headers": {"Content-Type": "application/x-ndjson"},
            "body": [
                {"index": "test"},
                {"query": {"match_all": {}}, "from": 0, "size": 10},
                {"index": "test", "search_type": "dfs_query_then_fetch"},
                {"query": {"match_all": {}}},
            ],
        }
        await r(es, params)

        es.perform_request.assert_called_once_with(
            method="GET",
            path="/_msearch",
            headers={"Content-Type": "application/x-ndjson"},
            body=[
                {"index": "test"},
                {"query": {"match_all": {}}, "from": 0, "size": 10},
                {"index": "test", "search_type": "dfs_query_then_fetch"},
                {"query": {"match_all": {}}},
            ],
            params={},
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_raw_with_timeout_and_opaqueid(self, es):
        es.perform_request = mock.AsyncMock()
        r = runner.RawRequest()

        params = {
            "path": "/_msearch",
            "headers": {"Content-Type": "application/x-ndjson"},
            "request-timeout": 3.0,
            "opaque-id": "test-id1",
            "body": [
                {"index": "test"},
                {"query": {"match_all": {}}, "from": 0, "size": 10},
                {"index": "test", "search_type": "dfs_query_then_fetch"},
                {"query": {"match_all": {}}},
            ],
        }
        await r(es, params)

        es.perform_request.assert_called_once_with(
            method="GET",
            path="/_msearch",
            headers={"Content-Type": "application/x-ndjson", "x-opaque-id": "test-id1"},
            body=[
                {"index": "test"},
                {"query": {"match_all": {}}, "from": 0, "size": 10},
                {"index": "test", "search_type": "dfs_query_then_fetch"},
                {"query": {"match_all": {}}},
            ],
            params={"request_timeout": 3.0},
        )


class TestSleep:
    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_missing_parameter(self, sleep, es):
        r = runner.Sleep()
        with pytest.raises(
            exceptions.DataError,
            match="Parameter source for operation 'sleep' did not provide the mandatory parameter "
            "'duration'. Add it to your parameter source and try again.",
        ):
            await r(es, params={})

        assert es.call_count == 0
        assert es.on_request_start.call_count == 1
        assert es.on_request_end.call_count == 1
        assert sleep.call_count == 0

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_sleep(self, sleep, es):
        r = runner.Sleep()
        await r(es, params={"duration": 4.3})

        assert es.call_count == 0
        assert es.on_request_start.call_count == 1
        assert es.on_request_end.call_count == 1
        sleep.assert_called_once_with(4.3)


class TestDeleteSnapshotRepository:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_snapshot_repository(self, es):
        es.snapshot.delete_repository = mock.AsyncMock()
        params = {"repository": "backups"}

        r = runner.DeleteSnapshotRepository()
        await r(es, params)

        es.snapshot.delete_repository.assert_called_once_with(repository="backups")


class TestCreateSnapshotRepository:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_snapshot_repository(self, es):
        es.snapshot.create_repository = mock.AsyncMock()
        params = {
            "repository": "backups",
            "body": {
                "type": "fs",
                "settings": {
                    "location": "/var/backups",
                },
            },
        }

        r = runner.CreateSnapshotRepository()
        await r(es, params)

        es.snapshot.create_repository.assert_called_once_with(
            repository="backups", body={"type": "fs", "settings": {"location": "/var/backups"}}, params={}
        )


class TestCreateSnapshot:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_snapshot_no_wait(self, es):
        es.snapshot.create = mock.AsyncMock(return_value={})

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {"indices": "logs-*"},
            "wait-for-completion": False,
            "request-params": {"request_timeout": 7200},
        }

        r = runner.CreateSnapshot()
        await r(es, params)

        es.snapshot.create.assert_awaited_once_with(
            repository="backups",
            snapshot="snapshot-001",
            body={"indices": "logs-*"},
            params={"request_timeout": 7200},
            wait_for_completion=False,
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_snapshot_wait_for_completion(self, es):
        es.snapshot.create = mock.AsyncMock(
            return_value={
                "snapshot": {
                    "snapshot": "snapshot-001",
                    "uuid": "wjt6zFEIRua_-jutT5vrAw",
                    "version_id": 7070099,
                    "version": "7.7.0",
                    "indices": ["logs-2020-01-01"],
                    "include_global_state": False,
                    "state": "SUCCESS",
                    "start_time": "2020-06-10T07:38:53.811Z",
                    "start_time_in_millis": 1591774733811,
                    "end_time": "2020-06-10T07:38:55.015Z",
                    "end_time_in_millis": 1591774735015,
                    "duration_in_millis": 1204,
                    "failures": [],
                    "shards": {"total": 5, "failed": 0, "successful": 5},
                }
            }
        )

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {"indices": "logs-*"},
            "wait-for-completion": True,
            "request-params": {"request_timeout": 7200},
        }

        r = runner.CreateSnapshot()
        await r(es, params)

        es.snapshot.create.assert_awaited_once_with(
            repository="backups",
            snapshot="snapshot-001",
            body={"indices": "logs-*"},
            params={"request_timeout": 7200},
            wait_for_completion=True,
        )


class TestWaitForSnapshotCreate:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_wait_for_snapshot_create_entire_lifecycle(self, es):
        es.snapshot.get = mock.AsyncMock(
            side_effect=[
                # target snapshot running
                {"snapshots": [{"snapshot": "restore_speed_snapshot"}]},
                # different snapshot running
                {"snapshots": [{"snapshot": "different_snapshot"}]},
                {},
                {},
            ]
        )
        es.snapshot.status = mock.AsyncMock(
            side_effect=[
                # empty response
                {},
                # active snapshot
                {
                    "snapshots": [
                        {
                            "snapshot": "restore_speed_snapshot",
                            "repository": "restore_speed",
                            "uuid": "92efRcQxRCCwJuuC2lb-Ow",
                            "state": "STARTED",
                            "include_global_state": True,
                            "shards_stats": {
                                "initializing": 0,
                                "started": 10,
                                "finalizing": 0,
                                "done": 3,
                                "failed": 0,
                                "total": 13,
                            },
                            "stats": {
                                "incremental": {
                                    "file_count": 222,
                                    "size_in_bytes": 243468220144,
                                },
                                "processed": {
                                    "file_count": 18,
                                    "size_in_bytes": 82839346,
                                },
                                "total": {
                                    "file_count": 222,
                                    "size_in_bytes": 243468220144,
                                },
                                "start_time_in_millis": 1597319858606,
                                "time_in_millis": 6606,
                            },
                            "indices": {
                                # skipping content as we don"t parse this
                            },
                        }
                    ]
                },
                # completed
                {
                    "snapshots": [
                        {
                            "snapshot": "restore_speed_snapshot",
                            "repository": "restore_speed",
                            "uuid": "6gDpGbxOTpWKIutWdpWCFw",
                            "state": "SUCCESS",
                            "include_global_state": True,
                            "shards_stats": {
                                "initializing": 0,
                                "started": 0,
                                "finalizing": 0,
                                "done": 13,
                                "failed": 0,
                                "total": 13,
                            },
                            "stats": {
                                "incremental": {
                                    "file_count": 204,
                                    "size_in_bytes": 243468188055,
                                },
                                "total": {
                                    "file_count": 204,
                                    "size_in_bytes": 243468188055,
                                },
                                "start_time_in_millis": 1597317564956,
                                "time_in_millis": 1113462,
                            },
                            "indices": {
                                # skipping content here as don"t parse this
                            },
                        }
                    ]
                },
            ]
        )

        basic_params = {
            "repository": "restore_speed",
            "snapshot": "restore_speed_snapshot",
            "completion-recheck-wait-period": 0,
        }

        r = runner.WaitForSnapshotCreate()
        result = await r(es, basic_params)

        es.snapshot.status.assert_awaited_with(repository="restore_speed", snapshot="restore_speed_snapshot", ignore_unavailable=True)
        es.snapshot.get.assert_awaited_with(repository="restore_speed", snapshot="_current", verbose=False)

        assert result == {
            "weight": 243468188055,
            "unit": "byte",
            "success": True,
            "duration": 1113462,
            "file_count": 204,
            "throughput": 218658731.10622546,
            "start_time_millis": 1597317564956,
            "stop_time_millis": 1597317564956 + 1113462,
        }

        assert es.snapshot.status.await_count == 3
        assert es.snapshot.get.await_count == 4

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_wait_for_snapshot_create_immediate_success(self, es):
        es.snapshot.get = mock.AsyncMock(
            side_effect=[
                {},
            ]
        )
        es.snapshot.status = mock.AsyncMock(
            return_value={
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
                                "size_in_bytes": 9399505,
                            },
                            "start_time_in_millis": 1591776481060,
                            "time_in_millis": 200,
                        },
                    }
                ]
            }
        )

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
        }

        r = runner.WaitForSnapshotCreate()
        result = await r(es, params)

        assert result == {
            "weight": 9399505,
            "unit": "byte",
            "success": True,
            "duration": 200,
            "file_count": 70,
            "throughput": 46997525.0,
            "start_time_millis": 1591776481060,
            "stop_time_millis": 1591776481060 + 200,
        }

        es.snapshot.status.assert_awaited_once_with(repository="backups", snapshot="snapshot-001", ignore_unavailable=True)
        es.snapshot.get.assert_awaited_once_with(repository="backups", snapshot="_current", verbose=False)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_wait_for_snapshot_create_failure(self, es):
        es.snapshot.get = mock.AsyncMock(
            side_effect=[
                {},
            ]
        )
        snapshot_status = {
            "snapshots": [
                {
                    "snapshot": "snapshot-001",
                    "repository": "backups",
                    "state": "FAILED",
                    "include_global_state": False,
                },
            ]
        }
        es.snapshot.status = mock.AsyncMock(return_value=snapshot_status)

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
        }

        r = runner.WaitForSnapshotCreate()

        with mock.patch.object(r.logger, "error") as mocked_error_logger:
            with pytest.raises(exceptions.RallyAssertionError) as exc:
                await r(es, params)
                assert exc.value.args[0] == "Snapshot [snapshot-001] failed. Please check logs."
            mocked_error_logger.assert_has_calls(
                [mock.call("Snapshot [%s] failed. Response:\n%s", "snapshot-001", json.dumps(snapshot_status, indent=2))]
            )


class TestRestoreSnapshot:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_restore_snapshot(self, es):
        es.snapshot.restore = mock.AsyncMock()

        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "wait-for-completion": True,
            "request-params": {"request_timeout": 7200},
        }

        r = runner.RestoreSnapshot()
        await r(es, params)

        es.snapshot.restore.assert_awaited_once_with(
            repository="backups", snapshot="snapshot-001", wait_for_completion=True, params={"request_timeout": 7200}
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_restore_snapshot_with_body(self, es):
        es.snapshot.restore = mock.AsyncMock()
        params = {
            "repository": "backups",
            "snapshot": "snapshot-001",
            "body": {
                "indices": "index1,index2",
                "include_global_state": False,
                "index_settings": {
                    "index.number_of_replicas": 0,
                },
            },
            "wait-for-completion": True,
            "request-params": {"request_timeout": 7200},
        }

        r = runner.RestoreSnapshot()
        await r(es, params)

        es.snapshot.restore.assert_awaited_once_with(
            repository="backups",
            snapshot="snapshot-001",
            body={
                "indices": "index1,index2",
                "include_global_state": False,
                "index_settings": {
                    "index.number_of_replicas": 0,
                },
            },
            wait_for_completion=True,
            params={"request_timeout": 7200},
        )


class TestIndicesRecovery:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_waits_for_ongoing_indices_recovery(self, es):
        # empty response
        es.indices.recovery = mock.AsyncMock(
            side_effect=[
                # recovery did not yet start
                {},
                # recovery about to be started
                {
                    "index1": {
                        "shards": [
                            {
                                "id": 0,
                                "type": "SNAPSHOT",
                                "stage": "INIT",
                                "primary": True,
                                "start_time_in_millis": 1393244159716,
                                "index": {
                                    "size": {
                                        "total": "75.4mb",
                                        "total_in_bytes": 79063092,
                                        "recovered": "0mb",
                                        "recovered_in_bytes": 0,
                                    }
                                },
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
                                },
                            },
                        ]
                    }
                },
                # active recovery - one shard is not yet finished
                {
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
                                },
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
                                },
                            },
                        ]
                    }
                },
                # completed
                {
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
                                },
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
                                },
                            },
                        ]
                    }
                },
            ]
        )

        r = runner.IndicesRecovery()

        result = await r(es, {"completion-recheck-wait-period": 0, "index": "index1"})

        # sum of both shards
        assert result == {
            "weight": 237783878,
            "unit": "byte",
            "success": True,
            # bytes recovered within these 5 seconds
            "throughput": 47556775.6,
            "start_time_millis": 1393244155000,
            "stop_time_millis": 1393244160000,
        }

        es.indices.recovery.assert_awaited_with(index="index1")
        # retries four times
        assert es.indices.recovery.await_count == 4


class TestShrinkIndex:
    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_shrink_index_with_shrink_node(self, sleep, es):
        es.indices.get = mock.AsyncMock(return_value={"src": {}})
        # cluster health API
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        es.indices.put_settings = mock.AsyncMock()
        es.indices.shrink = mock.AsyncMock()

        r = runner.ShrinkIndex()
        params = {
            "source-index": "src",
            "target-index": "target",
            "target-body": {
                "settings": {
                    "index.number_of_replicas": 2,
                    "index.number_of_shards": 0,
                },
            },
            "shrink-node": "rally-node-0",
        }

        await r(es, params)

        es.indices.put_settings.assert_awaited_once_with(
            index="src",
            body={
                "settings": {
                    "index.routing.allocation.require._name": "rally-node-0",
                    "index.blocks.write": "true",
                },
            },
            preserve_existing=True,
        )

        es.cluster.health.assert_has_awaits(
            [
                mock.call(index="src", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="target", params={"wait_for_no_relocating_shards": "true"}),
            ]
        )

        es.indices.shrink.assert_awaited_once_with(
            index="src",
            target="target",
            body={
                "settings": {
                    "index.number_of_replicas": 2,
                    "index.number_of_shards": 0,
                    "index.routing.allocation.require._name": None,
                    "index.blocks.write": None,
                }
            },
        )

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_shrink_index_derives_shrink_node(self, sleep, es):
        es.indices.get = mock.AsyncMock(return_value={"src": {}})
        # cluster health API
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        es.nodes.info = mock.AsyncMock(
            return_value={
                "_nodes": {"total": 3, "successful": 3, "failed": 0},
                "cluster_name": "elasticsearch",
                "nodes": {
                    "lsM0-tKnQqKEGVw-OZU5og": {"name": "node0", "roles": ["master", "data", "ingest"]},
                    "kxM0-tKnQqKEGVw-OZU5og": {"name": "node1", "roles": ["master"]},
                    "yyM0-tKnQqKEGVw-OZU5og": {"name": "node0", "roles": ["ingest"]},
                },
            }
        )
        es.indices.put_settings = mock.AsyncMock()
        es.indices.shrink = mock.AsyncMock()

        r = runner.ShrinkIndex()
        params = {
            "source-index": "src",
            "target-index": "target",
            "target-body": {"settings": {"index.number_of_replicas": 2, "index.number_of_shards": 0}},
        }

        await r(es, params)

        es.indices.put_settings.assert_awaited_once_with(
            index="src",
            body={
                "settings": {
                    # the only data node in the cluster was chosen
                    "index.routing.allocation.require._name": "node0",
                    "index.blocks.write": "true",
                }
            },
            preserve_existing=True,
        )

        es.cluster.health.assert_has_awaits(
            [
                mock.call(index="src", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="target", params={"wait_for_no_relocating_shards": "true"}),
            ]
        )

        es.indices.shrink.assert_awaited_once_with(
            index="src",
            target="target",
            body={
                "settings": {
                    "index.number_of_replicas": 2,
                    "index.number_of_shards": 0,
                    "index.routing.allocation.require._name": None,
                    "index.blocks.write": None,
                }
            },
        )

    @mock.patch("elasticsearch.Elasticsearch")
    # To avoid real sleeps in unit tests
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_shrink_index_pattern_with_shrink_node(self, sleep, es):
        es.indices.get = mock.AsyncMock(return_value={"src1": {}, "src2": {}, "src-2020": {}})
        # cluster health API
        es.cluster.health = mock.AsyncMock(return_value={"status": "green", "relocating_shards": 0})
        es.indices.put_settings = mock.AsyncMock()
        es.indices.shrink = mock.AsyncMock()

        r = runner.ShrinkIndex()
        params = {
            "source-index": "src*",
            "target-index": "target",
            "target-body": {"settings": {"index.number_of_replicas": 2, "index.number_of_shards": 0}},
            "shrink-node": "rally-node-0",
        }

        await r(es, params)

        es.indices.put_settings.assert_has_awaits(
            [
                mock.call(
                    index="src1",
                    body={"settings": {"index.routing.allocation.require._name": "rally-node-0", "index.blocks.write": "true"}},
                    preserve_existing=True,
                ),
                mock.call(
                    index="src2",
                    body={"settings": {"index.routing.allocation.require._name": "rally-node-0", "index.blocks.write": "true"}},
                    preserve_existing=True,
                ),
                mock.call(
                    index="src-2020",
                    body={"settings": {"index.routing.allocation.require._name": "rally-node-0", "index.blocks.write": "true"}},
                    preserve_existing=True,
                ),
            ]
        )

        es.cluster.health.assert_has_awaits(
            [
                mock.call(index="src1", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="target1", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="src2", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="target2", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="src-2020", params={"wait_for_no_relocating_shards": "true"}),
                mock.call(index="target-2020", params={"wait_for_no_relocating_shards": "true"}),
            ]
        )

        es.indices.shrink.assert_has_awaits(
            [
                mock.call(
                    index="src1",
                    target="target1",
                    body={
                        "settings": {
                            "index.number_of_replicas": 2,
                            "index.number_of_shards": 0,
                            "index.routing.allocation.require._name": None,
                            "index.blocks.write": None,
                        }
                    },
                ),
                mock.call(
                    index="src2",
                    target="target2",
                    body={
                        "settings": {
                            "index.number_of_replicas": 2,
                            "index.number_of_shards": 0,
                            "index.routing.allocation.require._name": None,
                            "index.blocks.write": None,
                        }
                    },
                ),
                mock.call(
                    index="src-2020",
                    target="target-2020",
                    body={
                        "settings": {
                            "index.number_of_replicas": 2,
                            "index.number_of_shards": 0,
                            "index.routing.allocation.require._name": None,
                            "index.blocks.write": None,
                        }
                    },
                ),
            ]
        )


class TestPutSettings:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_put_settings(self, es):
        es.cluster.put_settings = mock.AsyncMock()
        params = {"body": {"transient": {"indices.recovery.max_bytes_per_sec": "20mb"}}}

        r = runner.PutSettings()
        await r(es, params)

        es.cluster.put_settings.assert_awaited_once_with(body={"transient": {"indices.recovery.max_bytes_per_sec": "20mb"}})


class TestCreateTransform:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_transform(self, es):
        es.transform.put_transform = mock.AsyncMock()

        params = {
            "transform-id": "a-transform",
            "body": {
                "source": {"index": "source"},
                "pivot": {
                    "group_by": {"event_id": {"terms": {"field": "event_id"}}},
                    "aggregations": {"max_metric": {"max": {"field": "metric"}}},
                },
                "description": "an example transform",
                "dest": {"index": "dest"},
            },
            "defer-validation": random.choice([False, True]),
        }

        r = runner.CreateTransform()
        await r(es, params)

        es.transform.put_transform.assert_awaited_once_with(
            transform_id=params["transform-id"], body=params["body"], defer_validation=params["defer-validation"]
        )


class TestStartTransform:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_start_transform(self, es):
        es.transform.start_transform = mock.AsyncMock()

        transform_id = "a-transform"
        params = {"transform-id": transform_id, "timeout": "5s"}

        r = runner.StartTransform()
        await r(es, params)

        es.transform.start_transform.assert_awaited_once_with(transform_id=transform_id, timeout=params["timeout"])


class TestWaitForTransform:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_wait_for_transform(self, es):
        es.transform.stop_transform = mock.AsyncMock()
        transform_id = "a-transform"
        params = {
            "transform-id": transform_id,
            "force": random.choice([False, True]),
            "timeout": "5s",
            "wait-for-completion": random.choice([False, True]),
            "wait-for-checkpoint": random.choice([False, True]),
        }

        es.transform.get_transform_stats = mock.AsyncMock(
            return_value={
                "count": 1,
                "transforms": [
                    {
                        "id": "a-transform",
                        "state": "stopped",
                        "stats": {
                            "pages_processed": 1,
                            "documents_processed": 2,
                            "documents_indexed": 3,
                            "trigger_count": 4,
                            "index_time_in_ms": 5,
                            "index_total": 6,
                            "index_failures": 7,
                            "search_time_in_ms": 8,
                            "search_total": 9,
                            "search_failures": 10,
                            "processing_time_in_ms": 11,
                            "processing_total": 12,
                            "exponential_avg_checkpoint_duration_ms": 13.13,
                            "exponential_avg_documents_indexed": 14.14,
                            "exponential_avg_documents_processed": 15.15,
                        },
                        "checkpointing": {"last": {"checkpoint": 1, "timestamp_millis": 16}, "changes_last_detected_at": 16},
                    }
                ],
            }
        )

        r = runner.WaitForTransform()
        assert not r.completed
        assert r.percent_completed == 0.0

        result = await r(es, params)

        assert r.completed
        assert r.percent_completed == 1.0
        assert result.pop("throughput")
        assert result == {
            "weight": 2,
            "unit": "docs",
            "success": True,
            "transform-id": transform_id,
        }

        es.transform.stop_transform.assert_awaited_once_with(
            transform_id=transform_id,
            force=params["force"],
            timeout=params["timeout"],
            wait_for_completion=False,
            wait_for_checkpoint=params["wait-for-checkpoint"],
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_wait_for_transform_progress(self, es):
        es.transform.stop_transform = mock.AsyncMock()
        transform_id = "a-transform"
        params = {
            "transform-id": transform_id,
            "force": random.choice([False, True]),
            "timeout": "5s",
        }

        # return 4 times, simulating progress
        es.transform.get_transform_stats = mock.AsyncMock(
            side_effect=[
                {
                    "count": 1,
                    "transforms": [
                        {
                            "id": "a-transform",
                            "state": "indexing",
                            "stats": {
                                "pages_processed": 1,
                                "documents_processed": 10000,
                                "documents_indexed": 3,
                                "trigger_count": 4,
                                "index_time_in_ms": 200,
                                "index_total": 6,
                                "index_failures": 7,
                                "search_time_in_ms": 300,
                                "search_total": 9,
                                "search_failures": 10,
                                "processing_time_in_ms": 50,
                                "processing_total": 12,
                                "exponential_avg_checkpoint_duration_ms": 13.13,
                                "exponential_avg_documents_indexed": 14.14,
                                "exponential_avg_documents_processed": 15.15,
                            },
                            "checkpointing": {
                                "last": {},
                                "next": {"checkpoint": 1, "timestamp_millis": 16, "checkpoint_progress": {"percent_complete": 10.20}},
                                "changes_last_detected_at": 16,
                            },
                        }
                    ],
                },
                {
                    "count": 1,
                    "transforms": [
                        {
                            "id": "a-transform",
                            "state": "indexing",
                            "stats": {
                                "pages_processed": 2,
                                "documents_processed": 20000,
                                "documents_indexed": 3,
                                "trigger_count": 4,
                                "index_time_in_ms": 500,
                                "index_total": 6,
                                "index_failures": 7,
                                "search_time_in_ms": 1500,
                                "search_total": 9,
                                "search_failures": 10,
                                "processing_time_in_ms": 300,
                                "processing_total": 12,
                                "exponential_avg_checkpoint_duration_ms": 13.13,
                                "exponential_avg_documents_indexed": 14.14,
                                "exponential_avg_documents_processed": 15.15,
                            },
                            "checkpointing": {
                                "last": {},
                                "next": {"checkpoint": 1, "timestamp_millis": 16, "checkpoint_progress": {"percent_complete": 20.40}},
                                "changes_last_detected_at": 16,
                            },
                        }
                    ],
                },
                {
                    "count": 1,
                    "transforms": [
                        {
                            "id": "a-transform",
                            "state": "started",
                            "stats": {
                                "pages_processed": 1,
                                "documents_processed": 30000,
                                "documents_indexed": 3,
                                "trigger_count": 4,
                                "index_time_in_ms": 1000,
                                "index_total": 6,
                                "index_failures": 7,
                                "search_time_in_ms": 2000,
                                "search_total": 9,
                                "search_failures": 10,
                                "processing_time_in_ms": 600,
                                "processing_total": 12,
                                "exponential_avg_checkpoint_duration_ms": 13.13,
                                "exponential_avg_documents_indexed": 14.14,
                                "exponential_avg_documents_processed": 15.15,
                            },
                            "checkpointing": {
                                "last": {},
                                "next": {"checkpoint": 1, "timestamp_millis": 16, "checkpoint_progress": {"percent_complete": 30.60}},
                                "changes_last_detected_at": 16,
                            },
                        }
                    ],
                },
                {
                    "count": 1,
                    "transforms": [
                        {
                            "id": "a-transform",
                            "state": "stopped",
                            "stats": {
                                "pages_processed": 1,
                                "documents_processed": 60000,
                                "documents_indexed": 3,
                                "trigger_count": 4,
                                "index_time_in_ms": 1500,
                                "index_total": 6,
                                "index_failures": 7,
                                "search_time_in_ms": 3000,
                                "search_total": 9,
                                "search_failures": 10,
                                "processing_time_in_ms": 1000,
                                "processing_total": 12,
                                "exponential_avg_checkpoint_duration_ms": 13.13,
                                "exponential_avg_documents_indexed": 14.14,
                                "exponential_avg_documents_processed": 15.15,
                            },
                            "checkpointing": {"last": {"checkpoint": 1, "timestamp_millis": 16}, "changes_last_detected_at": 16},
                        }
                    ],
                },
            ]
        )

        r = runner.WaitForTransform()
        assert not r.completed
        assert r.percent_completed == 0.0

        total_calls = 0
        while not r.completed:
            result = await r(es, params)
            total_calls += 1
            if total_calls < 4:
                assert round(abs(r.percent_completed - (total_calls * 10.20) / 100.0), 7) == 0

        assert total_calls == 4
        assert r.completed
        assert r.percent_completed == 1.0
        assert result.pop("throughput")
        assert result == {
            "weight": 60_000,
            "unit": "docs",
            "success": True,
            "transform-id": "a-transform",
        }

        es.transform.stop_transform.assert_awaited_once_with(
            transform_id=transform_id, force=params["force"], timeout=params["timeout"], wait_for_completion=False, wait_for_checkpoint=True
        )


class TestDeleteTransform:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_transform(self, es):
        es.transform.delete_transform = mock.AsyncMock()

        transform_id = "a-transform"
        params = {"transform-id": transform_id, "force": random.choice([False, True])}

        r = runner.DeleteTransform()
        await r(es, params)

        es.transform.delete_transform.assert_awaited_once_with(transform_id=transform_id, force=params["force"], ignore=[404])


class TestTransformStatsRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_transform_stats_with_timeout_and_headers(self, es):
        es.transform.get_transform_stats = mock.AsyncMock(return_value={})
        transform_stats = runner.TransformStats()
        transform_id = "a-transform"
        result = await transform_stats(
            es,
            params={
                "transform-id": transform_id,
                "request-timeout": 3.0,
                "headers": {"header1": "value1"},
                "opaque-id": "test-id1",
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.transform.get_transform_stats.assert_awaited_once_with(
            transform_id=transform_id,
            headers={"header1": "value1"},
            opaque_id="test-id1",
            request_timeout=3.0,
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_transform_stats_with_failed_condition(self, es):
        transform_id = "a-transform"
        es.transform.get_transform_stats = mock.AsyncMock(
            return_value={
                "count": 3,
                "transforms": [
                    {
                        "id": transform_id,
                        "state": "started",
                        "stats": {},
                        "checkpointing": {
                            "last": {},
                            "operations_behind": 10000,
                        },
                    }
                ],
            }
        )

        transform_stats = runner.TransformStats()

        result = await transform_stats(
            es,
            params={
                "transform-id": transform_id,
                "condition": {
                    "path": "checkpointing.operations_behind",
                    "expected-value": None,
                },
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "condition": {
                "path": "checkpointing.operations_behind",
                "actual-value": "10000",
                "expected-value": None,
            },
        }
        es.transform.get_transform_stats.assert_awaited_once_with(transform_id=transform_id)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_transform_stats_with_successful_condition(self, es):
        transform_id = "a-transform"
        es.transform.get_transform_stats = mock.AsyncMock(
            return_value={
                "count": 3,
                "transforms": [
                    {
                        "id": transform_id,
                        "state": "started",
                        "stats": {},
                        "checkpointing": {
                            "last": {},
                        },
                    }
                ],
            }
        )

        transform_stats = runner.TransformStats()

        result = await transform_stats(
            es,
            params={
                "transform-id": transform_id,
                "condition": {
                    "path": "checkpointing.operations_behind",
                    "expected-value": None,
                },
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
            "condition": {
                "path": "checkpointing.operations_behind",
                "actual-value": None,
                "expected-value": None,
            },
        }

        es.transform.get_transform_stats.assert_awaited_once_with(transform_id=transform_id)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_transform_stats_with_non_existing_path(self, es):
        transform_id = "a-transform"
        es.transform.get_transform_stats = mock.AsyncMock(
            return_value={
                "count": 3,
                "transforms": [
                    {
                        "id": transform_id,
                        "state": "started",
                        "stats": {},
                        "checkpointing": {
                            "last": {},
                        },
                    }
                ],
            }
        )

        transform_stats = runner.TransformStats()

        result = await transform_stats(
            es,
            params={
                "transform-id": transform_id,
                "condition": {
                    "path": "checkpointing.last.checkpoint",
                    "expected-value": 42,
                },
            },
        )
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": False,
            "condition": {
                "path": "checkpointing.last.checkpoint",
                "actual-value": None,
                "expected-value": "42",
            },
        }

        es.transform.get_transform_stats.assert_awaited_once_with(transform_id=transform_id)


class TestCreateIlmPolicyRunner:

    params = {
        "policy-name": "my-ilm-policy",
        "request-params": {"master_timeout": "30s", "timeout": "30s"},
        "body": {
            "policy": {
                "phases": {"hot": {"min_age": "0ms", "actions": {"rollover": {"max_age": "30d"}, "set_priority": {"priority": 100}}}}
            }
        },
    }

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ilm_policy_with_request_params(self, es):
        es.ilm.put_lifecycle = mock.AsyncMock(return_value={})
        create_ilm_policy = runner.CreateIlmPolicy()
        result = await create_ilm_policy(es, params=self.params)

        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.ilm.put_lifecycle.assert_awaited_once_with(
            policy=self.params["policy-name"], body=self.params["body"], params=self.params["request-params"]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_create_ilm_policy_without_request_params(self, es):
        es.ilm.put_lifecycle = mock.AsyncMock(return_value={})
        create_ilm_policy = runner.CreateIlmPolicy()
        params = copy.deepcopy(self.params)
        del params["request-params"]
        result = await create_ilm_policy(es, params=params)
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.ilm.put_lifecycle.assert_awaited_once_with(policy=params["policy-name"], body=params["body"], params={})


class TestDeleteIlmPolicyRunner:

    params = {"policy-name": "my-ilm-policy", "request-params": {"master_timeout": "30s", "timeout": "30s"}}

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ilm_policy_with_request_params(self, es):
        es.ilm.delete_lifecycle = mock.AsyncMock(return_value={})
        delete_ilm_policy = runner.DeleteIlmPolicy()
        result = await delete_ilm_policy(es, params=self.params)
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.ilm.delete_lifecycle.assert_awaited_once_with(policy=self.params["policy-name"], params=self.params["request-params"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_ilm_policy_without_request_params(self, es):
        es.ilm.delete_lifecycle = mock.AsyncMock(return_value={})
        delete_ilm_policy = runner.DeleteIlmPolicy()
        params = copy.deepcopy(self.params)
        del params["request-params"]
        result = await delete_ilm_policy(es, params=params)
        assert result == {
            "weight": 1,
            "unit": "ops",
            "success": True,
        }

        es.ilm.delete_lifecycle.assert_awaited_once_with(policy=params["policy-name"], params={})


class TestSqlRunner:
    default_response = {
        "columns": [{"name": "first_name", "type": "text"}],
        "rows": [
            ["Georgi"],
            ["Bezalel"],
        ],
        "cursor": "firstCursor",
    }

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_fetch_one_page(self, es):
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(self.default_response)))

        sql_runner = runner.Sql()
        params = {
            "operation-type": "sql",
            "body": {
                "query": "SELECT first_name FROM emp",
                "fetch_size": 2,
                "page_timeout": "10s",
                "request_timeout": "20s",
            },
        }

        async with sql_runner:
            result = await sql_runner(es, params)

        assert result == {"success": True, "weight": 1, "unit": "ops"}

        es.perform_request.assert_awaited_once_with(method="POST", path="/_sql", body=params["body"])

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_fetch_all_pages(self, es):
        es.perform_request = mock.AsyncMock(return_value=io.StringIO(json.dumps(self.default_response)))

        sql_runner = runner.Sql()
        params = {"operation-type": "sql", "body": {"query": "SELECT first_name FROM emp"}, "pages": 3}

        async with sql_runner:
            result = await sql_runner(es, params)

        assert result == {"success": True, "weight": 3, "unit": "ops"}

        es.perform_request.assert_has_calls(
            [
                mock.call(method="POST", path="/_sql", body=params["body"]),
                mock.call(method="POST", path="/_sql", body={"cursor": self.default_response["cursor"]}),
                mock.call(method="POST", path="/_sql", body={"cursor": self.default_response["cursor"]}),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_failure_on_too_few_pages(self, es):
        es.perform_request = mock.AsyncMock(
            side_effect=[io.StringIO(json.dumps(self.default_response)), io.StringIO(json.dumps({"rows": [["John"]]}))]
        )

        sql_runner = runner.Sql()
        params = {
            "operation-type": "sql",
            "body": {
                "query": "SELECT first_name FROM emp",
            },
            "pages": 3,
        }

        with pytest.raises(exceptions.DataError) as ctx:
            async with sql_runner:
                await sql_runner(es, params)

        assert ctx.value.message == "Result set has been exhausted before all pages have been fetched, 1 page(s) remaining."

        es.perform_request.assert_has_calls(
            [
                mock.call(method="POST", path="/_sql", body=params["body"]),
                mock.call(method="POST", path="/_sql", body={"cursor": self.default_response["cursor"]}),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_mandatory_body_param(self, es):
        sql_runner = runner.Sql()
        params = {
            "operation-type": "sql",
            "pages": 3,
        }

        with pytest.raises(exceptions.DataError) as exc:
            await sql_runner(es, params)
        assert exc.value.args[0] == (
            "Parameter source for operation 'sql' did not provide the mandatory parameter 'body'. "
            "Add it to your parameter source and try again."
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_mandatory_query_in_body_param(self, es):
        sql_runner = runner.Sql()
        params = {
            "operation-type": "sql",
            "body": {},
            "pages": 3,
        }

        with pytest.raises(exceptions.DataError) as exc:
            await sql_runner(es, params)
        assert exc.value.args[0] == (
            "Parameter source for operation 'sql' did not provide the mandatory parameter 'body.query'. "
            "Add it to your parameter source and try again."
        )


class TestSubmitAsyncSearch:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_submit_async_search(self, es):
        es.async_search.submit = mock.AsyncMock(return_value={"id": "12345"})
        r = runner.SubmitAsyncSearch()
        params = {
            "name": "search-1",
            "body": {
                "query": {
                    "match_all": {},
                },
            },
            "index": "_all",
        }

        async with runner.CompositeContext():
            await r(es, params)
            # search id is registered in context
            assert runner.CompositeContext.get("search-1") == "12345"

        es.async_search.submit.assert_awaited_once_with(body={"query": {"match_all": {}}}, index="_all", params={})


class TestGetAsyncSearch:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_get_async_search(self, es):
        es.async_search.get = mock.AsyncMock(
            return_value={
                "is_running": False,
                "response": {
                    "took": 1122,
                    "timed_out": False,
                    "hits": {
                        "total": {
                            "value": 1520,
                            "relation": "eq",
                        },
                    },
                },
            }
        )
        r = runner.GetAsyncSearch()
        params = {"retrieve-results-for": "search-1"}

        async with runner.CompositeContext():
            runner.CompositeContext.put("search-1", "12345")
            response = await r(es, params)
            assert response == {
                "weight": 1,
                "unit": "ops",
                "success": True,
                "stats": {
                    "search-1": {
                        "hits": 1520,
                        "hits_relation": "eq",
                        "timed_out": False,
                        "took": 1122,
                    },
                },
            }

        es.async_search.get.assert_awaited_once_with(id="12345", params={})


class TestDeleteAsyncSearch:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_delete_async_search(self, es):
        es.async_search.delete = mock.AsyncMock(side_effect=[{}, {}])
        r = runner.DeleteAsyncSearch()
        params = {"delete-results-for": ["search-1", "search-2", "search-3"]}

        async with runner.CompositeContext():
            runner.CompositeContext.put("search-1", "12345")
            runner.CompositeContext.put("search-2", None)
            runner.CompositeContext.put("search-3", "6789")
            await r(es, params)

        es.async_search.delete.assert_has_awaits(
            [
                mock.call(id="12345"),
                mock.call(id="6789"),
            ]
        )


class TestOpenPointInTime:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_creates_point_in_time(self, es):
        pit_id = "0123456789abcdef"
        params = {"name": "open-pit-test", "index": "test-index"}

        es.open_point_in_time = mock.AsyncMock(return_value={"id": pit_id})

        r = runner.OpenPointInTime()
        async with runner.CompositeContext():
            await r(es, params)
            assert runner.CompositeContext.get("open-pit-test") == pit_id

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_can_only_be_run_in_composite(self, es):
        pit_id = "0123456789abcdef"
        params = {"name": "open-pit-test", "index": "test-index"}

        es.open_point_in_time = mock.AsyncMock(return_value={"id": pit_id})

        r = runner.OpenPointInTime()
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            await r(es, params)

        assert exc.value.args[0] == "This operation is only allowed inside a composite operation."


class TestClosePointInTime:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_closes_point_in_time(self, es):
        pit_id = "0123456789abcdef"
        params = {
            "name": "close-pit-test",
            "with-point-in-time-from": "open-pit-task1",
        }
        es.close_point_in_time = mock.AsyncMock()
        r = runner.ClosePointInTime()
        async with runner.CompositeContext():
            runner.CompositeContext.put("open-pit-task1", pit_id)
            await r(es, params)

        es.close_point_in_time.assert_awaited_once_with(body={"id": "0123456789abcdef"}, params={}, headers=None)


class TestQueryWithSearchAfterScroll:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_search_after_with_pit(self, es):
        pit_op = "open-point-in-time1"
        pit_id = "0123456789abcdef"
        params = {
            "name": "search-with-pit",
            "index": "test-index",
            "operation-type": "paginated-search",
            "with-point-in-time-from": pit_op,
            "pages": "all",
            "results-per-page": 2,
            "body": {
                "sort": [
                    {"timestamp": "asc", "tie_breaker_id": "asc"},
                ],
                "query": {
                    "match_all": {},
                },
            },
        }

        page_1 = {
            "pit_id": "fedcba9876543210",
            "took": 10,
            "timed_out": False,
            "hits": {
                "total": {"value": 3, "relation": "eq"},
                "hits": [
                    {"_id": "1", "timestamp": 1609780186, "sort": [1609780186, "1"]},
                    {"_id": "2", "timestamp": 1609780186, "sort": [1609780186, "2"]},
                ],
            },
        }

        page_2 = {
            "pit_id": "fedcba9876543211",
            "took": 10,
            "timed_out": False,
            "hits": {
                "total": {"value": "3", "relation": "eq"},
                "hits": [
                    {"_id": "3", "timestamp": 1609780187, "sort": [1609780187, "3"]},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(
            side_effect=[
                io.BytesIO(json.dumps(page_1).encode()),
                io.BytesIO(json.dumps(page_2).encode()),
            ]
        )

        r = runner.Query()

        async with runner.CompositeContext():
            runner.CompositeContext.put(pit_op, pit_id)
            await r(es, params)
            # make sure pit_id is updated afterward
            assert runner.CompositeContext.get(pit_op) == "fedcba9876543211"

        es.perform_request.assert_has_awaits(
            [
                mock.call(
                    method="GET",
                    path="/_search",
                    params={},
                    body={
                        "query": {
                            "match_all": {},
                        },
                        "sort": [
                            {
                                "timestamp": "asc",
                                "tie_breaker_id": "asc",
                            }
                        ],
                        "size": 2,
                        "pit": {
                            "id": "0123456789abcdef",
                            "keep_alive": "1m",
                        },
                    },
                    headers=None,
                ),
                mock.call(
                    method="GET",
                    path="/_search",
                    params={},
                    body={
                        "query": {
                            "match_all": {},
                        },
                        "sort": [
                            {
                                "timestamp": "asc",
                                "tie_breaker_id": "asc",
                            }
                        ],
                        "size": 2,
                        "pit": {
                            "id": "fedcba9876543210",
                            "keep_alive": "1m",
                        },
                        "search_after": [1609780186, "2"],
                    },
                    headers=None,
                ),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_search_after_without_pit(self, es):
        params = {
            "name": "search-with-pit",
            "operation-type": "paginated-search",
            "index": "test-index-1",
            "pages": "all",
            "results-per-page": 2,
            "body": {
                "sort": [{"timestamp": "asc", "tie_breaker_id": "asc"}],
                "query": {
                    "match_all": {},
                },
            },
        }
        page_1 = {
            "took": 10,
            "timed_out": False,
            "hits": {
                "total": {
                    "value": 3,
                    "relation": "eq",
                },
                "hits": [
                    {"_id": "1", "timestamp": 1609780186, "sort": [1609780186, "1"]},
                    {"_id": "2", "timestamp": 1609780186, "sort": [1609780186, "2"]},
                ],
            },
        }

        page_2 = {
            "took": 10,
            "timed_out": False,
            "hits": {
                "total": {
                    "value": 3,
                    "relation": "eq",
                },
                "hits": [
                    {"_id": "3", "timestamp": 1609780187, "sort": [1609780187, "3"]},
                ],
            },
        }

        es.perform_request = mock.AsyncMock(
            side_effect=[
                io.BytesIO(json.dumps(page_1).encode()),
                io.BytesIO(json.dumps(page_2).encode()),
            ]
        )
        r = runner.Query()
        await r(es, params)

        es.perform_request.assert_has_awaits(
            [
                mock.call(
                    method="GET",
                    path="/test-index-1/_search",
                    params={},
                    body={
                        "query": {
                            "match_all": {},
                        },
                        "sort": [
                            {"timestamp": "asc", "tie_breaker_id": "asc"},
                        ],
                        "size": 2,
                    },
                    headers=None,
                ),
                mock.call(
                    method="GET",
                    path="/test-index-1/_search",
                    params={},
                    body={
                        "query": {
                            "match_all": {},
                        },
                        "sort": [
                            {"timestamp": "asc", "tie_breaker_id": "asc"},
                        ],
                        "size": 2,
                        "search_after": [1609780186, "2"],
                    },
                    headers=None,
                ),
            ]
        )


class TestSearchAfterExtractor:
    response_text = """
        {
            "pit_id": "fedcba9876543210",
            "took": 10,
            "timed_out": false,
            "hits": {
                "total": 2,
                "hits": [
                    {
                        "_id": "1",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "1"]
                    },
                    {
                        "_id": "2",
                         "timestamp": 1609780186,
                         "sort": [1609780186, "2"]
                    }
                ]
            }
        }"""
    response = io.BytesIO(response_text.encode())

    def test_extract_all_properties(self):
        target = runner.SearchAfterExtractor()
        props, last_sort = target(response=self.response, get_point_in_time=True, hits_total=None)
        expected_props = {"hits.total.relation": "eq", "hits.total.value": 2, "pit_id": "fedcba9876543210", "timed_out": False, "took": 10}
        expected_sort_value = [1609780186, "2"]
        assert props == expected_props
        assert last_sort == expected_sort_value

    def test_extract_ignore_point_in_time(self):
        target = runner.SearchAfterExtractor()
        props, last_sort = target(response=self.response, get_point_in_time=False, hits_total=None)
        expected_props = {"hits.total.relation": "eq", "hits.total.value": 2, "timed_out": False, "took": 10}
        expected_sort_value = [1609780186, "2"]
        assert props == expected_props
        assert last_sort == expected_sort_value

    def test_extract_uses_provided_hits_total(self):
        target = runner.SearchAfterExtractor()
        # we use an incorrect hits_total just to prove we didn't extract it from the response
        props, last_sort = target(response=self.response, get_point_in_time=False, hits_total=10)
        expected_props = {"hits.total.relation": "eq", "hits.total.value": 10, "timed_out": False, "took": 10}
        expected_sort_value = [1609780186, "2"]
        assert props == expected_props
        assert last_sort == expected_sort_value

    def test_extract_missing_required_point_in_time(self):
        response_copy = json.loads(self.response_text)
        del response_copy["pit_id"]
        response_copy_bytesio = io.BytesIO(json.dumps(response_copy).encode())
        target = runner.SearchAfterExtractor()
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            target(response=response_copy_bytesio, get_point_in_time=True, hits_total=None)
        assert exc.value.args[0] == "Paginated query failure: pit_id was expected but not found in the response."

    def test_extract_missing_ignored_point_in_time(self):
        response_copy = json.loads(self.response_text)
        del response_copy["pit_id"]
        response_copy_bytesio = io.BytesIO(json.dumps(response_copy).encode())
        target = runner.SearchAfterExtractor()
        props, last_sort = target(response=response_copy_bytesio, get_point_in_time=False, hits_total=None)
        expected_props = {"hits.total.relation": "eq", "hits.total.value": 2, "timed_out": False, "took": 10}
        expected_sort_value = [1609780186, "2"]
        assert props == expected_props
        assert last_sort == expected_sort_value


class TestCompositeContext:
    def test_cannot_be_used_outside_of_composite(self):
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            runner.CompositeContext.put("test", 1)

        assert exc.value.args[0] == "This operation is only allowed inside a composite operation."

    @pytest.mark.asyncio
    async def test_put_get_and_remove(self):
        async with runner.CompositeContext():
            runner.CompositeContext.put("test", 1)
            runner.CompositeContext.put("don't clear this key", 1)
            assert runner.CompositeContext.get("test") == 1
            runner.CompositeContext.remove("test")

        # context is cleared properly
        async with runner.CompositeContext():
            with pytest.raises(KeyError) as exc:
                runner.CompositeContext.get("don't clear this key")
            assert exc.value.args[0] == "Unknown property [don't clear this key]. Currently recognized properties are []."

    @pytest.mark.asyncio
    async def test_fails_to_read_unknown_key(self):
        async with runner.CompositeContext():
            with pytest.raises(KeyError) as exc:
                runner.CompositeContext.put("test", 1)
                runner.CompositeContext.get("unknown")
            assert exc.value.args[0] == "Unknown property [unknown]. Currently recognized properties are [test]."

    @pytest.mark.asyncio
    async def test_fails_to_remove_unknown_key(self):
        async with runner.CompositeContext():
            with pytest.raises(KeyError) as exc:
                runner.CompositeContext.put("test", 1)
                runner.CompositeContext.remove("unknown")
            assert exc.value.args[0] == "Unknown property [unknown]. Currently recognized properties are [test]."


class TestComposite:
    class CounterRunner:
        def __init__(self):
            self.max_value = 0
            self.current = 0

        async def __aenter__(self):
            self.current += 1
            return self

        async def __call__(self, es, params):
            self.max_value = max(self.max_value, self.current)
            # wait for a short moment to ensure overlap
            await asyncio.sleep(0.1)

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            self.current -= 1
            return False

    class CallRecorderRunner:
        def __init__(self):
            self.calls = []

        async def __call__(self, es, params):
            self.calls.append(params["name"])
            # wait for a short moment to ensure overlap
            await asyncio.sleep(0.1)

    def setup_method(self, method):
        runner.register_default_runners()
        self.counter_runner = self.CounterRunner()
        self.call_recorder_runner = self.CallRecorderRunner()
        runner.register_runner("counter", self.counter_runner, async_runner=True)
        runner.register_runner("call-recorder", self.call_recorder_runner, async_runner=True)
        runner.enable_assertions(True)

    def teardown_method(self, method):
        runner.enable_assertions(False)
        runner.remove_runner("counter")
        runner.remove_runner("call-recorder")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_execute_multiple_streams(self, es):
        es.perform_request = mock.AsyncMock(
            side_effect=[
                # raw-request
                None,
                # search
                io.StringIO(
                    json.dumps(
                        {
                            "hits": {
                                "total": {
                                    "value": 10,
                                    "relation": "eq",
                                },
                            },
                        },
                    ),
                ),
            ]
        )

        params = {
            "max-connections": 4,
            "requests": [
                {
                    "stream": [
                        {
                            "operation-type": "raw-request",
                            "path": "/",
                            "body": {},
                        },
                        {
                            "operation-type": "search",
                            "index": "test",
                            "detailed-results": True,
                            "assertions": [
                                {
                                    "property": "hits",
                                    "condition": ">",
                                    "value": 0,
                                }
                            ],
                            "body": {
                                "query": {
                                    "match_all": {},
                                }
                            },
                        },
                    ]
                },
                {
                    "stream": [
                        {
                            "operation-type": "sleep",
                            "duration": 0.1,
                        }
                    ]
                },
            ],
        }

        r = runner.Composite()
        await r(es, params)

        es.perform_request.assert_has_awaits(
            [
                mock.call(method="GET", path="/", headers=None, body={}, params={}),
                mock.call(method="GET", path="/test/_search", params={}, body={"query": {"match_all": {}}}, headers=None),
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_propagates_violated_assertions(self, es):
        es.perform_request = mock.AsyncMock(
            side_effect=[
                # search
                io.StringIO(
                    json.dumps(
                        {
                            "hits": {
                                "total": {
                                    "value": 0,
                                    "relation": "eq",
                                },
                            }
                        }
                    )
                )
            ]
        )

        params = {
            "max-connections": 4,
            "requests": [
                {
                    "stream": [
                        {
                            "operation-type": "search",
                            "index": "test",
                            "detailed-results": True,
                            "assertions": [
                                {
                                    "property": "hits",
                                    "condition": ">",
                                    "value": 0,
                                }
                            ],
                            "body": {
                                "query": {
                                    "match_all": {},
                                }
                            },
                        }
                    ]
                }
            ],
        }

        r = runner.Composite()
        with pytest.raises(exceptions.RallyTaskAssertionError, match=r"Expected \[hits\] to be > \[0\] but was \[0\]."):
            await r(es, params)

        es.perform_request.assert_has_awaits(
            [
                mock.call(
                    method="GET",
                    path="/test/_search",
                    params={},
                    body={
                        "query": {
                            "match_all": {},
                        }
                    },
                    headers=None,
                )
            ]
        )

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_executes_tasks_in_specified_order(self, es):
        es.perform_request = mock.AsyncMock()

        params = {
            "requests": [
                {
                    "name": "initial-call",
                    "operation-type": "call-recorder",
                },
                {
                    "stream": [
                        {
                            "name": "stream-a",
                            "operation-type": "call-recorder",
                        }
                    ]
                },
                {
                    "stream": [
                        {
                            "name": "stream-b",
                            "operation-type": "call-recorder",
                        }
                    ]
                },
                {
                    "name": "call-after-stream-ab",
                    "operation-type": "call-recorder",
                },
                {
                    "stream": [
                        {
                            "name": "stream-c",
                            "operation-type": "call-recorder",
                        }
                    ]
                },
                {
                    "stream": [
                        {
                            "name": "stream-d",
                            "operation-type": "call-recorder",
                        }
                    ]
                },
                {
                    "name": "call-after-stream-cd",
                    "operation-type": "call-recorder",
                },
            ]
        }

        r = runner.Composite()
        r.supported_op_types = ["call-recorder"]
        await r(es, params)

        assert self.call_recorder_runner.calls == [
            "initial-call",
            # stream-a and stream-b are concurrent
            "stream-a",
            "stream-b",
            "call-after-stream-ab",
            # stream-c and stream-d are concurrent
            "stream-c",
            "stream-d",
            "call-after-stream-cd",
        ]

    @pytest.mark.asyncio
    async def test_adds_request_timings(self):
        # We only need the request context holder functionality but not any calls to Elasticsearch.
        # Therefore we can use the the request context holder as a substitute and get proper timing info.
        es = client.RequestContextHolder()

        params = {
            "requests": [
                {
                    "name": "initial-call",
                    "operation-type": "sleep",
                    "duration": 0.1,
                },
                {
                    "stream": [
                        {
                            "name": "stream-a",
                            "operation-type": "sleep",
                            "duration": 0.2,
                        }
                    ]
                },
                {
                    "stream": [
                        {
                            "name": "stream-b",
                            "operation-type": "sleep",
                            "duration": 0.1,
                        }
                    ]
                },
            ]
        }

        r = runner.Composite()
        response = await r(es, params)

        assert response["weight"] == 1
        assert response["unit"] == "ops"
        timings = response["dependent_timing"]
        assert len(timings) == 3

        assert timings[0]["operation"] == "initial-call"
        assert timings[0]["service_time"] == pytest.approx(0.1, abs=0.1)

        assert timings[1]["operation"] == "stream-a"
        assert timings[1]["service_time"] == pytest.approx(0.2, abs=0.1)

        assert timings[2]["operation"] == "stream-b"
        assert timings[2]["service_time"] == pytest.approx(0.1, abs=0.1)

        # common properties
        for timing in timings:
            assert timing["operation-type"] == "sleep"
            assert "absolute_time" in timing
            assert "request_start" in timing
            assert "request_end" in timing
            assert timing["request_end"] > timing["request_start"]

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_limits_connections(self, es):
        params = {
            "max-connections": 2,
            "requests": [
                {
                    "stream": [
                        {
                            "operation-type": "counter",
                        }
                    ]
                },
                {
                    "stream": [
                        {
                            "operation-type": "counter",
                        }
                    ]
                },
                {
                    "stream": [
                        {
                            "operation-type": "counter",
                        }
                    ]
                },
            ],
        }

        r = runner.Composite()
        r.supported_op_types = ["counter"]
        await r(es, params)

        # composite runner should limit to two concurrent connections
        assert self.counter_runner.max_value == 2

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_rejects_invalid_stream(self, es):
        # params contains a "streams" property (plural) but it should be "stream" (singular)
        params = {
            "max-connections": 2,
            "requests": [{"stream": [{"operation-type": "counter"}]}, {"streams": [{"operation-type": "counter"}]}],
        }

        r = runner.Composite()
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            await r(es, params)

        assert exc.value.args[0] == "Requests structure must contain [stream] or [operation-type]."

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_rejects_unsupported_operations(self, es):
        params = {"requests": [{"stream": [{"operation-type": "bulk"}]}]}

        r = runner.Composite()
        with pytest.raises(exceptions.RallyAssertionError) as exc:
            await r(es, params)

        assert exc.value.args[0] == (
            "Unsupported operation-type [bulk]. Use one of [open-point-in-time, close-point-in-time, "
            "search, paginated-search, raw-request, sleep, submit-async-search, get-async-search, "
            "delete-async-search, field-caps]."
        )


class TestRequestTiming:
    class StaticRequestTiming:
        def __init__(self, task_start):
            self.task_start = task_start
            self.current_request_start = self.task_start

        async def __aenter__(self):
            # pretend time advances on each request
            self.current_request_start += 5
            return self

        @property
        def request_start(self):
            return self.current_request_start

        @property
        def request_end(self):
            return self.current_request_start + 0.1

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_merges_timing_info(self, es):
        multi_cluster_client = {"default": es}
        es.new_request_context.return_value = self.StaticRequestTiming(task_start=2)

        delegate = mock.AsyncMock(return_value={"weight": 5, "unit": "ops", "success": True})
        params = {"name": "unit-test-operation", "operation-type": "test-op"}
        timer = runner.RequestTiming(delegate)

        response = await timer(multi_cluster_client, params)

        assert math.isclose(response["dependent_timing"].pop("service_time"), 0.1)
        assert response["dependent_timing"].pop("absolute_time") is not None
        assert response == {
            "weight": 5,
            "unit": "ops",
            "success": True,
            "dependent_timing": {
                "operation": "unit-test-operation",
                "operation-type": "test-op",
                "request_start": 7,
                "request_end": 7.1,
            },
        }

        delegate.assert_called_once_with(multi_cluster_client, params)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_creates_new_timing_info(self, es):
        multi_cluster_client = {"default": es}
        es.new_request_context.return_value = self.StaticRequestTiming(task_start=2)

        # a simple runner without a return value
        delegate = mock.AsyncMock()
        params = {"name": "unit-test-operation", "operation-type": "test-op"}
        timer = runner.RequestTiming(delegate)

        response = await timer(multi_cluster_client, params)

        assert math.isclose(response["dependent_timing"].pop("service_time"), 0.1)
        assert response["dependent_timing"].pop("absolute_time") is not None
        assert response == {
            # defaults added by the timing runner
            "weight": 1,
            "unit": "ops",
            "success": True,
            "dependent_timing": {
                "operation": "unit-test-operation",
                "operation-type": "test-op",
                "request_start": 7,
                "request_end": 7.1,
            },
        }

        delegate.assert_called_once_with(multi_cluster_client, params)


class TestRetry:
    @pytest.mark.asyncio
    async def test_is_transparent_on_success_when_no_retries(self):
        delegate = mock.AsyncMock()
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_is_transparent_on_exception_when_no_retries(self):
        delegate = mock.AsyncMock(side_effect=elasticsearch.ConnectionError("N/A", "no route to host"))
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        with pytest.raises(elasticsearch.ConnectionError):
            await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_is_transparent_on_application_error_when_no_retries(self):
        original_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.AsyncMock(return_value=original_return_value)
        es = None
        params = {
            # no retries
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        assert result == original_return_value
        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_is_does_not_retry_on_success(self):
        delegate = mock.AsyncMock()
        es = None
        params = {"retries": 3, "retry-wait-period": 0.1, "retry-on-timeout": True, "retry-on-error": True}
        retrier = runner.Retry(delegate)

        await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_retries_on_timeout_if_wanted_and_raises_if_no_recovery(self):
        delegate = mock.AsyncMock(
            side_effect=[
                elasticsearch.ConnectionError(message="no route to host"),
                elasticsearch.ConnectionError(message="no route to host"),
                elasticsearch.ConnectionError(message="no route to host"),
                elasticsearch.ConnectionError(message="no route to host"),
            ]
        )
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": True, "retry-on-error": True}
        retrier = runner.Retry(delegate)

        with pytest.raises(elasticsearch.ConnectionError):
            await retrier(es, params)

        delegate.assert_has_calls(
            [
                mock.call(es, params),
                mock.call(es, params),
                mock.call(es, params),
            ]
        )

    @pytest.mark.asyncio
    async def test_retries_on_timeout_if_wanted_and_returns_first_call(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.AsyncMock(
            side_effect=[
                elasticsearch.ConnectionError(message="no route to host"),
                failed_return_value,
            ]
        )
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": True, "retry-on-error": False}
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)
        assert result == failed_return_value

        delegate.assert_has_calls(
            [
                # has returned a connection error
                mock.call(es, params),
                # has returned normally
                mock.call(es, params),
            ]
        )

    @pytest.mark.asyncio
    async def test_retries_mixed_timeout_and_application_errors(self):
        connection_error = elasticsearch.ConnectionError(message="no route to host")
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.AsyncMock(
            side_effect=[
                connection_error,
                failed_return_value,
                connection_error,
                connection_error,
                failed_return_value,
                success_return_value,
            ]
        )
        es = None
        params = {
            # we try exactly as often as there are errors to also test the semantics of "retry".
            "retries": 5,
            "retry-wait-period": 0.01,
            "retry-on-timeout": True,
            "retry-on-error": True,
        }
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)
        assert result == success_return_value

        delegate.assert_has_calls(
            [
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
                mock.call(es, params),
            ]
        )

    @pytest.mark.asyncio
    async def test_does_not_retry_on_timeout_if_not_wanted(self):
        delegate = mock.AsyncMock(side_effect=elasticsearch.ConnectionTimeout(408, "timed out"))
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": False, "retry-on-error": True}
        retrier = runner.Retry(delegate)

        with pytest.raises(elasticsearch.ConnectionTimeout):
            await retrier(es, params)

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_retries_on_application_error_if_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        delegate = mock.AsyncMock(side_effect=[failed_return_value, success_return_value])
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": False, "retry-on-error": True}
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        assert result == success_return_value

        delegate.assert_has_calls(
            [
                mock.call(es, params),
                # one retry
                mock.call(es, params),
            ]
        )

    @pytest.mark.asyncio
    async def test_does_not_retry_on_application_error_if_not_wanted(self):
        failed_return_value = {"weight": 1, "unit": "ops", "success": False}

        delegate = mock.AsyncMock(return_value=failed_return_value)
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": True, "retry-on-error": False}
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        assert result == failed_return_value

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_assumes_success_if_runner_returns_non_dict(self):
        delegate = mock.AsyncMock(return_value=(1, "ops"))
        es = None
        params = {"retries": 3, "retry-wait-period": 0.01, "retry-on-timeout": True, "retry-on-error": True}
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        assert result == (1, "ops")

        delegate.assert_called_once_with(es, params)

    @pytest.mark.asyncio
    async def test_retries_until_success(self):
        failure_count = 5

        failed_return_value = {"weight": 1, "unit": "ops", "success": False}
        success_return_value = {"weight": 1, "unit": "ops", "success": True}

        responses = []
        responses += failure_count * [failed_return_value]
        responses += [success_return_value]

        delegate = mock.AsyncMock(side_effect=responses)
        es = None
        params = {"retry-until-success": True, "retry-wait-period": 0.01}
        retrier = runner.Retry(delegate)

        result = await retrier(es, params)

        assert result == success_return_value

        delegate.assert_has_calls([mock.call(es, params) for _ in range(failure_count + 1)])


class TestRemovePrefix:
    def test_remove_matching_prefix(self):
        suffix = runner.remove_prefix("index-20201117", "index")

        assert suffix == "-20201117"

    def test_prefix_doesnt_exit(self):
        index_name = "index-20201117"
        suffix = runner.remove_prefix(index_name, "unrelatedprefix")

        assert index_name == suffix


class TestRefreshRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_refresh_with_defaults(self, es):
        es.indices.refresh = mock.AsyncMock()
        refresh = runner.Refresh()
        await refresh(es, params={"index": "_all"})

        es.indices.refresh.assert_awaited_once_with(index="_all")

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_refresh_request_timeout(self, es):
        es.indices.refresh = mock.AsyncMock()
        refresh = runner.Refresh()
        await refresh(es, params={"index": "_all", "request-timeout": 50000})

        es.indices.refresh.assert_awaited_once_with(index="_all", request_timeout=50000)


class TestFieldCapsRunner:
    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_field_caps_without_index_filter(self, es):
        es.field_caps = mock.AsyncMock()
        field_caps = runner.FieldCaps()
        result = await field_caps(es, params={"index": "log-*"})
        assert result == {"weight": 1, "unit": "ops", "success": True}

        es.field_caps.assert_awaited_once_with(index="log-*", fields="*", body={}, params=None)

    @mock.patch("elasticsearch.Elasticsearch")
    @pytest.mark.asyncio
    async def test_field_caps_with_index_filter(self, es):
        es.field_caps = mock.AsyncMock()
        field_caps = runner.FieldCaps()
        index_filter = {"range": {"@timestamp": {"gte": "2022"}}}
        result = await field_caps(es, params={"fields": "time-*", "index_filter": index_filter})
        assert result == {"weight": 1, "unit": "ops", "success": True}

        expected_body = {"index_filter": index_filter}
        es.field_caps.assert_awaited_once_with(index="_all", fields="time-*", body=expected_body, params=None)
