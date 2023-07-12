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
# pylint: disable=protected-access

import datetime
import json
import logging
import os
import random
import string
import tempfile
import uuid
from dataclasses import dataclass
from unittest import mock

import elasticsearch.exceptions
import elasticsearch.helpers
import pytest

from esrally import config, exceptions, metrics, paths, track
from esrally.metrics import GlobalStatsCalculator
from esrally.track import Challenge, Operation, Task, Track
from esrally.utils import opts


class MockClientFactory:
    def __init__(self, cfg):
        self._es = mock.create_autospec(metrics.EsClient)

    def create(self):
        return self._es


class DummyIndexTemplateProvider:
    def __init__(self, cfg):
        pass

    def metrics_template(self):
        return "metrics-test-template"

    def races_template(self):
        return "races-test-template"

    def results_template(self):
        return "results-test-template"


class StaticClock:
    NOW = 1453362707

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return StaticStopWatch()


class StaticStopWatch:
    def start(self):
        pass

    def stop(self):
        pass

    def split_time(self):
        return 0

    def total_time(self):
        return 0


class TestEsClient:
    class NodeMock:
        def __init__(self, host, port):
            self.host = host
            self.port = port

    class NodePoolMock:
        def __init__(self, hosts):
            self.nodes = []
            for h in hosts:
                self.nodes.append(TestEsClient.NodeMock(host=h["host"], port=h["port"]))

        def get(self):
            return self.nodes[0]

    class TransportMock:
        def __init__(self, hosts):
            self.node_pool = TestEsClient.NodePoolMock(hosts)

    @dataclass
    class ApiResponseMeta:
        status: int

    class ClientMock:
        def __init__(self, hosts):
            self.transport = TestEsClient.TransportMock(hosts)

    @pytest.mark.parametrize("password_configuration", [None, "config", "environment"])
    @mock.patch("esrally.client.EsClientFactory")
    def test_config_opts_parsing(self, client_esclientfactory, password_configuration, monkeypatch):
        cfg = config.Config()

        _datastore_host = ".".join([str(random.randint(1, 254)) for _ in range(4)])
        _datastore_port = random.randint(1024, 65535)
        _datastore_secure = random.choice(["True", "true"])
        _datastore_user = "".join([random.choice(string.ascii_letters) for _ in range(8)])
        _datastore_password = "".join([random.choice(string.ascii_letters + string.digits + "_-@#$/") for _ in range(12)])
        _datastore_verify_certs = random.choice([True, False])
        _datastore_probe_version = False

        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.user", _datastore_user)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.probe.cluster_version", _datastore_probe_version)

        if password_configuration == "config":
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.password", _datastore_password)
        elif password_configuration == "environment":
            monkeypatch.setenv("RALLY_REPORTING_DATASTORE_PASSWORD", _datastore_password)

        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.ssl.verification_mode", "none")

        try:
            metrics.EsClientFactory(cfg)
        except exceptions.ConfigError as e:
            if password_configuration is not None:
                raise

            assert (
                e.message
                == "No password configured through [reporting] configuration or RALLY_REPORTING_DATASTORE_PASSWORD environment variable."
            )
            return

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "basic_auth_user": _datastore_user,
            "basic_auth_password": _datastore_password,
            "verify_certs": _datastore_verify_certs,
        }

        client_esclientfactory.assert_called_with(
            hosts=[{"host": _datastore_host, "port": _datastore_port}],
            client_options=expected_client_options,
            distribution_version=None,
            distribution_flavor=None,
        )

    @mock.patch("random.random")
    @mock.patch("esrally.time.sleep")
    def test_retries_on_various_errors(self, mocked_sleep, mocked_random, caplog):
        class ConnectionError:
            def logging_statements(self, retries):
                logging_statements = []
                for i, v in enumerate(range(retries)):
                    logging_statements.append(
                        "Connection error [%s] in attempt [%d/%d]. Sleeping for [%f] seconds."
                        % (
                            "unit-test",
                            i + 1,
                            max_retry,
                            sleep_slots[v],
                        )
                    )
                logging_statements.append(
                    "Could not connect to your Elasticsearch metrics store. Please check that it is running on host [127.0.0.1] at "
                    f"port [9200] or fix the configuration in [{paths.rally_confdir()}/rally.ini]."
                )
                return logging_statements

            def raise_error(self):
                raise elasticsearch.exceptions.ConnectionError("unit-test")

        class ConnectionTimeout:
            def logging_statements(self, retries):
                logging_statements = []
                for i, v in enumerate(range(retries)):
                    logging_statements.append(
                        "Connection timeout [%s] in attempt [%d/%d]. Sleeping for [%f] seconds."
                        % (
                            "unit-test",
                            i + 1,
                            max_retry,
                            sleep_slots[v],
                        )
                    )
                logging_statements.append(f"Connection timeout while running [raise_error] (retried {retries} times).")
                return logging_statements

            def raise_error(self):
                raise elasticsearch.exceptions.ConnectionTimeout("unit-test")

        class ApiError:
            def __init__(self, status_code):
                self.status_code = status_code

            def logging_statements(self, retries):
                logging_statements = []
                for i, v in enumerate(range(retries)):
                    logging_statements.append(
                        "%s (code: %d) in attempt [%d/%d]. Sleeping for [%f] seconds."
                        % (
                            "unit-test",
                            self.status_code,
                            i + 1,
                            max_retry,
                            sleep_slots[v],
                        )
                    )
                logging_statements.append(
                    "An error [unit-test] occurred while running the operation [raise_error] against your Elasticsearch "
                    "metrics store on host [127.0.0.1] at port [9200]."
                )
                return logging_statements

            def raise_error(self):
                err = elasticsearch.exceptions.ApiError("unit-test", meta=TestEsClient.ApiResponseMeta(status=self.status_code), body={})
                raise err

        class BulkIndexError:
            def __init__(self, errors):
                self.errors = errors
                self.error_message = f"{len(self.errors)} document(s) failed to index"

            def logging_statements(self, retries):
                logging_statements = []
                for i, v in enumerate(range(retries)):
                    logging_statements.append(
                        "Error in sending metrics to remote metrics store [%s] in attempt [%d/%d]. Sleeping for [%f] seconds."
                        % (
                            self.error_message,
                            i + 1,
                            max_retry,
                            sleep_slots[v],
                        )
                    )
                logging_statements.append(
                    f"Failed to send metrics to remote metrics store: [{self.errors}] - Full error(s) [{self.errors}]"
                )
                return logging_statements

            def raise_error(self):
                raise elasticsearch.helpers.BulkIndexError(self.error_message, self.errors)

        bulk_index_errors = [
            {
                "index": {
                    "_index": "rally-metrics-2023-04",
                    "_id": "dffAc4cBOnIJ2Omtflwg",
                    "status": 429,
                    "error": {
                        "type": "circuit_breaking_exception",
                        "reason": "[parent] Data too large, data for [<http_request>] would be [123848638/118.1mb], "
                        "which is larger than the limit of [123273216/117.5mb], real usage: [120182112/114.6mb], "
                        "new bytes reserved: [3666526/3.4mb]",
                        "bytes_wanted": 123848638,
                        "bytes_limit": 123273216,
                        "durability": "TRANSIENT",
                    },
                }
            },
        ]

        retryable_errors = [
            ApiError(429),
            ApiError(502),
            ApiError(503),
            ApiError(504),
            ConnectionError(),
            ConnectionTimeout(),
            BulkIndexError(bulk_index_errors),
        ]

        max_retry = 10

        # The sec to sleep for 10 transport errors is
        # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] ~> 17.05min in total
        sleep_slots = [float(2**i) for i in range(0, max_retry)]

        # we want deterministic timings to assess logging statements
        mocked_random.return_value = 0

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9200"}]))

        exepcted_logger_calls = []
        expected_sleep_calls = []

        for e in retryable_errors:
            exepcted_logger_calls += e.logging_statements(max_retry)
            expected_sleep_calls += [mock.call(int(sleep_slots[i])) for i in range(0, max_retry)]

            with pytest.raises(exceptions.RallyError):
                with caplog.at_level(logging.DEBUG):
                    client.guarded(e.raise_error)

        actual_logger_calls = [r.message for r in caplog.records]
        actual_sleep_calls = mocked_sleep.call_args_list

        assert actual_sleep_calls == expected_sleep_calls
        assert actual_logger_calls == exepcted_logger_calls

    def test_raises_sytem_setup_error_on_authentication_problems(self):
        def raise_authentication_error():
            raise elasticsearch.exceptions.AuthenticationException(meta=None, body=None, message="unit-test")

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with pytest.raises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authentication_error)
        assert ctx.value.args[0] == (
            "The configured user could not authenticate against your Elasticsearch metrics store running on host [127.0.0.1] "
            f"at port [9243] (wrong password?). Please fix the configuration in [{paths.rally_confdir()}/rally.ini]."
        )

    def test_raises_sytem_setup_error_on_authorization_problems(self):
        def raise_authorization_error():
            raise elasticsearch.exceptions.AuthorizationException(meta=None, body=None, message="unit-test")

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with pytest.raises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authorization_error)
        assert ctx.value.args[0] == (
            "The configured user does not have enough privileges to run the operation [raise_authorization_error] against "
            "your Elasticsearch metrics store running on host [127.0.0.1] at port [9243]. Please adjust your x-pack "
            f"configuration or specify a user with enough privileges in the configuration in [{paths.rally_confdir()}/rally.ini]."
        )

    def test_raises_rally_error_on_unknown_problems(self):
        def raise_unknown_error():
            exc = elasticsearch.exceptions.TransportError(message="unit-test")
            raise exc

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with pytest.raises(exceptions.RallyError) as ctx:
            client.guarded(raise_unknown_error)
        assert ctx.value.args[0] == (
            "Transport error(s) [unit-test] occurred while running the operation [raise_unknown_error] against your Elasticsearch metrics "
            "store on host [127.0.0.1] at port [9243]."
        )

    def test_raises_rally_error_on_unretryable_bulk_indexing_errors(self):
        bulk_index_errors = [
            {
                "index": {
                    "_index": "rally-metrics-2023-04",
                    "_id": "dffAc4cBOnIJ2Omtflwg",
                    "status": 429,
                    "error": {
                        "type": "circuit_breaking_exception",
                        "reason": "[parent] Data too large, data for [<http_request>] would be [123848638/118.1mb], "
                        "which is larger than the limit of [123273216/117.5mb], real usage: [120182112/114.6mb], "
                        "new bytes reserved: [3666526/3.4mb]",
                        "bytes_wanted": 123848638,
                        "bytes_limit": 123273216,
                        "durability": "TRANSIENT",
                    },
                }
            },
            {
                "index": {
                    "_id": "1",
                    "_index": "rally-metrics-2023-04",
                    "error": {"type": "version_conflict_engine_exception"},
                    "status": 409,
                }
            },
            {
                "index": {
                    "_index": "rally-metrics-2023-04",
                    "_id": "dffAc4cBOnIJ2Omtflwg",
                    "status": 400,
                    "error": {
                        "type": "mapper_parsing_exception",
                        "reason": "failed to parse field [meta.error-description] of type [keyword] in document with id "
                        "'dffAc4cBOnIJ2Omtflwg'. Preview of field's value: 'HTTP status: 400, message: failed to parse "
                        "field [@timestamp] of type [date] in document with id '-PXAc4cBOnIJ2OmtX33J'. Preview of "
                        "field's value: '1998-04-30T15:02:56-05:00'",
                    },
                }
            },
        ]

        def raise_bulk_index_error():
            err = elasticsearch.helpers.BulkIndexError(f"{len(bulk_index_errors)} document(s) failed to index", bulk_index_errors)
            raise err

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with pytest.raises(
            exceptions.RallyError,
            match=(r"Unretryable error encountered when sending metrics to remote metrics store: \[version_conflict_engine_exception\]"),
        ):
            client.guarded(raise_bulk_index_error)


class TestEsMetrics:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {"shard-count": 3})
        self.metrics_store = metrics.EsMetricsStore(
            self.cfg, client_factory_class=MockClientFactory, index_template_provider_class=DummyIndexTemplateProvider, clock=StaticClock
        )
        # get hold of the mocked client...
        self.es_mock = self.metrics_store._client
        self.es_mock.exists.return_value = False

    def test_put_value_without_meta_info(self):
        throughput = 5000
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        self.metrics_store.put_value_cluster_level("indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {"shard-count": 3},
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {},
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc])

    def test_put_value_with_explicit_timestamps(self):
        throughput = 5000
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        self.metrics_store.put_value_cluster_level(
            name="indexing_throughput", value=throughput, unit="docs/s", absolute_time=0, relative_time=10
        )
        expected_doc = {
            "@timestamp": 0,
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "relative-time": 10000,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {"shard-count": 3},
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {},
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc])

    def test_put_value_with_meta_info(self):
        throughput = 5000
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tags", opts.to_dict("intention:testing,disk_type:hdd"))
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_value_node_level("node0", "indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "track": "test",
            "track-params": {"shard-count": 3},
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0",
            },
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc])

    def test_put_doc_no_meta_data(self):
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        self.metrics_store.put_doc(
            doc={
                "name": "custom_metric",
                "total": 1234567,
                "per-shard": [17, 18, 1289, 273, 222],
                "unit": "byte",
            }
        )
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "track": "test",
            "track-params": {"shard-count": 3},
            "challenge": "append",
            "car": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte",
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc])

    def test_put_doc_with_metadata(self):
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tags", opts.to_dict("intention:testing,disk_type:hdd"))
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_doc(
            doc={
                "name": "custom_metric",
                "total": 1234567,
                "per-shard": [17, 18, 1289, 273, 222],
                "unit": "byte",
            },
            level=metrics.MetaInfoScope.node,
            node_name="node0",
            meta_data={
                "node_type": "hot",
            },
        )
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "relative-time": 0,
            "environment": "unittest",
            "track": "test",
            "track-params": {"shard-count": 3},
            "challenge": "append",
            "car": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0",
                "node_type": "hot",
            },
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="rally-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc])

    def test_get_one(self):
        duration = StaticClock.NOW * 1000
        search_result = {
            "hits": {
                "total": 2,
                "hits": [
                    {"_source": {"relative-time": duration, "value": 500}},
                    {"_source": {"relative-time": duration - 200, "value": 700}},
                ],
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "service_time"}},
                        {"term": {"task": "task1"}},
                    ]
                }
            },
            "size": 1,
            "sort": [{"relative-time": {"order": "desc"}}],
        }

        actual_duration = self.metrics_store.get_one(
            "service_time", task="task1", mapper=lambda doc: doc["relative-time"], sort_key="relative-time", sort_reverse=True
        )

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_duration == duration

    def test_get_one_no_hits(self):
        duration = None
        search_result = {"hits": {"total": 0, "hits": []}}
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "latency"}},
                        {"term": {"task": "task2"}},
                    ]
                }
            },
            "size": 1,
            "sort": [{"value": {"order": "asc"}}],
        }

        actual_duration = self.metrics_store.get_one(
            "latency", task="task2", mapper=lambda doc: doc["value"], sort_key="value", sort_reverse=False
        )

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_duration == duration

    def test_get_value(self):
        throughput = 5000
        search_result = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": StaticClock.NOW * 1000,
                            "value": throughput,
                        },
                    },
                ],
            },
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "race-id": self.RACE_ID,
                            },
                        },
                        {
                            "term": {
                                "name": "indexing_throughput",
                            },
                        },
                    ],
                },
            },
            "size": 1,
        }

        actual_throughput = self.metrics_store.get_one("indexing_throughput")

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_throughput == throughput

    def test_get_per_node_value(self):
        index_size = 5000
        search_result = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": StaticClock.NOW * 1000,
                            "value": index_size,
                        },
                    },
                ],
            },
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "final_index_size_bytes"}},
                        {"term": {"meta.node_name": "rally-node-3"}},
                    ]
                }
            },
            "size": 1,
        }

        actual_index_size = self.metrics_store.get_one("final_index_size_bytes", node_name="rally-node-3")

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_index_size == index_size

    def test_get_mean(self):
        mean_throughput = 1734
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "metric_stats": {
                    "count": 17,
                    "min": 1208,
                    "max": 1839,
                    "avg": mean_throughput,
                    "sum": 28934,
                },
            },
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "indexing_throughput"}},
                        {"term": {"operation-type": "bulk"}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "metric_stats": {
                    "stats": {
                        "field": "value",
                    },
                },
            },
        }

        actual_mean_throughput = self.metrics_store.get_mean("indexing_throughput", operation_type="bulk")

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_mean_throughput == mean_throughput

    def test_get_median(self):
        median_throughput = 30535
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "percentile_stats": {
                    "values": {
                        "50.0": median_throughput,
                    },
                },
            },
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "indexing_throughput"}},
                        {"term": {"operation-type": "bulk"}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "percentile_stats": {
                    "percentiles": {
                        "field": "value",
                        "percents": ["50.0"],
                    },
                },
            },
        }

        actual_median_throughput = self.metrics_store.get_median("indexing_throughput", operation_type="bulk")

        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)

        assert actual_median_throughput == median_throughput

    def test_get_error_rate_implicit_zero(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 1,
                        "key_as_string": "true",
                        "doc_count": 0,
                    },
                ],
            )
            == 0.0
        )

    def test_get_error_rate_explicit_zero(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 0,
                        "key_as_string": "false",
                        "doc_count": 0,
                    },
                    {
                        "key": 1,
                        "key_as_string": "true",
                        "doc_count": 500,
                    },
                ]
            )
            == 0.0
        )

    def test_get_error_rate_implicit_one(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 0,
                        "key_as_string": "false",
                        "doc_count": 123,
                    },
                ],
            )
            == 1.0
        )

    def test_get_error_rate_explicit_one(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 0,
                        "key_as_string": "false",
                        "doc_count": 123,
                    },
                    {
                        "key": 1,
                        "key_as_string": "true",
                        "doc_count": 0,
                    },
                ]
            )
            == 1.0
        )

    def test_get_error_rate_mixed(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 0,
                        "key_as_string": "false",
                        "doc_count": 500,
                    },
                    {
                        "key": 1,
                        "key_as_string": "true",
                        "doc_count": 500,
                    },
                ]
            )
            == 0.5
        )

    def test_get_error_rate_additional_unknown_key(self):
        assert (
            self._get_error_rate(
                buckets=[
                    {
                        "key": 0,
                        "key_as_string": "false",
                        "doc_count": 500,
                    },
                    {
                        "key": 1,
                        "key_as_string": "true",
                        "doc_count": 1500,
                    },
                    {
                        "key": 2,
                        "key_as_string": "undefined_for_test",
                        "doc_count": 13700,
                    },
                ]
            )
            == 0.25
        )

    def _get_error_rate(self, buckets):
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {"error_rate": {"buckets": buckets}},
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                        {"term": {"name": "service_time"}},
                        {"term": {"task": "scroll_query"}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "error_rate": {
                    "terms": {
                        "field": "meta.success",
                    },
                },
            },
        }

        actual_error_rate = self.metrics_store.get_error_rate("scroll_query")
        self.es_mock.search.assert_called_with(index="rally-metrics-2016-01", body=expected_query)
        return actual_error_rate


class TestEsRaceStore:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "time.start", self.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", self.RACE_ID)
        self.race_store = metrics.EsRaceStore(
            self.cfg,
            client_factory_class=MockClientFactory,
            index_template_provider_class=DummyIndexTemplateProvider,
        )
        # get hold of the mocked client...
        self.es_mock = self.race_store.client

    def test_find_existing_race_by_race_id(self):
        self.es_mock.search.return_value = {
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {
                        "_source": {
                            "rally-version": "0.4.4",
                            "environment": "unittest",
                            "race-id": self.RACE_ID,
                            "race-timestamp": "20160131T000000Z",
                            "pipeline": "from-sources",
                            "track": "unittest",
                            "challenge": "index",
                            "track-revision": "abc1",
                            "car": "defaults",
                            "results": {
                                "young_gc_time": 100,
                                "old_gc_time": 5,
                            },
                        }
                    }
                ],
            }
        }

        race = self.race_store.find_by_race_id(race_id=self.RACE_ID)
        assert race.race_id == self.RACE_ID

    def test_does_not_find_missing_race_by_race_id(self):
        self.es_mock.search.return_value = {
            "hits": {
                "total": {
                    "value": 0,
                    "relation": "eq",
                },
                "hits": [],
            },
        }

        with pytest.raises(exceptions.NotFound, match=r"No race with race id \[.*\]"):
            self.race_store.find_by_race_id(race_id="some invalid race id")

    def test_store_race(self):
        schedule = [track.Task("index #1", track.Operation("index", track.OperationType.Bulk))]

        t = track.Track(
            name="unittest",
            indices=[track.Index(name="tests", types=["_doc"])],
            challenges=[track.Challenge(name="index", default=True, schedule=schedule)],
        )

        race = metrics.Race(
            rally_version="0.4.4",
            rally_revision="123abc",
            environment_name="unittest",
            race_id=self.RACE_ID,
            race_timestamp=self.RACE_TIMESTAMP,
            pipeline="from-sources",
            user_tags={"os": "Linux"},
            track=t,
            track_params={"shard-count": 3},
            challenge=t.default_challenge,
            car="defaults",
            car_params={"heap_size": "512mb"},
            plugin_params=None,
            track_revision="abc1",
            team_revision="abc12333",
            distribution_version="5.0.0",
            distribution_flavor="default",
            revision="aaaeeef",
            results=self.DictHolder(
                {
                    "young_gc_time": 100,
                    "old_gc_time": 5,
                    "op_metrics": [
                        {
                            "task": "index #1",
                            "operation": "index",
                            "throughput": {"min": 1000, "median": 1250, "max": 1500, "unit": "docs/s"},
                        }
                    ],
                }
            ),
        )

        self.race_store.store_race(race)

        expected_doc = {
            "rally-version": "0.4.4",
            "rally-revision": "123abc",
            "environment": "unittest",
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "pipeline": "from-sources",
            "user-tags": {"os": "Linux"},
            "track": "unittest",
            "track-params": {"shard-count": 3},
            "challenge": "index",
            "track-revision": "abc1",
            "car": "defaults",
            "car-params": {"heap_size": "512mb"},
            "cluster": {
                "revision": "aaaeeef",
                "distribution-version": "5.0.0",
                "distribution-flavor": "default",
                "team-revision": "abc12333",
            },
            "results": {
                "young_gc_time": 100,
                "old_gc_time": 5,
                "op_metrics": [
                    {
                        "task": "index #1",
                        "operation": "index",
                        "throughput": {
                            "min": 1000,
                            "median": 1250,
                            "max": 1500,
                            "unit": "docs/s",
                        },
                    }
                ],
            },
        }
        self.es_mock.index.assert_called_with(index="rally-races-2016-01", id=self.RACE_ID, item=expected_doc)

    @mock.patch("esrally.utils.console.println")
    def test_delete_race(self, console):
        self.es_mock.delete_by_query.return_value = {"deleted": 0}
        self.cfg.add(config.Scope.application, "system", "delete.id", "0101")
        self.race_store.delete_race()
        expected_query = {"query": {"bool": {"filter": [{"term": {"environment": "unittest-env"}}, {"term": {"race-id": "0101"}}]}}}
        self.es_mock.delete_by_query.assert_called_with(index="rally-results-*", body=expected_query)
        console.assert_called_with("Did not find [0101] in environment [unittest-env].")

    @mock.patch("esrally.utils.console.println")
    def test_delete_annotation(self, console):
        self.es_mock.delete.return_value = {"result": "deleted"}
        self.cfg.add(config.Scope.application, "system", "delete.id", "0101")
        self.race_store.delete_annotation()
        self.es_mock.delete.assert_called_with(index="rally-annotations", id="0101")
        console.assert_called_with("Successfully deleted [0101].")

    @mock.patch("esrally.utils.console.println")
    @mock.patch("uuid.uuid4")
    def test_add_annotation(self, id_uuid, console):
        self.es_mock.delete_by_query.return_value = {"deleted": 0}
        id_uuid.return_value = 7
        self.cfg.add(config.Scope.application, "system", "admin.track", "unittest-track")
        self.cfg.add(config.Scope.application, "system", "add.chart_type", "unittest-chart_type")
        self.cfg.add(config.Scope.application, "system", "add.chart_name", "unittest-chart_name")
        self.cfg.add(config.Scope.application, "system", "add.message", "Test Annotation")
        self.cfg.add(config.Scope.application, "system", "add.race_timestamp", "20221217T200000Z")

        item = {
            "environment": "unittest-env",
            "race-timestamp": "20221217T000000Z",
            "track": "unittest-track",
            "chart": "unittest-chart_type",
            "chart-name": "unittest-chart_name",
            "message": "Test Annotation",
        }
        self.race_store.add_annotation()
        self.es_mock.index(index="rally-annotations", id=7, item=item)
        console.assert_called_with("Successfully added annotation [7].")

    @mock.patch("esrally.utils.console.println")
    def test_list_annotations(self, console):
        self.es_mock.search.return_value = {"hits": {"total": 0}}
        self.cfg.add(config.Scope.application, "system", "admin.track", "unittest-track")
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160131")
        self.cfg.add(config.Scope.application, "system", "list.from_date", "20160230")
        self.race_store.list_annotations()
        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"environment": "unittest-env"}},
                        {"range": {"race-timestamp": {"gte": "20160230", "lte": "20160131", "format": "basic_date"}}},
                        {"term": {"track": "unittest-track"}},
                    ]
                }
            },
            "sort": [{"race-timestamp": "desc"}, {"track": "asc"}, {"chart": "asc"}],
            "size": 100,
        }
        self.es_mock.search.assert_called_with(index="rally-annotations", body=expected_query)
        console.assert_called_with("No annotations found in environment [unittest-env].")

    def test_filter_race(self):
        self.es_mock.search.return_value = {"hits": {"total": 0}}
        self.cfg.add(config.Scope.application, "system", "admin.track", "unittest")
        self.cfg.add(config.Scope.application, "system", "list.races.benchmark_name", "unittest-test")
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160131")
        self.cfg.add(config.Scope.application, "system", "list.from_date", "20160230")
        self.race_store.list()
        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"environment": "unittest-env"}},
                        {"range": {"race-timestamp": {"gte": "20160230", "lte": "20160131", "format": "basic_date"}}},
                        {"term": {"track": "unittest"}},
                        {
                            "bool": {
                                "should": [
                                    {"term": {"user-tags.benchmark-name": "unittest-test"}},
                                    {"term": {"user-tags.name": "unittest-test"}},
                                ]
                            }
                        },
                    ]
                }
            },
            "size": 100,
            "sort": [{"race-timestamp": {"order": "desc"}}],
        }
        self.es_mock.search.assert_called_with(index="rally-races-*", body=expected_query)


class TestEsResultsStore:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "system", "time.start", self.RACE_TIMESTAMP)
        self.results_store = metrics.EsResultsStore(
            self.cfg,
            client_factory_class=MockClientFactory,
            index_template_provider_class=DummyIndexTemplateProvider,
        )
        # get hold of the mocked client...
        self.es_mock = self.results_store.client

    def test_store_results(self):
        schedule = [track.Task("index #1", track.Operation("index", track.OperationType.Bulk))]

        t = track.Track(
            name="unittest-track",
            indices=[track.Index(name="tests", types=["_doc"])],
            challenges=[track.Challenge(name="index", default=True, meta_data={"saturation": "70% saturated"}, schedule=schedule)],
            meta_data={"track-type": "saturation-degree", "saturation": "oversaturation"},
        )

        race = metrics.Race(
            rally_version="0.4.4",
            rally_revision="123abc",
            environment_name="unittest",
            race_id=self.RACE_ID,
            race_timestamp=self.RACE_TIMESTAMP,
            pipeline="from-sources",
            user_tags={"os": "Linux"},
            track=t,
            track_params=None,
            challenge=t.default_challenge,
            car="4gheap",
            car_params=None,
            plugin_params={"some-param": True},
            track_revision="abc1",
            team_revision="123ab",
            distribution_version="5.0.0",
            distribution_flavor="oss",
            results=metrics.GlobalStats(
                {
                    "young_gc_time": 100,
                    "old_gc_time": 5,
                    "op_metrics": [
                        {
                            "task": "index #1",
                            "operation": "index",
                            # custom op-metric which will override the defaults provided by the race
                            "meta": {
                                "track-type": "saturation-degree",
                                "saturation": "70% saturated",
                                "op-type": "bulk",
                            },
                            "throughput": {
                                "min": 1000,
                                "median": 1250,
                                "max": 1500,
                                "unit": "docs/s",
                            },
                        }
                    ],
                }
            ),
        )

        self.results_store.store_results(race)

        expected_docs = [
            {
                "rally-version": "0.4.4",
                "rally-revision": "123abc",
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {"some-param": True},
                "active": True,
                "name": "old_gc_time",
                "value": {"single": 5},
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                },
            },
            {
                "rally-version": "0.4.4",
                "rally-revision": "123abc",
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {"some-param": True},
                "active": True,
                "name": "throughput",
                "task": "index #1",
                "operation": "index",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s",
                },
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                    "op-type": "bulk",
                },
            },
            {
                "rally-version": "0.4.4",
                "rally-revision": "123abc",
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "plugin-params": {"some-param": True},
                "active": True,
                "name": "young_gc_time",
                "value": {"single": 100},
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                },
            },
        ]
        self.es_mock.bulk_index.assert_called_with(index="rally-results-2016-01", items=expected_docs)

    def test_store_results_with_missing_version(self):
        schedule = [track.Task("index #1", track.Operation("index", track.OperationType.Bulk))]

        t = track.Track(
            name="unittest-track",
            indices=[track.Index(name="tests", types=["_doc"])],
            challenges=[track.Challenge(name="index", default=True, meta_data={"saturation": "70% saturated"}, schedule=schedule)],
            meta_data={"track-type": "saturation-degree", "saturation": "oversaturation"},
        )

        race = metrics.Race(
            rally_version="0.4.4",
            rally_revision=None,
            environment_name="unittest",
            race_id=self.RACE_ID,
            race_timestamp=self.RACE_TIMESTAMP,
            pipeline="from-sources",
            user_tags={"os": "Linux"},
            track=t,
            track_params=None,
            challenge=t.default_challenge,
            car="4gheap",
            car_params=None,
            plugin_params=None,
            track_revision="abc1",
            team_revision="123ab",
            distribution_version=None,
            distribution_flavor=None,
            results=metrics.GlobalStats(
                {
                    "young_gc_time": 100,
                    "old_gc_time": 5,
                    "op_metrics": [
                        {
                            "task": "index #1",
                            "operation": "index",
                            # custom op-metric which will override the defaults provided by the race
                            "meta": {
                                "track-type": "saturation-degree",
                                "saturation": "70% saturated",
                                "op-type": "bulk",
                            },
                            "throughput": {
                                "min": 1000,
                                "median": 1250,
                                "max": 1500,
                                "unit": "docs/s",
                            },
                        }
                    ],
                }
            ),
        )

        self.results_store.store_results(race)

        expected_docs = [
            {
                "rally-version": "0.4.4",
                "rally-revision": None,
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "active": True,
                "name": "old_gc_time",
                "value": {"single": 5},
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                },
            },
            {
                "rally-version": "0.4.4",
                "rally-revision": None,
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "active": True,
                "name": "throughput",
                "task": "index #1",
                "operation": "index",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s",
                },
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                    "op-type": "bulk",
                },
            },
            {
                "rally-version": "0.4.4",
                "rally-revision": None,
                "environment": "unittest",
                "race-id": self.RACE_ID,
                "race-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {"os": "Linux"},
                "track": "unittest-track",
                "team-revision": "123ab",
                "track-revision": "abc1",
                "challenge": "index",
                "car": "4gheap",
                "active": True,
                "name": "young_gc_time",
                "value": {"single": 100},
                "meta": {
                    "track-type": "saturation-degree",
                    "saturation": "70% saturated",
                },
            },
        ]
        self.es_mock.bulk_index.assert_called_with(index="rally-results-2016-01", items=expected_docs)


class TestInMemoryMetricsStore:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {})
        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)

    def teardown_method(self, method):
        del self.metrics_store
        del self.cfg

    def test_get_one(self):
        duration = StaticClock.NOW
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level("service_time", 500, "ms", relative_time=duration - 400, task="task1")
        self.metrics_store.put_value_cluster_level("service_time", 600, "ms", relative_time=duration, task="task1")
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", relative_time=duration - 300)

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        actual_duration = self.metrics_store.get_one(
            "service_time", task="task1", mapper=lambda doc: doc["relative-time"], sort_key="relative-time", sort_reverse=True
        )

        assert duration * 1000 == actual_duration

    def test_get_one_no_hits(self):
        duration = StaticClock.NOW
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", relative_time=duration - 300)

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        actual_duration = self.metrics_store.get_one(
            "service_time", task="task1", mapper=lambda doc: doc["relative-time"], sort_key="relative-time", sort_reverse=True
        )

        assert actual_duration is None

    def test_get_value(self):
        throughput = 5000
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level("indexing_throughput", 1, "docs/s", sample_type=metrics.SampleType.Warmup)
        self.metrics_store.put_value_cluster_level("indexing_throughput", throughput, "docs/s")
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB")

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Warmup) == 1
        assert self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Normal) == throughput

    def test_get_percentile(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        self.assert_equal_percentiles("query_latency", [100.0], {100.0: 1000.0})
        self.assert_equal_percentiles("query_latency", [99.0], {99.0: 990.0})
        self.assert_equal_percentiles("query_latency", [99.9], {99.9: 999.0})
        self.assert_equal_percentiles("query_latency", [0.0], {0.0: 1.0})

        self.assert_equal_percentiles("query_latency", [99, 99.9, 100], {99: 990.0, 99.9: 999.0, 100: 1000.0})

    def test_get_mean(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        for i in range(1, 100):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert round(abs(50 - self.metrics_store.get_mean("query_latency")), 7) == 0

    def test_get_median(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert round(abs(500.5 - self.metrics_store.get_median("query_latency")), 7) == 0

    def assert_equal_percentiles(self, name, percentiles, expected_percentiles):
        actual_percentiles = self.metrics_store.get_percentiles(name, percentiles=percentiles)
        assert len(expected_percentiles) == len(actual_percentiles)
        for percentile, actual_percentile_value in actual_percentiles.items():
            assert round(abs(expected_percentiles[percentile] - actual_percentile_value), 1) == 0

    def test_externalize_and_bulk_add(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB")

        assert len(self.metrics_store.docs) == 1
        memento = self.metrics_store.to_externalizable()

        self.metrics_store.close()
        del self.metrics_store

        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)
        assert len(self.metrics_store.docs) == 0

        self.metrics_store.bulk_add(memento)
        assert len(self.metrics_store.docs) == 1
        assert self.metrics_store.get_one("final_index_size") == 1000

    def test_meta_data_per_document(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "cluster-name", "test")

        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", meta_data={"fs-block-size-bytes": 512})
        self.metrics_store.put_value_cluster_level("final_bytes_written", 1, "TB", meta_data={"io-batch-size-kb": 4})

        assert len(self.metrics_store.docs) == 2
        assert self.metrics_store.docs[0]["meta"] == {"cluster-name": "test", "fs-block-size-bytes": 512}

        assert self.metrics_store.docs[1]["meta"] == {"cluster-name": "test", "io-batch-size-kb": 4}

    def test_get_error_rate_zero_without_samples(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal) == 0.0

    def test_get_error_rate_by_sample_type(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Warmup, meta_data={"success": False}
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": True}
        )

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup) == 1.0
        assert self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal) == 0.0

    def test_get_error_rate_mixed(self):
        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-no-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": True}
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": True}
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": False}
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": True}
        )
        self.metrics_store.put_value_cluster_level(
            "service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal, meta_data={"success": True}
        )

        self.metrics_store.close()

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append-no-conflicts", "defaults")

        assert self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup) == 0.0
        assert self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal) == 0.2


class TestFileRaceStore:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setup_method(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", self.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", self.RACE_ID)
        self.race_store = metrics.FileRaceStore(self.cfg)

    def test_race_not_found(self):
        with pytest.raises(exceptions.NotFound, match=r"No race with race id \[.*\]"):
            # did not store anything yet
            self.race_store.find_by_race_id(self.RACE_ID)

    def test_store_race(self):
        schedule = [track.Task("index #1", track.Operation("index", track.OperationType.Bulk))]

        t = track.Track(
            name="unittest",
            indices=[track.Index(name="tests", types=["_doc"])],
            challenges=[track.Challenge(name="index", default=True, schedule=schedule)],
        )

        race = metrics.Race(
            rally_version="0.4.4",
            rally_revision="123abc",
            environment_name="unittest",
            race_id=self.RACE_ID,
            race_timestamp=self.RACE_TIMESTAMP,
            pipeline="from-sources",
            user_tags={"os": "Linux"},
            track=t,
            track_params={"clients": 12},
            challenge=t.default_challenge,
            car="4gheap",
            car_params=None,
            plugin_params=None,
            track_revision="abc1",
            team_revision="abc12333",
            distribution_version="5.0.0",
            distribution_flavor="default",
            revision="aaaeeef",
            results=self.DictHolder(
                {
                    "young_gc_time": 100,
                    "old_gc_time": 5,
                    "op_metrics": [
                        {
                            "task": "index #1",
                            "operation": "index",
                            "throughput": {
                                "min": 1000,
                                "median": 1250,
                                "max": 1500,
                                "unit": "docs/s",
                            },
                        }
                    ],
                }
            ),
        )

        self.race_store.store_race(race)

        retrieved_race = self.race_store.find_by_race_id(race_id=self.RACE_ID)
        assert race.race_id == retrieved_race.race_id
        assert race.race_timestamp == retrieved_race.race_timestamp
        assert len(self.race_store.list()) == 1

    def test_filter_race(self):
        t = track.Track(
            name="unittest",
            indices=[track.Index(name="tests", types=["_doc"])],
            challenges=[track.Challenge(name="index", default=True)],
        )

        race = metrics.Race(
            rally_version="0.4.4",
            rally_revision="123abc",
            environment_name="unittest",
            race_id=self.RACE_ID,
            race_timestamp=self.RACE_TIMESTAMP,
            pipeline="from-sources",
            user_tags={"name": "unittest-test"},
            track=t,
            track_params={"clients": 12},
            challenge=t.default_challenge,
            car="4gheap",
            car_params=None,
            plugin_params=None,
        )

        self.race_store.store_race(race)
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "admin.track", "unittest-2")
        assert len(self.race_store.list()) == 0
        self.cfg.add(config.Scope.application, "system", "admin.track", "unittest")
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.races.benchmark_name", "unittest-test-2")
        assert len(self.race_store.list()) == 0
        self.cfg.add(config.Scope.application, "system", "list.races.benchmark_name", "unittest-test")
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160129")
        assert len(self.race_store.list()) == 0
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160131")
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.from_date", "20160131")
        assert len(self.race_store.list()) == 1

    def test_delete_race(self):
        self.cfg.add(config.Scope.application, "system", "delete.id", "0101")

        with pytest.raises(NotImplementedError) as ctx:
            self.race_store.delete_race()
        assert ctx.value.args[0] == "Not supported for in-memory datastore."


class TestStatsCalculator:
    def test_calculate_global_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "system", "race.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "mechanic", "car.names", ["unittest_car"])
        cfg.add(config.Scope.application, "mechanic", "car.params", {})
        cfg.add(config.Scope.application, "mechanic", "plugin.params", {})
        cfg.add(config.Scope.application, "race", "user.tags", {})
        cfg.add(config.Scope.application, "race", "pipeline", "from-sources")
        cfg.add(config.Scope.application, "track", "params", {})

        index1 = track.Task(name="index #1", operation=track.Operation(name="index", operation_type=track.OperationType.Bulk, params=None))
        index2 = track.Task(name="index #2", operation=track.Operation(name="index", operation_type=track.OperationType.Bulk, params=None))
        challenge = track.Challenge(name="unittest", schedule=[index1, index2], default=True)
        t = track.Track("unittest", "unittest-track", challenges=[challenge])

        store = metrics.metrics_store(cfg, read_only=False, track=t, challenge=challenge)

        store.put_value_cluster_level("throughput", 500, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 2000, unit="docs/s", task="index #1", operation_type=track.OperationType.Bulk)

        store.put_value_cluster_level(
            "latency", 2800, unit="ms", task="index #1", operation_type=track.OperationType.Bulk, sample_type=metrics.SampleType.Warmup
        )
        store.put_value_cluster_level("latency", 200, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("latency", 220, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)
        store.put_value_cluster_level("latency", 225, unit="ms", task="index #1", operation_type=track.OperationType.Bulk)

        for collector in ("young_gen", "old_gen", "zgc_cycles", "zgc_pauses"):
            store.put_value_node_level("rally-node-0", f"node_{collector}_gc_time", 100, "ms")
            store.put_value_node_level("rally-node-0", f"node_{collector}_gc_count", 1)
            store.put_value_cluster_level(f"node_total_{collector}_gc_time", 100, "ms")
            store.put_value_cluster_level(f"node_total_{collector}_gc_count", 1)

        store.put_value_cluster_level(
            "service_time",
            250,
            unit="ms",
            task="index #1",
            operation_type=track.OperationType.Bulk,
            sample_type=metrics.SampleType.Warmup,
            meta_data={"success": False},
            relative_time=536,
        )
        store.put_value_cluster_level(
            "service_time",
            190,
            unit="ms",
            task="index #1",
            operation_type=track.OperationType.Bulk,
            meta_data={"success": True},
            relative_time=595,
        )
        store.put_value_cluster_level(
            "service_time",
            200,
            unit="ms",
            task="index #1",
            operation_type=track.OperationType.Bulk,
            meta_data={"success": False},
            relative_time=709,
        )
        store.put_value_cluster_level(
            "service_time",
            210,
            unit="ms",
            task="index #1",
            operation_type=track.OperationType.Bulk,
            meta_data={"success": True},
            relative_time=653,
        )

        # only warmup samples
        store.put_value_cluster_level(
            "throughput",
            500,
            unit="docs/s",
            task="index #2",
            sample_type=metrics.SampleType.Warmup,
            operation_type=track.OperationType.Bulk,
        )
        store.put_value_cluster_level(
            "latency", 2800, unit="ms", task="index #2", operation_type=track.OperationType.Bulk, sample_type=metrics.SampleType.Warmup
        )
        store.put_value_cluster_level(
            "service_time",
            250,
            unit="ms",
            task="index #2",
            operation_type=track.OperationType.Bulk,
            sample_type=metrics.SampleType.Warmup,
            relative_time=600,
        )

        store.put_doc(
            doc={
                "name": "ml_processing_time",
                "job": "benchmark_ml_job_1",
                "min": 2.2,
                "mean": 12.3,
                "median": 17.2,
                "max": 36.0,
                "unit": "ms",
            },
            level=metrics.MetaInfoScope.cluster,
        )

        stats = metrics.calculate_results(store, metrics.create_race(cfg, t, challenge))

        del store

        opm = stats.metrics("index #1")
        assert opm["throughput"] == {
            "min": 500,
            "mean": 1125,
            "median": 1000,
            "max": 2000,
            "unit": "docs/s",
        }
        assert opm["latency"] == {
            "50_0": 220,
            "100_0": 225,
            "mean": 215,
            "unit": "ms",
        }
        assert opm["service_time"] == {
            "50_0": 200,
            "100_0": 210,
            "mean": 200,
            "unit": "ms",
        }
        assert round(abs(0.3333333333333333 - opm["error_rate"]), 7) == 0
        assert opm["duration"] == 709 * 1000

        opm2 = stats.metrics("index #2")
        assert opm2["throughput"] == {
            "min": None,
            "mean": None,
            "median": None,
            "max": None,
            "unit": "docs/s",
        }

        assert stats.ml_processing_time == [
            {
                "job": "benchmark_ml_job_1",
                "min": 2.2,
                "mean": 12.3,
                "median": 17.2,
                "max": 36.0,
                "unit": "ms",
            }
        ]
        assert opm2["duration"] == 600 * 1000

        assert stats.young_gc_time == 100
        assert stats.young_gc_count == 1
        assert stats.old_gc_time == 100
        assert stats.old_gc_count == 1
        assert stats.zgc_cycles_gc_time == 100
        assert stats.zgc_cycles_gc_count == 1
        assert stats.zgc_pauses_gc_time == 100
        assert stats.zgc_pauses_gc_count == 1

    def test_calculate_system_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "system", "race.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "reporting", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "mechanic", "car.names", ["unittest_car"])
        cfg.add(config.Scope.application, "mechanic", "car.params", {})
        cfg.add(config.Scope.application, "mechanic", "plugin.params", {})
        cfg.add(config.Scope.application, "race", "user.tags", {})
        cfg.add(config.Scope.application, "race", "pipeline", "from-sources")
        cfg.add(config.Scope.application, "track", "params", {})

        index = track.Task(name="index #1", operation=track.Operation(name="index", operation_type=track.OperationType.Bulk, params=None))
        challenge = track.Challenge(name="unittest", schedule=[index], default=True)
        t = track.Track("unittest", "unittest-track", challenges=[challenge])

        store = metrics.metrics_store(cfg, read_only=False, track=t, challenge=challenge)
        store.add_meta_info(metrics.MetaInfoScope.node, "rally-node-0", "node_name", "rally-node-0")

        store.put_value_node_level("rally-node-0", "final_index_size_bytes", 2048, unit="bytes")
        # ensure this value will be filtered as it does not belong to our node
        store.put_value_node_level("rally-node-1", "final_index_size_bytes", 4096, unit="bytes")

        stats = metrics.calculate_system_results(store, "rally-node-0")

        del store

        assert stats.node_metrics == [
            {
                "node": "rally-node-0",
                "name": "index_size",
                "value": 2048,
                "unit": "bytes",
            }
        ]


def select(l, name, operation=None, job=None, node=None):
    for item in l:
        if item["name"] == name and item.get("operation") == operation and item.get("node") == node and item.get("job") == job:
            return item
    return None


class TestGlobalStatsCalculator:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "fb26018b-428d-4528-b36b-cf8c54a303ec"

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {})
        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)

    def teardown_method(self, method):
        del self.metrics_store
        del self.cfg

    def test_add_administrative_task_with_error_rate_in_report(self):
        op = Operation(name="delete-index", operation_type="DeleteIndex", params={"include-in-reporting": False})
        task = Task("delete-index", operation=op, schedule="deterministic")
        challenge = Challenge(name="append-fast-with-conflicts", schedule=[task], meta_data={})

        self.metrics_store.open(
            self.RACE_ID,
            self.RACE_TIMESTAMP,
            "test",
            "append-fast-with-conflicts",
            "defaults",
            create=True,
        )
        self.metrics_store.put_doc(
            doc={
                "@timestamp": 1595896761994,
                "relative-time": 283.382,
                "race-id": "fb26018b-428d-4528-b36b-cf8c54a303ec",
                "race-timestamp": "20200728T003905Z",
                "environment": "local",
                "track": "geonames",
                "challenge": "append-fast-with-conflicts",
                "car": "defaults",
                "name": "service_time",
                "value": 72.67997100007051,
                "unit": "ms",
                "sample-type": "normal",
                "meta": {
                    "source_revision": "7f634e9f44834fbc12724506cc1da681b0c3b1e3",
                    "distribution_version": "7.6.0",
                    "distribution_flavor": "oss",
                    "success": False,
                },
                "task": "delete-index",
                "operation": "delete-index",
                "operation-type": "DeleteIndex",
            }
        )

        result = GlobalStatsCalculator(store=self.metrics_store, track=Track(name="geonames", meta_data={}), challenge=challenge)()
        assert "delete-index" in [op_metric.get("task") for op_metric in result.op_metrics]


class TestGlobalStats:
    def test_as_flat_list(self):
        d = {
            "op_metrics": [
                {
                    "task": "index #1",
                    "operation": "index",
                    "throughput": {
                        "min": 450,
                        "mean": 450,
                        "median": 450,
                        "max": 452,
                        "unit": "docs/s",
                    },
                    "latency": {
                        "50": 340,
                        "100": 376,
                    },
                    "service_time": {
                        "50": 341,
                        "100": 376,
                    },
                    "error_rate": 0.0,
                    "meta": {
                        "clients": 8,
                        "phase": "idx",
                    },
                },
                {
                    "task": "search #2",
                    "operation": "search",
                    "throughput": {
                        "min": 9,
                        "mean": 10,
                        "median": 10,
                        "max": 12,
                        "unit": "ops/s",
                    },
                    "latency": {
                        "50": 99,
                        "100": 111,
                    },
                    "service_time": {
                        "50": 98,
                        "100": 110,
                    },
                    "error_rate": 0.1,
                },
            ],
            "ml_processing_time": [
                {
                    "job": "job_1",
                    "min": 3.3,
                    "mean": 5.2,
                    "median": 5.8,
                    "max": 12.34,
                },
                {
                    "job": "job_2",
                    "min": 3.55,
                    "mean": 4.2,
                    "median": 4.9,
                    "max": 9.4,
                },
            ],
            "young_gc_time": 68,
            "young_gc_count": 7,
            "old_gc_time": 0,
            "old_gc_count": 0,
            "zgc_cycles_gc_time": 100,
            "zgc_cycles_gc_count": 1,
            "zgc_pauses_gc_time": 50,
            "zgc_pauses_gc_count": 1,
            "merge_time": 3702,
            "merge_time_per_shard": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms",
            },
            "merge_count": 2,
            "refresh_time": 596,
            "refresh_time_per_shard": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms",
            },
            "refresh_count": 10,
            "flush_time": None,
            "flush_time_per_shard": {},
            "flush_count": 0,
        }

        s = metrics.GlobalStats(d)
        metric_list = s.as_flat_list()
        assert select(metric_list, "throughput", operation="index") == {
            "name": "throughput",
            "task": "index #1",
            "operation": "index",
            "value": {
                "min": 450,
                "mean": 450,
                "median": 450,
                "max": 452,
                "unit": "docs/s",
            },
            "meta": {
                "clients": 8,
                "phase": "idx",
            },
        }

        assert select(metric_list, "service_time", operation="index") == {
            "name": "service_time",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 341,
                "100": 376,
            },
            "meta": {
                "clients": 8,
                "phase": "idx",
            },
        }

        assert select(metric_list, "latency", operation="index") == {
            "name": "latency",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 340,
                "100": 376,
            },
            "meta": {
                "clients": 8,
                "phase": "idx",
            },
        }

        assert select(metric_list, "error_rate", operation="index") == {
            "name": "error_rate",
            "task": "index #1",
            "operation": "index",
            "value": {
                "single": 0.0,
            },
            "meta": {
                "clients": 8,
                "phase": "idx",
            },
        }

        assert select(metric_list, "throughput", operation="search") == {
            "name": "throughput",
            "task": "search #2",
            "operation": "search",
            "value": {
                "min": 9,
                "mean": 10,
                "median": 10,
                "max": 12,
                "unit": "ops/s",
            },
        }

        assert select(metric_list, "service_time", operation="search") == {
            "name": "service_time",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 98,
                "100": 110,
            },
        }

        assert select(metric_list, "latency", operation="search") == {
            "name": "latency",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 99,
                "100": 111,
            },
        }

        assert select(metric_list, "error_rate", operation="search") == {
            "name": "error_rate",
            "task": "search #2",
            "operation": "search",
            "value": {
                "single": 0.1,
            },
        }

        assert select(metric_list, "ml_processing_time", job="job_1") == {
            "name": "ml_processing_time",
            "job": "job_1",
            "value": {
                "min": 3.3,
                "mean": 5.2,
                "median": 5.8,
                "max": 12.34,
            },
        }

        assert select(metric_list, "ml_processing_time", job="job_2") == {
            "name": "ml_processing_time",
            "job": "job_2",
            "value": {
                "min": 3.55,
                "mean": 4.2,
                "median": 4.9,
                "max": 9.4,
            },
        }

        assert select(metric_list, "young_gc_time") == {
            "name": "young_gc_time",
            "value": {
                "single": 68,
            },
        }
        assert select(metric_list, "young_gc_count") == {
            "name": "young_gc_count",
            "value": {
                "single": 7,
            },
        }

        assert select(metric_list, "old_gc_time") == {
            "name": "old_gc_time",
            "value": {
                "single": 0,
            },
        }
        assert select(metric_list, "old_gc_count") == {
            "name": "old_gc_count",
            "value": {
                "single": 0,
            },
        }

        assert select(metric_list, "zgc_cycles_gc_time") == {
            "name": "zgc_cycles_gc_time",
            "value": {
                "single": 100,
            },
        }
        assert select(metric_list, "zgc_cycles_gc_count") == {
            "name": "zgc_cycles_gc_count",
            "value": {
                "single": 1,
            },
        }

        assert select(metric_list, "zgc_pauses_gc_time") == {
            "name": "zgc_pauses_gc_time",
            "value": {
                "single": 50,
            },
        }
        assert select(metric_list, "zgc_pauses_gc_count") == {
            "name": "zgc_pauses_gc_count",
            "value": {
                "single": 1,
            },
        }

        assert select(metric_list, "merge_time") == {
            "name": "merge_time",
            "value": {
                "single": 3702,
            },
        }

        assert select(metric_list, "merge_time_per_shard") == {
            "name": "merge_time_per_shard",
            "value": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms",
            },
        }

        assert select(metric_list, "merge_count") == {
            "name": "merge_count",
            "value": {
                "single": 2,
            },
        }

        assert select(metric_list, "refresh_time") == {
            "name": "refresh_time",
            "value": {
                "single": 596,
            },
        }

        assert select(metric_list, "refresh_time_per_shard") == {
            "name": "refresh_time_per_shard",
            "value": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms",
            },
        }

        assert select(metric_list, "refresh_count") == {
            "name": "refresh_count",
            "value": {
                "single": 10,
            },
        }

        assert select(metric_list, "flush_time") is None
        assert select(metric_list, "flush_time_per_shard") is None
        assert select(metric_list, "flush_count") == {
            "name": "flush_count",
            "value": {
                "single": 0,
            },
        }


class TestSystemStats:
    def test_as_flat_list(self):
        d = {
            "node_metrics": [
                {"node": "rally-node-0", "name": "startup_time", "value": 3.4},
                {"node": "rally-node-1", "name": "startup_time", "value": 4.2},
                {"node": "rally-node-0", "name": "index_size", "value": 300 * 1024 * 1024},
                {"node": "rally-node-1", "name": "index_size", "value": 302 * 1024 * 1024},
                {"node": "rally-node-0", "name": "bytes_written", "value": 817 * 1024 * 1024},
                {"node": "rally-node-1", "name": "bytes_written", "value": 833 * 1024 * 1024},
            ],
        }

        s = metrics.SystemStats(d)
        metric_list = s.as_flat_list()

        assert select(metric_list, "startup_time", node="rally-node-0") == {
            "node": "rally-node-0",
            "name": "startup_time",
            "value": {
                "single": 3.4,
            },
        }

        assert select(metric_list, "startup_time", node="rally-node-1") == {
            "node": "rally-node-1",
            "name": "startup_time",
            "value": {
                "single": 4.2,
            },
        }

        assert select(metric_list, "index_size", node="rally-node-0") == {
            "node": "rally-node-0",
            "name": "index_size",
            "value": {
                "single": 300 * 1024 * 1024,
            },
        }

        assert select(metric_list, "index_size", node="rally-node-1") == {
            "node": "rally-node-1",
            "name": "index_size",
            "value": {
                "single": 302 * 1024 * 1024,
            },
        }

        assert select(metric_list, "bytes_written", node="rally-node-0") == {
            "node": "rally-node-0",
            "name": "bytes_written",
            "value": {
                "single": 817 * 1024 * 1024,
            },
        }

        assert select(metric_list, "bytes_written", node="rally-node-1") == {
            "node": "rally-node-1",
            "name": "bytes_written",
            "value": {
                "single": 833 * 1024 * 1024,
            },
        }


class TestIndexTemplateProvider:
    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", TestFileRaceStore.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", TestFileRaceStore.RACE_ID)

    def test_primary_and_replica_shard_count_specified_index_template_update(self):
        _datastore_type = "elasticsearch"
        _datastore_number_of_shards = random.randint(1, 100)
        _datastore_number_of_replicas = random.randint(0, 100)

        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", _datastore_number_of_shards)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.annotations_template(),
            _index_template_provider.metrics_template(),
            _index_template_provider.races_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == _datastore_number_of_shards
            assert t["settings"]["index"]["number_of_replicas"] == _datastore_number_of_replicas

    def test_primary_shard_count_specified_index_template_update(self):
        _datastore_type = "elasticsearch"
        _datastore_number_of_shards = random.randint(1, 100)

        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", _datastore_number_of_shards)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.annotations_template(),
            _index_template_provider.metrics_template(),
            _index_template_provider.races_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == _datastore_number_of_shards
            with pytest.raises(KeyError):
                # pylint: disable=unused-variable
                number_of_replicas = t["settings"]["index"]["number_of_replicas"]

    def test_replica_shard_count_specified_index_template_update(self):
        _datastore_type = "elasticsearch"
        _datastore_number_of_replicas = random.randint(1, 100)

        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.annotations_template(),
            _index_template_provider.metrics_template(),
            _index_template_provider.races_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_replicas"] == _datastore_number_of_replicas
            with pytest.raises(KeyError):
                # pylint: disable=unused-variable
                number_of_shards = t["settings"]["index"]["number_of_shards"]

    def test_primary_shard_count_less_than_one(self):
        _datastore_type = "elasticsearch"
        _datastore_number_of_shards = 0

        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", _datastore_number_of_shards)
        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        with pytest.raises(exceptions.SystemSetupError) as ctx:
            # pylint: disable=unused-variable
            templates = [
                _index_template_provider.annotations_template(),
                _index_template_provider.metrics_template(),
                _index_template_provider.races_template(),
                _index_template_provider.results_template(),
            ]
        assert ctx.value.args[0] == (
            "The setting: datastore.number_of_shards must be >= 1. Please check the configuration in "
            f"{_index_template_provider._config.config_file.location}"
        )

    def test_primary_and_replica_shard_counts_passed_as_strings(self):
        _datastore_type = "elasticsearch"
        _datastore_number_of_shards = "200"
        _datastore_number_of_replicas = "1"

        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", _datastore_number_of_shards)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.annotations_template(),
            _index_template_provider.metrics_template(),
            _index_template_provider.races_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == 200
            assert t["settings"]["index"]["number_of_replicas"] == 1
