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
from __future__ import annotations

import datetime
import json
import logging
import os
import random
import tempfile
import uuid
from dataclasses import dataclass
from unittest import mock

import elasticsearch.exceptions
import elasticsearch.helpers
import pytest

from esrally import client, config, exceptions, metrics, paths, time, track
from esrally.metrics import GlobalStatsCalculator
from esrally.track import Challenge, Operation, Task, Track
from esrally.utils import cases, opts


def rally_metric_template():
    return {
        "mappings": {
            "_source": {"enabled": True},
            "date_detection": False,
            "dynamic_templates": [
                {"strings": {"mapping": {"ignore_above": 8191, "type": "keyword"}, "match": "*", "match_mapping_type": "string"}}
            ],
            "properties": {
                "@timestamp": {"format": "epoch_millis", "type": "date"},
                "car": {"type": "keyword"},
                "challenge": {"type": "keyword"},
                "environment": {"type": "keyword"},
                "job": {"type": "keyword"},
                "max": {"type": "float"},
                "mean": {"type": "float"},
                "median": {"type": "float"},
                "meta": {"properties": {"error-description": {"type": "wildcard"}}},
                "min": {"type": "float"},
                "name": {"type": "keyword"},
                "operation": {"type": "keyword"},
                "operation-type": {"type": "keyword"},
                "race-id": {"type": "keyword"},
                "race-timestamp": {"fields": {"raw": {"type": "keyword"}}, "format": "basic_date_time_no_millis", "type": "date"},
                "relative-time": {"type": "float"},
                "sample-type": {"type": "keyword"},
                "task": {"type": "keyword"},
                "track": {"type": "keyword"},
                "unit": {"type": "keyword"},
                "value": {"type": "float"},
            },
        },
        "settings": {"index": {"mapping": {"total_fields": {"limit": "2000"}}, "number_of_replicas": "3", "number_of_shards": "3"}},
    }


def provided_metrics_template() -> dict:
    template = rally_metric_template()
    template["settings"]["index"] = {"mapping.total_fields.limit": 2000, "number_of_shards": 1, "number_of_replicas": 1}
    return template


class MockClientFactory:

    def __init__(self, cfg):
        self._es = mock.create_autospec(metrics.EsClient)

    def create(self):
        return self._es


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


class TestTrackParamsForReporting:
    def test_empty(self):
        assert metrics.track_params_for_reporting({}) == {}
        assert metrics.track_params_for_reporting(None) == {}

    def test_redacts_secret_prefix_values(self):
        params = {"clients": 8, "secret_api_key": "x", "secret_": "edge", "SECRET_not_filtered": 1}
        assert metrics.track_params_for_reporting(params) == {
            "clients": 8,
            "secret_api_key": metrics.SECRET_TRACK_PARAM_PLACEHOLDER,
            "secret_": metrics.SECRET_TRACK_PARAM_PLACEHOLDER,
            "SECRET_not_filtered": 1,
        }


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
    def test_config_opts_parsing_basic(self, password_configuration, monkeypatch):
        cfg = config.Config()

        _datastore_host = "rally-metrics-123abc.es.us-east-1.aws.elastic.cloud"
        _datastore_port = 443
        _datastore_secure = True
        _datastore_user = "metricsuser"
        _datastore_password = "metricspassword"
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
            monkeypatch.setattr(metrics, "DATASTORE_PASSWORD", _datastore_password)

        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.ssl.verification_mode", "none")

        client_factory = mock.create_autospec(client.EsClientFactory)
        try:
            metrics.EsClientFactory(cfg, client_factory=client_factory)
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

        client_factory.assert_called_with(
            hosts=[{"host": _datastore_host, "port": _datastore_port}],
            client_options=expected_client_options,
            distribution_version=None,
            distribution_flavor=None,
        )

    @pytest.mark.parametrize("apikey_configuration", ["config", "environment"])
    def test_config_opts_parsing_apikey(self, apikey_configuration, monkeypatch):
        cfg = config.Config()

        _datastore_host = "rally-metrics-123abc.es.us-east-1.aws.elastic.cloud"
        _datastore_port = 443
        _datastore_secure = True
        _datastore_apikey = "METRICSAPIKEYTEST=="
        _datastore_verify_certs = False
        _datastore_probe_version = False

        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.probe.cluster_version", _datastore_probe_version)

        if apikey_configuration == "config":
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.api_key", _datastore_apikey)
        elif apikey_configuration == "environment":
            monkeypatch.setattr(metrics, "DATASTORE_API_KEY", _datastore_apikey)

        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.ssl.verification_mode", "none")

        client_factory = mock.create_autospec(client.EsClientFactory)
        try:
            metrics.EsClientFactory(cfg, client_factory=client_factory)
        except exceptions.ConfigError as e:
            if apikey_configuration is not None:
                raise

            assert (
                e.message == "Either basic authentication (username and password) or an API key is required in the reporting configuration."
            )
            return

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "verify_certs": _datastore_verify_certs,
            "api_key": _datastore_apikey,
        }

        client_factory.assert_called_with(
            hosts=[{"host": _datastore_host, "port": _datastore_port}],
            client_options=expected_client_options,
            distribution_version=None,
            distribution_flavor=None,
        )

    def test_config_opts_parsing_both_password_apikey(self, monkeypatch):
        cfg = config.Config()

        _datastore_host = "rally-metrics-123abc.es.us-east-1.aws.elastic.cloud"
        _datastore_port = 443
        _datastore_secure = True
        _datastore_user = "metricsuser"
        _datastore_password = "metricspassword"
        _datastore_apikey = "METRICSAPIKEYTEST=="
        _datastore_verify_certs = False
        _datastore_probe_version = False
        _datastore_verify_certs = False
        _datastore_probe_version = False

        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.user", _datastore_user)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.probe.cluster_version", _datastore_probe_version)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.password", _datastore_password)
        cfg.add(config.Scope.applicationOverride, "reporting", "datastore.api_key", _datastore_apikey)

        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "reporting", "datastore.ssl.verification_mode", "none")

        client_factory = mock.create_autospec(client.EsClientFactory)
        try:
            metrics.EsClientFactory(cfg, client_factory=client_factory)
        except exceptions.ConfigError as e:
            assert (
                e.message
                == "Both basic authentication (username/password) and API key are provided. Please provide only one authentication method."
            )
            return

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "verify_certs": _datastore_verify_certs,
            "api_key": _datastore_apikey,
        }

        client_factory.assert_called_with(
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
                    "metrics store on host [127.0.0.1] at port [9200]. args: [()], kwargs: [{}]"
                )
                return logging_statements

            def raise_error(self):
                err = elasticsearch.exceptions.ApiError(
                    "unit-test",
                    # TODO remove this ignore when introducing type hints
                    meta=TestEsClient.ApiResponseMeta(status=self.status_code),  # type: ignore[arg-type]
                    body={},
                )
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
            raise elasticsearch.exceptions.AuthenticationException(
                meta=None, body=None, message="unit-test"  # type: ignore[arg-type]  # TODO remove this ignore when introducing type hints
            )

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with pytest.raises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authentication_error)
        assert ctx.value.args[0] == (
            "The configured user could not authenticate against your Elasticsearch metrics store running on host [127.0.0.1] "
            f"at port [9243] (wrong password?). Please fix the configuration in [{paths.rally_confdir()}/rally.ini]."
        )

    def test_raises_sytem_setup_error_on_authorization_problems(self):
        def raise_authorization_error():
            raise elasticsearch.exceptions.AuthorizationException(
                meta=None, body=None, message="unit-test"  # type: ignore[arg-type]  # TODO remove this ignore when introducing type hints
            )

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
            "store on host [127.0.0.1] at port [9243]. args: [()], kwargs: [{}]"
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

    @mock.patch("random.random")
    @mock.patch("esrally.time.sleep")
    def test_bulk_index_error_retryable_via_create_key(self, mocked_sleep, mocked_random):
        # When data streams are in use, Elasticsearch structures bulk errors under "create",
        # not "index". A retryable status (429) must still be retried, not treated as fatal.
        mocked_random.return_value = 0

        bulk_index_errors = [
            {
                "create": {
                    "_index": "rally-metrics-v1",
                    "_id": None,
                    "status": 429,
                    "error": {"type": "circuit_breaking_exception", "reason": "Data too large"},
                }
            }
        ]

        call_count = 0

        def raise_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise elasticsearch.helpers.BulkIndexError("1 document(s) failed to index", bulk_index_errors)

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))
        client.guarded(raise_then_succeed)
        assert call_count == 2
        mocked_sleep.assert_called_once_with(1)

    def test_bulk_index_error_unretryable_via_create_key(self):
        # An unretryable error under "create" must raise RallyError immediately.
        bulk_index_errors = [
            {
                "create": {
                    "_index": "rally-metrics-v1",
                    "_id": None,
                    "status": 409,
                    "error": {"type": "version_conflict_engine_exception"},
                }
            }
        ]

        def raise_bulk_index_error():
            raise elasticsearch.helpers.BulkIndexError("1 document(s) failed to index", bulk_index_errors)

        client = metrics.EsClient(self.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))
        with pytest.raises(
            exceptions.RallyError,
            match=r"Unretryable error encountered when sending metrics to remote metrics store: \[version_conflict_engine_exception\]",
        ):
            client.guarded(raise_bulk_index_error)


class TestIndexTemplateProvider:
    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", TestFileRaceStore.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", TestFileRaceStore.RACE_ID)

    def _make_provider(self, number_of_shards=None, number_of_replicas=None):
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", "elasticsearch")
        if number_of_shards is not None:
            self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", number_of_shards)
        if number_of_replicas is not None:
            self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_replicas", number_of_replicas)
        return metrics.IndexTemplateProvider(self.cfg)

    def _all_templates(self, provider):
        return [
            provider.annotations_template(),
            provider.get_template(metrics.EsStoreType.metrics),
            provider.get_template(metrics.EsStoreType.races),
            provider.get_template(metrics.EsStoreType.results),
        ]

    @dataclass
    class ShardSettingsCase:
        number_of_shards: int | str | None = None
        number_of_replicas: int | str | None = None
        expected_shards: int | None = None
        expected_replicas: int | None = None
        expect_error: bool = False

    @cases.cases(
        both_specified=ShardSettingsCase(
            number_of_shards=random.randint(1, 100),
            number_of_replicas=random.randint(0, 100),
        ),
        shards_only=ShardSettingsCase(
            number_of_shards=random.randint(1, 100),
        ),
        replicas_only=ShardSettingsCase(
            number_of_replicas=random.randint(1, 100),
        ),
        as_strings=ShardSettingsCase(
            number_of_shards="200",
            number_of_replicas="1",
            expected_shards=200,
            expected_replicas=1,
        ),
        shards_less_than_one=ShardSettingsCase(
            number_of_shards=0,
            expect_error=True,
        ),
    )
    def test_shard_settings(self, case: ShardSettingsCase):
        if case.expect_error:
            provider = self._make_provider(number_of_shards=case.number_of_shards, number_of_replicas=case.number_of_replicas)
            with pytest.raises(exceptions.SystemSetupError, match="datastore.number_of_shards must be >= 1"):
                self._all_templates(provider)
            return

        provider = self._make_provider(number_of_shards=case.number_of_shards, number_of_replicas=case.number_of_replicas)

        want_shards = case.expected_shards if case.expected_shards is not None else case.number_of_shards
        want_replicas = case.expected_replicas if case.expected_replicas is not None else case.number_of_replicas

        for template in self._all_templates(provider):
            t = json.loads(template)
            idx_settings = t["template"]["settings"]["index"]
            if want_shards is not None:
                assert idx_settings["number_of_shards"] == want_shards
            else:
                assert "number_of_shards" not in idx_settings
            if want_replicas is not None:
                assert idx_settings["number_of_replicas"] == want_replicas
            else:
                assert "number_of_replicas" not in idx_settings

    def test_templates_have_timestamp(self):
        provider = self._make_provider()

        # annotations don't have @timestamp
        annotations = json.loads(provider.annotations_template())
        assert "@timestamp" not in annotations["template"]["mappings"]["properties"]
        assert annotations["index_patterns"] == ["rally-annotations"]

        # all other templates have @timestamp from the resource files
        for es_store_type in [metrics.EsStoreType.metrics, metrics.EsStoreType.races, metrics.EsStoreType.results]:
            t = json.loads(provider.get_template(es_store_type))
            assert t["index_patterns"] == [f"{es_store_type.index_prefix}*"]
            assert t["template"]["mappings"]["properties"]["@timestamp"] == {"type": "date", "format": "epoch_millis"}


class TestComponentTemplateProvider:
    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", TestFileRaceStore.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", TestFileRaceStore.RACE_ID)

    def _make_provider(self, number_of_shards=None, number_of_replicas=None):
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.type", "elasticsearch")
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.use_data_streams", True)
        if number_of_shards is not None:
            self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_shards", number_of_shards)
        if number_of_replicas is not None:
            self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.number_of_replicas", number_of_replicas)
        return metrics.ComponentTemplateProvider(self.cfg)

    @dataclass
    class StoreTypeCase:
        store_name: str
        lifecycle_policy: str

    @cases.cases(
        metrics=StoreTypeCase(store_name="metrics", lifecycle_policy="rally-metrics-default"),
        races=StoreTypeCase(store_name="races", lifecycle_policy="rally-races-default"),
        results=StoreTypeCase(store_name="results", lifecycle_policy="rally-results-default"),
    )
    def test_component_template_structure(self, case: StoreTypeCase):
        provider = self._make_provider()
        template_fn = getattr(provider, "get_template")
        es_store_type = metrics.EsStoreType[case.store_name]
        components = json.loads(template_fn(es_store_type))

        versioned_name = f"{es_store_type.index_prefix}{es_store_type.data_stream_version}"
        custom_key = f"{versioned_name}{metrics.ComponentTemplateProvider.COMPONENT_TEMPLATE_CUSTOM_SUFFIX}"

        assert set(components.keys()) == {versioned_name, custom_key}

        # main component has both mappings and settings with lifecycle
        main_tmpl = json.loads(components[versioned_name])
        assert "mappings" in main_tmpl["template"]
        assert main_tmpl["template"]["mappings"]["properties"]["@timestamp"] == {"type": "date", "format": "epoch_millis"}
        assert main_tmpl["template"]["settings"]["index"]["lifecycle"]["name"] == case.lifecycle_policy

        # custom component is an empty placeholder
        custom_tmpl = json.loads(components[custom_key])
        assert custom_tmpl["template"] == {}

    def test_shard_settings_ignored_for_component_templates(self):
        provider = self._make_provider(number_of_shards=3, number_of_replicas=2)

        for store_name in ["metrics", "races", "results"]:
            es_store_type = metrics.EsStoreType[store_name]
            components = json.loads(provider.get_template(es_store_type))
            versioned_name = f"{es_store_type.index_prefix}{es_store_type.data_stream_version}"
            idx_settings = json.loads(components[versioned_name])["template"]["settings"]["index"]

            assert "number_of_shards" not in idx_settings
            assert "number_of_replicas" not in idx_settings

    def test_annotations_template_is_inherited_from_base(self):
        provider = self._make_provider()
        annotations = json.loads(provider.annotations_template())
        assert "data_stream" not in annotations
        assert "template" in annotations
        assert annotations["index_patterns"] == ["rally-annotations"]
        assert "@timestamp" not in annotations["template"]["mappings"]["properties"]

    def test_component_names(self):
        provider = self._make_provider()
        for es_store_type in [metrics.EsStoreType.metrics, metrics.EsStoreType.races, metrics.EsStoreType.results]:
            names = provider.component_names(es_store_type)
            versioned_name = f"{es_store_type.index_prefix}{es_store_type.data_stream_version}"
            assert names == [
                versioned_name,
                f"{versioned_name}{metrics.ComponentTemplateProvider.COMPONENT_TEMPLATE_CUSTOM_SUFFIX}",
            ]


class TestIndexHandler:
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)

    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.client = mock.create_autospec(metrics.EsClient)

    def test_data_stream_template(self):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        for es_store_type in [metrics.EsStoreType.metrics, metrics.EsStoreType.races, metrics.EsStoreType.results]:
            handler = metrics.IndexHandler(self.cfg, self.client, es_store_type)
            component_names = handler._index_template_provider.component_names(es_store_type)
            index_template = json.loads(handler._data_stream_template(component_names))

            assert index_template["index_patterns"] == [f"{es_store_type.index_prefix}{es_store_type.data_stream_version}"]
            assert index_template["data_stream"] == {}
            assert index_template["composed_of"] == component_names
            assert index_template["priority"] == metrics.IndexHandler.TEMPLATE_PRIORITY

    @dataclass
    class IndexNameCase:
        es_store_type: metrics.EsStoreType
        use_data_streams: bool

    @cases.cases(
        metrics_data_streams=IndexNameCase(es_store_type=metrics.EsStoreType.metrics, use_data_streams=True),
        races_data_streams=IndexNameCase(es_store_type=metrics.EsStoreType.races, use_data_streams=True),
        results_data_streams=IndexNameCase(es_store_type=metrics.EsStoreType.results, use_data_streams=True),
        metrics_date_based=IndexNameCase(es_store_type=metrics.EsStoreType.metrics, use_data_streams=False),
        races_date_based=IndexNameCase(es_store_type=metrics.EsStoreType.races, use_data_streams=False),
        results_date_based=IndexNameCase(es_store_type=metrics.EsStoreType.results, use_data_streams=False),
    )
    def test_index_name(self, case: IndexNameCase):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", case.use_data_streams)
        handler = metrics.IndexHandler(self.cfg, self.client, case.es_store_type)

        if case.use_data_streams:
            assert handler.index_name(self.RACE_TIMESTAMP) == f"{case.es_store_type.index_prefix}{case.es_store_type.data_stream_version}"
            assert (
                handler.index_name(time.to_iso8601(self.RACE_TIMESTAMP))
                == f"{case.es_store_type.index_prefix}{case.es_store_type.data_stream_version}"
            )
        else:
            assert handler.index_name(self.RACE_TIMESTAMP) == f"{case.es_store_type.index_prefix}2016-01"
            assert handler.index_name(time.to_iso8601(self.RACE_TIMESTAMP)) == f"{case.es_store_type.index_prefix}2016-01"

    @dataclass
    class ShouldApplyUpdateCase:
        old_resource: object | None = None
        new_resource: object | None = None
        overwrite_templates: bool = False
        expected: bool = True

    @cases.cases(
        old_is_none=ShouldApplyUpdateCase(
            old_resource=None,
            new_resource={"key": "value"},
            expected=True,
        ),
        identical=ShouldApplyUpdateCase(
            old_resource={"key": "value"},
            new_resource={"key": "value"},
            expected=False,
        ),
        diff_no_overwrite=ShouldApplyUpdateCase(
            old_resource={"key": "old"},
            new_resource={"key": "new"},
            overwrite_templates=False,
            expected=False,
        ),
        diff_with_overwrite=ShouldApplyUpdateCase(
            old_resource={"key": "old"},
            new_resource={"key": "new"},
            overwrite_templates=True,
            expected=True,
        ),
    )
    def test_should_apply_update(self, case: ShouldApplyUpdateCase):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", False)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.overwrite_existing_templates", case.overwrite_templates)
        handler = metrics.IndexHandler(self.cfg, self.client, metrics.EsStoreType.metrics)
        result = handler._should_apply_update("test resource", case.old_resource, case.new_resource)
        assert result == case.expected

    @dataclass
    class DataStreamEnsureTemplateCase:
        create: bool = True
        es_store_type: metrics.EsStoreType = metrics.EsStoreType.metrics
        lifecycle_exists: bool = False
        component_templates_exist: bool = False
        index_template_exists: bool = False
        identical: bool = False
        overwrite_templates: bool = False
        expect_put_lifecycle: bool = False
        expect_put_component_templates: int = 0
        expect_put_template: bool = True
        expect_get_template: bool = False

    @dataclass
    class DateBasedEnsureTemplateCase:
        create: bool = True
        es_store_type: metrics.EsStoreType = metrics.EsStoreType.metrics
        index_template_exists: bool = False
        identical: bool = False
        overwrite_templates: bool = False
        expect_index_template_exists: bool = True
        expect_put_template: bool = True
        expect_get_template: bool = False

    def _assert_index_template_calls(
        self,
        use_data_streams,
        es_store_type,
        expect_get_template,
        expect_put_template,
        expect_index_template_exists=True,
    ):
        if use_data_streams:
            index_template_name = es_store_type.data_stream_template_name
        else:
            index_template_name = es_store_type.date_based_template_name
        if expect_index_template_exists:
            self.client.index_template_exists.assert_called_once_with(index_template_name)
        else:
            self.client.index_template_exists.assert_not_called()
        if expect_get_template:
            self.client.get_template.assert_called_with(index_template_name)
        if expect_put_template:
            self.client.put_template.assert_called_once_with(index_template_name, mock.ANY)
        else:
            self.client.put_template.assert_not_called()

    @cases.cases(
        fresh=DataStreamEnsureTemplateCase(
            expect_put_lifecycle=True,
            expect_put_component_templates=2,
        ),
        all_identical=DataStreamEnsureTemplateCase(
            lifecycle_exists=True,
            component_templates_exist=True,
            index_template_exists=True,
            identical=True,
            expect_put_template=False,
            expect_get_template=True,
        ),
        all_differ_no_overwrite=DataStreamEnsureTemplateCase(
            lifecycle_exists=True,
            component_templates_exist=True,
            index_template_exists=True,
            expect_put_template=False,
            expect_get_template=True,
        ),
        all_differ_overwrite=DataStreamEnsureTemplateCase(
            lifecycle_exists=True,
            component_templates_exist=True,
            index_template_exists=True,
            overwrite_templates=True,
            expect_put_lifecycle=True,
            expect_put_component_templates=1,
            expect_get_template=True,
        ),
    )
    def test_ensure_index_template_data_stream(self, case: DataStreamEnsureTemplateCase):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.overwrite_existing_templates", case.overwrite_templates)

        handler = metrics.IndexHandler(self.cfg, self.client, case.es_store_type)

        if not case.lifecycle_exists:
            handler._ilm_default_template = mock.MagicMock(return_value=json.dumps({"policy": {}}))

        real_component_templates = json.loads(handler._index_template_provider.get_template(case.es_store_type))

        if case.lifecycle_exists:
            if case.identical:
                ilm_body = json.loads(handler._ilm_default_template(case.es_store_type.ilm_default_resource))
            else:
                ilm_body = {"policy": {}}
            self.client.get_lifecycle.return_value = mock.MagicMock(body={case.es_store_type.ilm_default_name: ilm_body})
        else:
            self.client.get_lifecycle.side_effect = Exception("lifecycle not found")

        self.client.component_template_exists.return_value = case.component_templates_exist
        if case.component_templates_exist:

            def _get_component_template(name):
                if case.identical:
                    real_body = json.loads(real_component_templates[name])
                else:
                    real_body = {"template": {}}
                return mock.MagicMock(body={"component_templates": [{"component_template": real_body}]})

            self.client.get_component_template.side_effect = _get_component_template

        self.client.index_template_exists.return_value = case.index_template_exists
        if case.index_template_exists:
            if case.identical:
                real_ds_template = {
                    "index_patterns": [f"{case.es_store_type.index_prefix}{case.es_store_type.data_stream_version}"],
                    "data_stream": {},
                    "composed_of": list(real_component_templates.keys()),
                    "priority": metrics.IndexHandler.TEMPLATE_PRIORITY,
                }
            else:
                real_ds_template = {}
            self.client.get_template.return_value = mock.MagicMock(body={"index_templates": [{"index_template": real_ds_template}]})

        handler.ensure_index_template(create=case.create)

        self.client.get_lifecycle.assert_called_with(case.es_store_type.ilm_default_name)
        if case.expect_put_lifecycle:
            self.client.put_lifecycle.assert_called_once_with(case.es_store_type.ilm_default_name, mock.ANY)
        else:
            self.client.put_lifecycle.assert_not_called()

        assert self.client.component_template_exists.call_count == 2
        if case.expect_put_component_templates:
            assert self.client.put_component_template.call_count == case.expect_put_component_templates
        else:
            self.client.put_component_template.assert_not_called()

        self._assert_index_template_calls(
            True,
            case.es_store_type,
            expect_get_template=case.expect_get_template,
            expect_put_template=case.expect_put_template,
        )

    @cases.cases(
        fresh_create=DateBasedEnsureTemplateCase(),
        identical_create=DateBasedEnsureTemplateCase(
            index_template_exists=True,
            identical=True,
            expect_put_template=False,
            expect_get_template=True,
        ),
        differ_no_overwrite_create=DateBasedEnsureTemplateCase(
            index_template_exists=True,
            expect_put_template=False,
            expect_get_template=True,
        ),
        differ_overwrite_new_index=DateBasedEnsureTemplateCase(
            index_template_exists=True,
            overwrite_templates=True,
            expect_get_template=True,
        ),
        differ_overwrite_index_exists=DateBasedEnsureTemplateCase(
            index_template_exists=True,
            overwrite_templates=True,
            expect_get_template=True,
        ),
        read_only_open_does_not_touch_templates=DateBasedEnsureTemplateCase(
            create=False,
            expect_index_template_exists=False,
            expect_put_template=False,
            expect_get_template=False,
        ),
    )
    def test_ensure_index_template_date_based(self, case: DateBasedEnsureTemplateCase):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", False)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.overwrite_existing_templates", case.overwrite_templates)

        handler = metrics.IndexHandler(self.cfg, self.client, case.es_store_type)

        self.client.index_template_exists.return_value = case.index_template_exists
        if case.index_template_exists:
            if case.identical:
                real_template = json.loads(handler._index_template_provider.get_template(case.es_store_type))["template"]
            else:
                real_template = {}
            self.client.get_template.return_value = mock.MagicMock(
                body={"index_templates": [{"index_template": {"template": real_template}}]}
            )

        handler.ensure_index_template(create=case.create)

        self._assert_index_template_calls(
            False,
            case.es_store_type,
            expect_get_template=case.expect_get_template,
            expect_put_template=case.expect_put_template,
            expect_index_template_exists=case.expect_index_template_exists,
        )

    # Custom component templates are only touched manually, they do not get overwritten at any point.
    def test_ensure_component_template_does_not_overwrite_existing_custom_template(self):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        self.cfg.add(config.Scope.applicationOverride, "reporting", "datastore.overwrite_existing_templates", True)

        handler = metrics.IndexHandler(self.cfg, self.client, metrics.EsStoreType.metrics)
        custom_name = (
            f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}"
            f"{metrics.ComponentTemplateProvider.COMPONENT_TEMPLATE_CUSTOM_SUFFIX}"
        )

        self.client.component_template_exists.return_value = True

        handler._ensure_component_template(custom_name, json.dumps({"template": {"settings": {"index": {"number_of_replicas": 0}}}}))

        self.client.component_template_exists.assert_called_once_with(custom_name)
        self.client.get_component_template.assert_not_called()
        self.client.put_component_template.assert_not_called()

    def test_ensure_component_template_creates_custom_template_if_missing(self):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)

        handler = metrics.IndexHandler(self.cfg, self.client, metrics.EsStoreType.metrics)
        custom_name = (
            f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}"
            f"{metrics.ComponentTemplateProvider.COMPONENT_TEMPLATE_CUSTOM_SUFFIX}"
        )
        custom_template = json.dumps({"template": {}})

        self.client.component_template_exists.return_value = False

        handler._ensure_component_template(custom_name, custom_template)

        self.client.component_template_exists.assert_called_once_with(custom_name)
        self.client.put_component_template.assert_called_once_with(custom_name, custom_template)


class TestEsMetricsStore:  # pylint: disable=too-many-public-methods
    RACE_TIMESTAMP = datetime.datetime(2016, 1, 31)
    RACE_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setup_method(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "track", "params", {"shard-count": 3})
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        self.metrics_store, self.es_mock = self._make_metrics_store(use_data_streams=True)

    def _mock_index_handler(self, use_data_streams):
        index_handler = mock.MagicMock()
        index_handler.use_data_streams = use_data_streams

        def _index_name(race_timestamp):
            if use_data_streams:
                return f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}"
            ts = time.from_iso8601(race_timestamp) if isinstance(race_timestamp, str) else race_timestamp
            return f"{metrics.EsStoreType.metrics.index_prefix}{ts.year:04d}-{ts.month:02d}"

        index_handler.index_name.side_effect = _index_name
        index_handler.migrated_index_name.side_effect = lambda index_name: f"{index_name}.new"
        return index_handler

    def _make_metrics_store(self, use_data_streams):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", use_data_streams)
        store = metrics.EsMetricsStore(
            self.cfg,
            client_factory_class=MockClientFactory,
            clock=StaticClock,
        )
        store._index_handler = self._mock_index_handler(use_data_streams)
        es_mock = store._client
        store.logger = mock.create_autospec(logging.Logger)
        return store, es_mock

    @dataclass
    class OpenCase:
        create: bool = True
        use_data_streams: bool = True
        want_refresh: bool = False
        prefer_new_index_suffix: bool = False

    @cases.cases(
        create_false=OpenCase(create=False),
        create_false_date_based=OpenCase(create=False, use_data_streams=False, want_refresh=True, prefer_new_index_suffix=True),
        data_streams_create=OpenCase(create=True, use_data_streams=True, want_refresh=False),
        date_based_create=OpenCase(create=True, use_data_streams=False, want_refresh=True),
    )
    def test_open(self, case: OpenCase):
        self.metrics_store, self.es_mock = self._make_metrics_store(case.use_data_streams)
        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=case.create)
        self.metrics_store._index_handler.ensure_index_template.assert_called_once_with(create=case.create)
        expected_index = self.metrics_store._index_handler.index_name(self.RACE_TIMESTAMP)
        if case.prefer_new_index_suffix:
            expected_index = self.metrics_store._index_handler.migrated_index_name(expected_index)
        if case.want_refresh:
            self.es_mock.refresh.assert_called_once_with(index=expected_index)
        else:
            self.es_mock.refresh.assert_not_called()
        self.es_mock.bulk_index.assert_not_called()
        self.es_mock.search.assert_not_called()

    def test_open_read_only_prefers_new_index_suffix(self):
        self.metrics_store, self.es_mock = self._make_metrics_store(use_data_streams=False)
        self.metrics_store._index_handler.migrated_index_name.return_value = "rally-metrics-2016-01.new"
        self.es_mock.exists.return_value = True

        self.metrics_store.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=False)

        self.metrics_store._index_handler.ensure_index_template.assert_called_once_with(create=False)
        self.es_mock.exists.assert_called_once_with(index="rally-metrics-2016-01.new")
        self.es_mock.refresh.assert_called_once_with(index="rally-metrics-2016-01.new")

    def test_put_value_redacts_secret_prefixed_track_param_values(self):
        self.cfg.add(config.Scope.application, "track", "params", {"shard-count": 3, "secret_token": "nope"})
        self.metrics_store, self.es_mock = self._make_metrics_store(use_data_streams=False)

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
            "track-params": {"shard-count": 3, "secret_token": metrics.SECRET_TRACK_PARAM_PLACEHOLDER},
            "challenge": "append",
            "car": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {},
        }
        self.metrics_store.close()
        self.es_mock.bulk_index.assert_called_with(index="rally-metrics-2016-01", items=[expected_doc], use_data_streams=False)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_put_value_without_meta_info(self, use_data_streams):
        ms, es_mock = self._make_metrics_store(use_data_streams)
        throughput = 5000
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        ms.put_value_cluster_level("indexing_throughput", throughput, "docs/s")
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
        ms.close()
        expected_index = ms._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=[expected_doc], use_data_streams=use_data_streams)
        es_mock.refresh.assert_called_with(index=expected_index)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_put_value_with_explicit_timestamps(self, use_data_streams):
        ms, es_mock = self._make_metrics_store(use_data_streams)
        throughput = 5000
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        ms.put_value_cluster_level(name="indexing_throughput", value=throughput, unit="docs/s", absolute_time=0, relative_time=10)
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
        ms.close()
        expected_index = ms._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=[expected_doc], use_data_streams=use_data_streams)
        es_mock.refresh.assert_called_with(index=expected_index)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_put_value_with_meta_info(self, use_data_streams):
        ms, es_mock = self._make_metrics_store(use_data_streams)
        throughput = 5000
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tags", opts.to_dict("intention:testing,disk_type:hdd"))
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        ms.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        ms.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        ms.put_value_node_level("node0", "indexing_throughput", throughput, "docs/s")
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
        ms.close()
        expected_index = ms._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=[expected_doc], use_data_streams=use_data_streams)
        es_mock.refresh.assert_called_with(index=expected_index)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_put_doc_no_meta_data(self, use_data_streams):
        ms, es_mock = self._make_metrics_store(use_data_streams)
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        ms.put_doc(
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
        ms.close()
        expected_index = ms._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=[expected_doc], use_data_streams=use_data_streams)
        es_mock.refresh.assert_called_with(index=expected_index)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_put_doc_with_metadata(self, use_data_streams):
        ms, es_mock = self._make_metrics_store(use_data_streams)
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "race", "user.tags", opts.to_dict("intention:testing,disk_type:hdd"))
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        ms.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        ms.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        ms.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        ms.put_doc(
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
        ms.close()
        expected_index = ms._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=[expected_doc], use_data_streams=use_data_streams)
        es_mock.refresh.assert_called_with(index=expected_index)

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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

        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )

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
        self.es_mock.search.assert_called_with(
            index=f"{metrics.EsStoreType.metrics.index_prefix}{metrics.EsStoreType.metrics.data_stream_version}", body=expected_query
        )
        return actual_error_rate

    def test_flush_snapshots_docs_before_bulk_index(self):
        # flush() must snapshot and reset self._docs before calling bulk_index so that
        # docs added concurrently by background sampler threads land in the next flush,
        # not in the current one where they would be sent without _op_type="create".
        ms, es_mock = self._make_metrics_store(use_data_streams=True)
        ms.open(self.RACE_ID, self.RACE_TIMESTAMP, "test", "append", "defaults", create=True)

        doc_before = {"name": "before"}
        doc_during = {"name": "during"}
        ms._add(doc_before)

        captured_items = []

        def bulk_index_side_effect(*, index, items, use_data_streams):
            # Simulate a background thread appending during bulk_index.
            ms._add(doc_during)
            captured_items.extend(items)

        es_mock.bulk_index.side_effect = bulk_index_side_effect
        ms.flush(refresh=False)

        # Only doc_before should have been sent in this flush.
        assert captured_items == [doc_before]
        # doc_during must be buffered for the next flush, not lost.
        assert ms._docs == [doc_during]

        # A second flush sends doc_during.
        es_mock.bulk_index.side_effect = None
        ms.flush(refresh=False)
        es_mock.bulk_index.assert_called_with(
            index=ms._index_handler.index_name(self.RACE_TIMESTAMP),
            items=[doc_during],
            use_data_streams=True,
        )


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
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "time.start", self.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "race.id", self.RACE_ID)
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        self.race_store = metrics.EsRaceStore(
            self.cfg,
            client_factory_class=MockClientFactory,
        )
        self.race_store._index_handler = self._mock_index_handler(use_data_streams=True)
        # get hold of the mocked client...
        self.es_mock = self.race_store.client

    def _mock_index_handler(self, use_data_streams):
        index_handler = mock.MagicMock()
        index_handler.use_data_streams = use_data_streams

        def _index_name(race_timestamp):
            if use_data_streams:
                return f"{metrics.EsStoreType.races.index_prefix}{metrics.EsStoreType.races.data_stream_version}"
            return f"{metrics.EsStoreType.races.index_prefix}{race_timestamp:%Y-%m}"

        index_handler.index_name.side_effect = _index_name
        return index_handler

    def _make_race_store(self, use_data_streams):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", use_data_streams)
        store = metrics.EsRaceStore(
            self.cfg,
            client_factory_class=MockClientFactory,
        )
        store._index_handler = self._mock_index_handler(use_data_streams)
        return store, store.client

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

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": self.RACE_ID}},
                    ],
                },
            },
        }
        self.es_mock.search.assert_called_once_with(index="rally-races-*", body=expected_query)

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

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"race-id": "some invalid race id"}},
                    ],
                },
            },
        }
        self.es_mock.search.assert_called_once_with(index="rally-races-*", body=expected_query)

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_store_race(self, use_data_streams):
        rs, es_mock = self._make_race_store(use_data_streams)
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

        rs.store_race(race)

        expected_doc = {
            "rally-version": "0.4.4",
            "rally-revision": "123abc",
            "environment": "unittest",
            "race-id": self.RACE_ID,
            "race-timestamp": "20160131T000000Z",
            "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
        expected_index = rs._index_handler.index_name(self.RACE_TIMESTAMP)
        if use_data_streams:
            es_mock.index.assert_called_with(
                index=expected_index,
                item=expected_doc,
                use_data_streams=True,
            )
        else:
            es_mock.index.assert_called_with(
                index=expected_index,
                id=self.RACE_ID,
                item=expected_doc,
                use_data_streams=False,
            )
        rs._index_handler.ensure_index_template.assert_called_once_with(create=True)

    def test_store_race_update_with_data_streams(self):
        rs, es_mock = self._make_race_store(use_data_streams=True)
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
        )

        # First call creates the race document
        rs.store_race(race)
        es_mock.index.assert_called_once()
        assert rs._race_stored is True

        # Second call (e.g. after benchmark completes) updates via update_by_query
        race.add_results(
            self.DictHolder(
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
            )
        )
        rs.store_race(race)

        expected_index = rs._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.refresh.assert_called_once_with(expected_index)
        es_mock.update_by_query.assert_called_once_with(
            index=expected_index,
            body={
                "query": {"term": {"race-id": self.RACE_ID}},
                "script": {
                    "source": "ctx._source.putAll(params)",
                    "lang": "painless",
                    "params": race.as_dict(),
                },
            },
        )
        # index should still have been called only once (from the first store_race)
        es_mock.index.assert_called_once()

    def test_store_race_redacts_secret_prefixed_track_param_values(self):
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
            user_tags={},
            track=t,
            track_params={"shard-count": 3, "secret_token": "hidden"},
            challenge=t.default_challenge,
            car="defaults",
            car_params=None,
            plugin_params=None,
            track_revision=None,
            team_revision=None,
            distribution_version=None,
            distribution_flavor=None,
            revision=None,
            results={},
        )

        self.race_store.store_race(race)

        indexed = self.es_mock.index.call_args.kwargs["item"]
        assert indexed["track-params"] == {
            "shard-count": 3,
            "secret_token": metrics.SECRET_TRACK_PARAM_PLACEHOLDER,
        }

    @mock.patch("esrally.utils.console.println")
    def test_delete_race(self, console):
        self.es_mock.delete_by_query.return_value = {"deleted": 0}
        self.cfg.add(config.Scope.application, "system", "delete.id", "0101")
        self.race_store.delete_race()
        expected_query = {"query": {"bool": {"filter": [{"term": {"environment": "unittest-env"}}, {"term": {"race-id": "0101"}}]}}}
        assert self.es_mock.delete_by_query.call_count == 3
        self.es_mock.delete_by_query.assert_any_call(index="rally-races-*", body=expected_query)
        self.es_mock.delete_by_query.assert_any_call(index="rally-metrics-*", body=expected_query)
        self.es_mock.delete_by_query.assert_any_call(index="rally-results-*", body=expected_query)
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

        self.es_mock.exists.assert_called_once_with(index="rally-annotations")
        self.es_mock.index.assert_called_once_with(
            index="rally-annotations",
            id="7",
            item=item,
            use_data_streams=False,
        )
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
        self.cfg.add(config.Scope.application, "system", "list.challenge", "unittest-challenge")
        self.cfg.add(config.Scope.application, "system", "list.races.benchmark_name", "unittest-test")
        self.cfg.add(config.Scope.application, "system", "list.races.user_tags", {"env-id": "123", "name": "unittest-test2"})
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
                        {"term": {"challenge": "unittest-challenge"}},
                        {"term": {"user-tags.env-id": "123"}},
                        {"term": {"user-tags.name": "unittest-test2"}},
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
        self.cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "system", "time.start", self.RACE_TIMESTAMP)
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", True)
        self.results_store = metrics.EsResultsStore(
            self.cfg,
            client_factory_class=MockClientFactory,
        )
        self.results_store._index_handler = self._mock_index_handler(use_data_streams=True)
        # get hold of the mocked client...
        self.es_mock = self.results_store.client

    def _mock_index_handler(self, use_data_streams):
        index_handler = mock.MagicMock()
        index_handler.use_data_streams = use_data_streams

        def _index_name(race_timestamp):
            if use_data_streams:
                return f"{metrics.EsStoreType.results.index_prefix}{metrics.EsStoreType.results.data_stream_version}"
            return f"{metrics.EsStoreType.results.index_prefix}{race_timestamp:%Y-%m}"

        index_handler.index_name.side_effect = _index_name
        return index_handler

    def _make_results_store(self, use_data_streams):
        self.cfg.add(config.Scope.application, "reporting", "datastore.use_data_streams", use_data_streams)
        store = metrics.EsResultsStore(
            self.cfg,
            client_factory_class=MockClientFactory,
        )
        store._index_handler = self._mock_index_handler(use_data_streams)
        return store, store.client

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_store_results(self, use_data_streams):
        rs, es_mock = self._make_results_store(use_data_streams)
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

        rs.store_results(race)

        expected_docs = [
            {
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
        expected_index = rs._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=expected_docs, use_data_streams=use_data_streams)
        rs._index_handler.ensure_index_template.assert_called_once_with(create=True)
        es_mock.index.assert_not_called()
        es_mock.refresh.assert_not_called()

    @pytest.mark.parametrize("use_data_streams", [True, False])
    def test_store_results_with_missing_version(self, use_data_streams):
        rs, es_mock = self._make_results_store(use_data_streams)
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

        rs.store_results(race)

        expected_docs = [
            {
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
                "@timestamp": time.to_epoch_millis(self.RACE_TIMESTAMP.timestamp()),
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
        expected_index = rs._index_handler.index_name(self.RACE_TIMESTAMP)
        es_mock.bulk_index.assert_called_with(index=expected_index, items=expected_docs, use_data_streams=use_data_streams)
        rs._index_handler.ensure_index_template.assert_called_once_with(create=True)
        es_mock.index.assert_not_called()
        es_mock.refresh.assert_not_called()


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
            user_tags={"name": "unittest-test", "env-id": "123"},
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
        self.cfg.add(config.Scope.application, "system", "list.races.user_tags", {"env-id": "321", "name": "unittest-test"})
        assert len(self.race_store.list()) == 0
        self.cfg.add(config.Scope.application, "system", "list.races.user_tags", {"env-id": "123", "name": "unittest-test"})
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160129")
        assert len(self.race_store.list()) == 0
        self.cfg.add(config.Scope.application, "system", "list.to_date", "20160131")
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.from_date", "20160131")
        assert len(self.race_store.list()) == 1
        self.cfg.add(config.Scope.application, "system", "list.challenge", t.default_challenge.name)
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
